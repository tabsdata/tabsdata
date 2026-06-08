#
# Copyright 2026 Tabs Data Inc.
#

"""Argument parsing and run orchestration for tdrunner commands.

Implements the ``--<opt> <V> | env:<VAR> | val:<V>`` shape from
``docs/01-CommandBasics.md`` and provides a parser builder pre-installed
with the spec's common options:

* ``--help`` / ``-h``
* ``--man`` / ``-m``
* ``--version`` / ``-v``
* ``--info`` / ``-i``
* ``--liveness`` / ``-l``
* ``--log-config`` / ``-c``
* ``--responder-url``
* ``--responder-method``
* ``--responder-authorization``

Also defines the :func:`command` decorator, which wraps every command
body in a single try/except orchestration. On exit, it logs the outcome,
posts a failure payload to the responder endpoint, and returns the
appropriate exit code. The handled cases are:

* :class:`CommandError`
* :class:`Exception`
* :class:`SystemExit`
"""

from __future__ import annotations

import argparse
import json
import logging
import os
import sys
import urllib.error
import urllib.request
from pathlib import Path
from typing import Callable, NoReturn

from tenacity import retry, stop_after_delay, wait_fixed

from tabsdatak._logging import setup_logging
from tabsdatak.server._tdrunner.function._lib.errors import (
    EXIT_INCONSISTENT,
    EXIT_PREMATURE,
    EXIT_UNEXPECTED,
    CommandError,
    CommandErrorCode,
    CommandException,
)
from tabsdatak.server._tdrunner.function._lib.liveness import install_liveness_handler
from tabsdatak.server._tdrunner.function.commands import tabsdata_command

logger = logging.getLogger(__name__)

_ENV_PREFIX = "env:"
_VAL_PREFIX = "val:"

_RESPONDER_RETRY_WAIT_SECONDS = 2
_RESPONDER_RETRY_TOTAL_SECONDS = 32


def resolve_value(arg: str) -> str:
    """Resolve a ``--<opt> <ARG>`` value.

    ``arg`` may take one of three forms:

    * a literal string — returned verbatim.
    * ``env:<VAR>`` — looked up in the environment.
    * ``val:<VALUE>`` — explicit literal form, useful when the literal
      would otherwise look like an ``env:`` reference.

    Raises:
        UndefinedEnvError: if ``env:<VAR>`` references an unset variable.
    """
    if arg.startswith(_VAL_PREFIX):
        return arg[len(_VAL_PREFIX) :]
    if arg.startswith(_ENV_PREFIX):
        var_name = arg[len(_ENV_PREFIX) :]
        try:
            return os.environ[var_name]
        except KeyError as exc:
            raise CommandErrorCode.CMD_2.exception(cause=exc, var=var_name)
    return arg


def resolve_flag(arg: str) -> bool:
    """Resolve a flag-shaped `--<opt> <ARG>`.

    The resolved value (after applying :func:`resolve_value`) must be
    ``true`` or ``false`` (case insensitive).

    Raises:
        UndefinedEnvError: if the underlying ``env:<VAR>`` is unset.
        InvalidOptionsError: if the resolved value is neither `true` nor
            `false`.
    """
    value = resolve_value(arg)
    lower = value.lower()
    if lower == "true":
        return True
    if lower == "false":
        return False
    raise CommandErrorCode.CMD_1.exception(
        detail=(
            f"flag value must be 'true' or 'false' (case insensitive), got {value!r}"
        ),
    )


class _Parser(argparse.ArgumentParser):
    """ArgumentParser that turns parse errors into InvalidOptionsError."""

    def error(self, message: str) -> NoReturn:  # type: ignore[override]
        raise CommandErrorCode.CMD_1.exception(detail=message)


def _make_print_action(text: str) -> type[argparse.Action]:
    class _Action(argparse.Action):
        def __init__(self, option_strings, dest, **kwargs):
            kwargs["nargs"] = 0
            kwargs.setdefault("default", argparse.SUPPRESS)
            super().__init__(option_strings, dest, **kwargs)

        def __call__(self, parser, namespace, values, option_string=None):
            print(text)
            parser.exit(0)

    return _Action


def build_parser(
    prog: str,
    description: str,
    *,
    version: str = "",
    info: str = "",
    man: str = "",
) -> argparse.ArgumentParser:
    """Build an ArgumentParser pre-installed with the spec's common options.

    The returned parser supports:

    * ``--help`` / ``-h`` — auto from argparse.
    * ``--man`` / ``-m`` — print the manual and exit 0.
    * ``--version`` / ``-v`` — print the version and exit 0.
    * ``--info`` / ``-i`` — print the info blurb and exit 0.
    * ``--shared-dir`` / ``-s PATH`` — optional; shared working dir
      (owned by the pod).
    * ``--work-dir`` / ``-w PATH`` — optional; private working dir
      (owned by the container).
    * ``--log-config`` / ``-c PATH`` — optional; path to a logging
      config YAML. Defaults to the bundled config.
    * ``--liveness`` / ``-l PATH`` — required; the file the command
      touches at least once a minute to signal liveness.
    * ``--responder-url`` — required; URL of the responder endpoint.
    * ``--responder-method`` — required; HTTP method for the responder
      call.
    * ``--responder-authorization`` — required; ``Authorization``
      header value for the responder call.
    """
    parser = _Parser(prog=prog, description=description)
    parser.add_argument(
        "--man",
        "-m",
        action=_make_print_action(man),
        help="Print the detailed user guide and exit.",
    )
    parser.add_argument(
        "--version",
        "-v",
        action=_make_print_action(version),
        help="Print the command build version and exit.",
    )
    parser.add_argument(
        "--info",
        "-i",
        action=_make_print_action(info),
        help="Print the command build info and exit.",
    )
    parser.add_argument(
        "--shared-dir",
        "-s",
        default=None,
        metavar="PATH",
        help="Shared working dir (owned by the pod).",
    )
    parser.add_argument(
        "--work-dir",
        "-w",
        default=None,
        metavar="PATH",
        help="Private working dir (owned by the container).",
    )
    parser.add_argument(
        "--log-config",
        "-c",
        default=None,
        metavar="PATH",
        help=(
            "Path to a logging config YAML. When omitted, the bundled default is used."
        ),
    )
    parser.add_argument(
        "--liveness",
        "-l",
        required=True,
        metavar="PATH",
        help=(
            "File the command must touch at least once a minute to signal "
            "liveness to the framework."
        ),
    )
    parser.add_argument(
        "--responder-url",
        required=True,
        help="Responder endpoint URL.",
    )
    parser.add_argument(
        "--responder-method",
        required=True,
        help="Responder HTTP method.",
    )
    parser.add_argument(
        "--responder-authorization",
        required=True,
        help="Responder Authorization header value.",
    )
    return parser


def run_command(main_fn: Callable[[], int | None]) -> int:
    """Run a command's ``main_fn``, mapping a raised
    :class:`CommandException` (new) or :class:`CommandError` (legacy
    shim) to the matching process exit code.

    Each mapped exception is printed to stderr first, then the
    declared ``exit_code`` is returned. A return value of ``None`` is
    treated as success (0); anything else is coerced via ``int()``.

    Test-only helper: production commands go through the ``command``
    decorator's orchestration. This shape is preserved so unit tests
    can call ``run_command(lambda: body(args))`` without standing up
    argparse, logging, or the responder.
    """
    try:
        result = main_fn()
    except CommandException as exc:
        print(str(exc), file=sys.stderr)
        return exc.exit_code
    except CommandError as exc:
        # Legacy shim path: command modules still raising
        # ``CommandSpecificError(exit_code, msg)``. Removed once every
        # command has migrated to its own ``error.py`` (TD-2263).
        print(str(exc), file=sys.stderr)
        return exc.exit_code
    if result is None:
        return 0
    return int(result)


@retry(
    stop=stop_after_delay(_RESPONDER_RETRY_TOTAL_SECONDS),
    wait=wait_fixed(_RESPONDER_RETRY_WAIT_SECONDS),
    reraise=True,
)
def _failure_to_responder_request(request: urllib.request.Request) -> None:
    with urllib.request.urlopen(request) as response:
        response.read()


def _failure_to_responder(
    args: argparse.Namespace | None,
    exit_code: int,
    error: BaseException,
) -> None:
    """Best-effort POST a failure status back to the responder endpoint.

    Any failure inside this helper is swallowed and logged — we are
    already in an error path and must not mask the original failure.
    The HTTP call is wrapped with a tenacity retry: fixed wait of
    :data:`_RESPONDER_RETRY_WAIT_SECONDS` between attempts, total budget
    :data:`_RESPONDER_RETRY_TOTAL_SECONDS`.
    """
    if args is None:
        logger.warning(
            "Skipping responder failure POST: command arguments not available."
        )
        return
    # noinspection PyBroadException
    try:
        url = resolve_value(args.responder_url)
        method = resolve_value(args.responder_method)
        authorization = resolve_value(args.responder_authorization)
        body = json.dumps({
            "status": "Failed",
            "exit_code": exit_code,
            "message": repr(error),
        }).encode("utf-8")
        request = urllib.request.Request(
            url,
            data=body,
            method=method,
            headers={
                "Authorization": authorization,
                "Content-Type": "application/json",
            },
        )
        logger.warning(
            f"Posting responder failure: {method} {url} exit_code={exit_code}"
        )
        _failure_to_responder_request(request)
        logger.warning(
            f"Posted responder failure: {method} {url} exit_code={exit_code}"
        )
    except Exception:
        logger.warning("Failed to post failure status to responder", exc_info=True)


def command(
    *,
    name: str,
    parser: Callable[[], argparse.ArgumentParser],
) -> Callable[[Callable[[argparse.Namespace], int | None]], Callable[[], int]]:
    """Decorator that wraps a command body with the standard orchestration.

    The returned zero-arg ``main`` performs, in order:

    1. parse argv via ``parser``,
    2. configure logging from ``--log-config``,
    3. install the liveness handler from ``--liveness``,
    4. invoke the decorated body with the parsed ``Namespace``,
    5. coerce the body's return into an exit code (a ``None`` return
       becomes 0).

    The whole run lives inside one ``try`` with three ``except`` arms,
    so the decorated body never needs its own top-level try/except:

    * :class:`CommandError` — logged at ERROR with traceback; posted to
      the responder; ``exit_code`` is promoted to
      :data:`EXIT_INCONSISTENT` if the error claimed ``exit_code=0``.
    * :class:`Exception` — logged at ERROR with traceback; posted to
      the responder; returns :data:`EXIT_UNEXPECTED`.
    * :class:`SystemExit` — logged at ERROR (no traceback); posted to
      the responder; returns :data:`EXIT_PREMATURE`.

    The result is additionally tagged with :func:`tabsdata_command` so
    the SDK packager picks it up as a console-script entry point.

    Example::

        @command(name="td_fetch_request", parser=_build_parser)
        def main(args: argparse.Namespace) -> int:
            url = resolve_value(args.resolver_url)
            ...
            return 0
    """

    # ToDo:
    #  Resolve environment variables (similar to config resolver) for
    #   parameters that represent configuration files
    def wrap(
        body: Callable[[argparse.Namespace], int | None],
    ) -> Callable[[], int]:
        @tabsdata_command(name=name)
        def main() -> int:
            args: argparse.Namespace | None = None
            try:
                parsed = parser().parse_args()
                args = parsed
                setup_logging(parsed.log_config)
                install_liveness_handler(Path(parsed.liveness))
                result = body(parsed)
                exit_code = 0 if result is None else int(result)
                return exit_code
            except CommandError as error:
                exit_code = error.exit_code
                logger.exception("Handled exception caught by command")
                if exit_code == 0:
                    logger.error(
                        f"CommandError raised with exit_code=0; "
                        f"promoting to {EXIT_INCONSISTENT}"
                    )
                    exit_code = EXIT_INCONSISTENT
                _failure_to_responder(args, exit_code, error)
                return exit_code
            except Exception as exception:
                exit_code = EXIT_UNEXPECTED
                logger.exception("Unhandled exception caught by command")
                _failure_to_responder(args, exit_code, exception)
                return exit_code
            except SystemExit as exit_event:
                exit_code = EXIT_PREMATURE
                logger.error(
                    f"Exit ({exit_event.code}) intercepted by command; "
                    f"promoting to {exit_code}"
                )
                _failure_to_responder(args, exit_code, exit_event)
                return exit_code

        return main

    return wrap
