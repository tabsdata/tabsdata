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
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Callable, NoReturn

from tenacity import retry, stop_after_delay, wait_fixed

from tabsdatak._logging import setup_logging
from tabsdatak.common._enums import WorkerCallbackStatus
from tabsdatak.server._tdrunner.function._lib.errors import (
    CommandErrorCode,
    CommandException,
    InconsistentError,
    PrematureError,
    UnexpectedError,
)
from tabsdatak.server._tdrunner.function._lib.journal import Journal
from tabsdatak.server._tdrunner.function._lib.liveness import install_liveness_handler
from tabsdatak.server._tdrunner.function._lib.message import (
    EXIT_STATUS_SUCCESS,
    ResponseMessagePayload,
)
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
    * ``--journal-file PATH`` — required; path to the shared
      run-metadata YAML (``Journal``) every command in the chain
      reads/writes.
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
    parser.add_argument(
        "--journal-file",
        required=True,
        metavar="PATH",
        help=(
            "Path to the shared Journal YAML every command in the "
            "chain reads/writes on the shared volume."
        ),
    )
    return parser


def run_command(main_fn: Callable[[], int | None]) -> int:
    """Run a command's ``main_fn``, mapping a raised
    :class:`CommandException` to the matching process exit code.

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
    if result is None:
        return 0
    return int(result)


def _notify_status(
    args: argparse.Namespace,
    payload: ResponseMessagePayload,
) -> None:
    """POST a pre-built payload to the responder endpoint, raising on failure.

    Not meant to be called directly — use :func:`notify_running`,
    :func:`notify_done`, or :func:`notify_failed`, which build the proper
    payload for each case.

    The HTTP call is wrapped with a tenacity retry: fixed wait of
    :data:`_RESPONDER_RETRY_WAIT_SECONDS` between attempts, total budget
    :data:`_RESPONDER_RETRY_TOTAL_SECONDS`. If every attempt fails the
    error propagates, so a command body posting a status (e.g.
    ``Running`` or ``Done``) that cannot reach the responder fails like
    any other command error and is reported to the responder as
    ``Failed`` by the :func:`command` orchestration.
    """

    @retry(
        stop=stop_after_delay(_RESPONDER_RETRY_TOTAL_SECONDS),
        wait=wait_fixed(_RESPONDER_RETRY_WAIT_SECONDS),
        reraise=True,
    )
    def post(request: urllib.request.Request) -> None:
        with urllib.request.urlopen(request) as response:
            response.read()

    url = resolve_value(args.responder_url)
    method = resolve_value(args.responder_method)
    authorization = resolve_value(args.responder_authorization)
    body = json.dumps(payload.to_body()).encode("utf-8")
    request = urllib.request.Request(
        url,
        data=body,
        method=method,
        headers={
            "Authorization": authorization,
            "Content-Type": "application/json",
        },
    )
    status = payload.status
    exit_status = payload.exit_status
    logger.info(f"Posting responder {status}: {method} {url} exit_status={exit_status}")
    post(request)
    logger.info(f"Posted responder {status}: {method} {url} exit_status={exit_status}")


def notify_running(args: argparse.Namespace) -> None:
    """POST a ``Running`` status to the responder, raising on failure.

    ``start`` (unix-epoch milliseconds) is read back from the run's
    ``Journal`` and carried through to the apiserver callback envelope's
    ``start`` field. The read is strict: a missing or broken journal
    fails the command and is reported as ``Failed`` by the
    :func:`command` orchestration.
    """
    journal = Journal.load(
        Path(args.journal_file),
        strict=True,
    )
    _notify_status(
        args,
        ResponseMessagePayload(
            status=WorkerCallbackStatus.RUNNING,
            start=journal.start,
        ),
    )


def notify_done(
    args: argparse.Namespace,
    *,
    context: Any | None = None,
    exit_code: int = EXIT_STATUS_SUCCESS,
) -> None:
    """POST a ``Done`` status to the responder, raising on failure.

    ``start`` and ``end`` (unix-epoch milliseconds) are read back from
    the run's ``Journal`` and carried through to the apiserver callback
    envelope's ``start`` / ``end`` fields. The read is strict: a missing
    or broken journal on the success path fails the command and is
    reported as ``Failed`` by the :func:`command` orchestration.
    ``context`` is the optional per-run payload (the worker output).
    """
    journal = Journal.load(
        Path(args.journal_file),
        strict=True,
    )
    logger.debug(f"notify_done context: {context}")
    _notify_status(
        args,
        ResponseMessagePayload(
            status=WorkerCallbackStatus.DONE,
            start=journal.start,
            end=journal.end,
            context=context,
            exit_status=exit_code,
        ),
    )


def notify_failed(
    args: argparse.Namespace | None,
    exit_code: int,
    error: BaseException,
) -> None:
    """Best-effort POST a ``Failed`` status back to the responder endpoint.

    ``start`` and ``end`` are read from the run's ``Journal`` — ``start``
    falls back to ``0`` and ``end`` to the current time when it cannot be
    read or parsed, both unix-epoch milliseconds. Any failure inside this
    helper is swallowed and logged — this is the terminal error path and
    must not mask the original failure, so there is nowhere left to
    escalate a failed post to.
    """
    if args is None:
        logger.warning(
            "Skipping responder Failed POST: command arguments not available."
        )
        return
    # noinspection PyBroadException
    try:
        journal = Journal.load(Path(args.journal_file), strict=False)
        end = (
            journal.end
            if journal.end is not None
            else int(datetime.now(tz=timezone.utc).timestamp() * 1000)
        )
        start = journal.start if journal.start is not None else end
        _notify_status(
            args,
            ResponseMessagePayload(
                status=WorkerCallbackStatus.FAILED,
                start=start,
                end=end,
                exit_status=exit_code,
                error_message=repr(error),
            ),
        )
    except Exception:
        logger.warning("Failed to post Failed status to responder", exc_info=True)


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

    * :class:`CommandException` — logged at ERROR with traceback; posted to
      the responder; ``exit_code`` is promoted to
      :class:`InconsistentError`'s ``exit_code`` (4) if the raised
      exception claimed ``exit_code=0``.
    * :class:`Exception` — logged at ERROR with traceback; wrapped in
      :class:`UnexpectedError` (exit 3); posted to the responder.
    * :class:`SystemExit` — logged at ERROR (no traceback); wrapped in
      :class:`PrematureError` (exit 5); posted to the responder.

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
            except CommandException as error:
                exit_code = error.exit_code
                logger.exception("Handled exception caught by command")
                if exit_code == 0:
                    logger.error(
                        f"CommandException raised with exit_code=0; "
                        f"promoting to {InconsistentError.exit_code}"
                    )
                    exit_code = InconsistentError.exit_code
                notify_failed(args, exit_code, error)
                return exit_code
            except Exception as exception:
                exit_code = UnexpectedError.exit_code
                logger.exception("Unhandled exception caught by command")
                notify_failed(args, exit_code, exception)
                return exit_code
            except SystemExit as exit_event:
                exit_code = PrematureError.exit_code
                logger.error(
                    f"Exit ({exit_event.code}) intercepted by command; "
                    f"promoting to {exit_code}"
                )
                notify_failed(args, exit_code, exit_event)
                return exit_code
            except BaseException as exception:
                exit_code = UnexpectedError.exit_code
                logger.exception("Unhandled core exception caught by command")
                notify_failed(args, exit_code, exception)
                return exit_code

        return main

    return wrap
