#
# Copyright 2026 Tabs Data Inc.
#

"""Argument parsing for tdrunner commands.

Implements the `--<opt> <V> | env:<VAR> | val:<V>` shape from
`docs/01-CommandBasics.md` and provides a parser builder that pre-installs
the spec's common options (`--help/-h`, `--man/-m`, `--version/-v`,
`--info/-i`, `--liveness/-l`).
"""

from __future__ import annotations

import argparse
import os
import sys
from typing import Callable, NoReturn

from tabsdatak._tdrunner.function._lib.errors import (
    CommandError,
    InvalidOptionsError,
    UndefinedEnvError,
)

_ENV_PREFIX = "env:"
_VAL_PREFIX = "val:"


def resolve_value(arg: str) -> str:
    """Resolve a `--<opt> <ARG>` value.

    `arg` may be a literal string, ``env:<VAR>`` (read from the
    environment), or ``val:<VALUE>`` (explicit literal form, useful when
    the literal would otherwise look like an `env:` reference).

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
            raise UndefinedEnvError(
                f"environment variable {var_name!r} is not set"
            ) from exc
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
    raise InvalidOptionsError(
        f"flag value must be 'true' or 'false' (case insensitive), got {value!r}"
    )


class _Parser(argparse.ArgumentParser):
    """ArgumentParser that turns parse errors into InvalidOptionsError."""

    def error(self, message: str) -> NoReturn:  # type: ignore[override]
        raise InvalidOptionsError(message)


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

    The returned parser supports ``--help/-h`` (auto from argparse),
    ``--man/-m``, ``--version/-v``, ``--info/-i`` (each printing the
    corresponding text and exiting 0), and a required
    ``--liveness/-l PATH``.
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
        "--log-config",
        "-c",
        default=None,
        metavar="PATH",
        help="Path to a logging config YAML. When omitted, the bundled default is used.",
    )
    return parser


def run_command(main_fn: Callable[[], int | None]) -> int:
    """Run a command's `main_fn`, mapping CommandError to its exit code.

    A return value of ``None`` is treated as success (exit 0). Anything
    else is coerced to ``int`` and returned.
    """
    try:
        result = main_fn()
    except CommandError as exc:
        print(str(exc), file=sys.stderr)
        return exc.exit_code
    if result is None:
        return 0
    return int(result)
