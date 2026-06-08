#
# Copyright 2026 Tabs Data Inc.
#

"""Error machinery for tdrunner commands.

Same three-layer pattern as :mod:`tabsdatak.error`:

* :class:`CommandException` -- base (a :class:`TabsdataException`),
  carries an ``exit_code`` class attribute that
  :func:`tabsdatak.server._tdrunner.function._lib.cli.run_command`
  maps to the process exit code.
* :class:`CommandInitException` / :class:`CommandRuntimeException` /
  :class:`CommandInternalException` -- severity tiers, mirroring the
  ``Api{Init,Runtime,Internal}Exception`` split.
* Per-site exception classes inheriting from the matching severity,
  each pinning its own ``exit_code``.
* :class:`CommandErrorCode` -- the enum binding ``ErrorDef(exception,
  template)`` per cross-command site. Per-command sites live in their
  own ``commands/<name>/error.py``.

Exit-code semantics (per ``docs/01-CommandBasics.md``):

* ``1``       -- invalid or missing options (:class:`InvalidOptionsError`).
* ``2``       -- undefined env in an ``env:<VAR>`` option (:class:`UndefinedEnvError`).
* ``3``       -- unhandled / unexpected exception (:class:`UnexpectedError`).
* ``4``       -- inconsistent state, e.g. ``CommandException`` raised with
  ``exit_code=0`` (:class:`InconsistentError`).
* ``5``       -- premature exit, ``SystemExit`` intercepted in command body
  (:class:`PrematureError`).
* ``6..9``    -- reserved.
* ``10..``    -- command-specific (see ``commands/<command>/error.py``).
"""

from __future__ import annotations

from tabsdatak.error import (
    ErrorCode,
    ErrorDef,
    TabsdataException,
)

EXIT_INVALID_OPTIONS: int = 1
EXIT_UNDEFINED_ENV: int = 2
EXIT_UNEXPECTED: int = 3
EXIT_INCONSISTENT: int = 4
EXIT_PREMATURE: int = 5
COMMAND_SPECIFIC_MIN_EXIT_CODE: int = 10


# --- bases / severity ---------------------------------------------------


class CommandException(TabsdataException):
    """Base class for every tdrunner-command failure.

    Concrete leaf subclasses pin their own ``exit_code`` class attribute;
    :func:`run_command` reads it to return the process exit code.
    """

    exit_code: int  # set on each concrete leaf subclass


class CommandInitException(CommandException):
    """Construction-time precondition failed (bad options, malformed
    config, ...). Mirrors :class:`tabsdatak.error.ApiInitException`."""


class CommandRuntimeException(CommandException):
    """Runtime precondition failed (missing env var, missing file,
    unreachable endpoint, ...). Mirrors
    :class:`tabsdatak.error.ApiRuntimeException`."""


class CommandInternalException(CommandException):
    """Internal bug; should never reach the user. Mirrors
    :class:`tabsdatak.error.ApiInternalException`."""


# --- shared per-site exception classes ---------------------------------


class InvalidOptionsError(CommandInitException):
    """Invalid or missing command-line options (exit 1)."""

    exit_code = EXIT_INVALID_OPTIONS


class UndefinedEnvError(CommandRuntimeException):
    """``env:<VAR>`` reference whose environment variable is unset
    (exit 2)."""

    exit_code = EXIT_UNDEFINED_ENV


# --- orchestration-boundary exceptions ---------------------------------
#
# These are raised by the ``command`` decorator's outer try/except
# wrapper in :mod:`tabsdatak.server._tdrunner.function._lib.cli`. A
# command body should normally raise a per-command typed exception
# (its own ``<Cmd>ErrorCode`` enum); these three are the fallback
# classes for failures that escape that contract.


class UnexpectedError(CommandInternalException):
    """An unhandled, non-:class:`CommandException` exception escaped
    the command body. The decorator's outer ``except Exception`` arm
    wraps it and exits with code 3."""

    exit_code = EXIT_UNEXPECTED


class InconsistentError(CommandInternalException):
    """A :class:`CommandException` reached the orchestration layer
    with ``exit_code=0``. Treated as an internal-bug exit code so the
    pod reports a non-zero status to the responder."""

    exit_code = EXIT_INCONSISTENT


class PrematureError(CommandInternalException):
    """``SystemExit`` reached the orchestration layer (the command
    body called ``sys.exit`` or a library short-circuited the
    interpreter). The decorator promotes it to exit code 5 so the
    pod's failure-reporting path runs."""

    exit_code = EXIT_PREMATURE


# --- enum binding the shared templates ---------------------------------


class CommandErrorCode(ErrorCode):
    """Cross-command error sites. Per-command sites live in
    ``commands/<name>/error.py``."""

    CMD_1 = ErrorDef(
        InvalidOptionsError,
        "invalid or missing options: {detail}",
    )
    # Raised by ``_lib.cli._Parser.error`` and by ``resolve_flag`` when the
    # resolved literal isn't ``true`` / ``false``.

    CMD_2 = ErrorDef(
        UndefinedEnvError,
        "environment variable {var!r} referenced via env: but not defined",
    )
    # Raised by ``_lib.cli.resolve_value`` when an ``env:<VAR>`` reference
    # resolves to an unset variable.
