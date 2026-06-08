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

* ``1``       -- invalid or missing options.
* ``2``       -- undefined env in an ``env:<VAR>`` option.
* ``3``       -- unhandled / unexpected exception.
* ``4``       -- inconsistent state (e.g. ``CommandError`` raised with
  ``exit_code=0``).
* ``5``       -- premature exit (``SystemExit`` intercepted in command body).
* ``6..9``    -- reserved.
* ``10..``    -- command-specific (see ``commands/<command>/error.py``).

Legacy shim layer
-----------------

Pre-existing command modules raise ``CommandSpecificError(exit_code,
message)`` directly. Until each command moves to its own ``error.py``
(TD-2263), :class:`CommandError` and :class:`CommandSpecificError`
stay alive as plain ``Exception`` subclasses; ``run_command`` catches
both bases.
"""

from __future__ import annotations

from tabsdatak.error import (
    ErrorCode,
    ErrorDef,
    TabsdataException,
)

# Legacy public constants. Kept for back-compat with command modules
# that import them directly; new code should reach the same numbers
# via the bound exception class's ``exit_code`` attribute.
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


# --- legacy shim layer --------------------------------------------------
#
# Old command modules raise ``CommandSpecificError(EXIT_X, message)``
# directly. Until each command migrates to its own ``error.py`` per
# TD-2263, these shims keep them working. ``run_command`` catches both
# bases.


class CommandError(Exception):
    """Legacy base for errors with an explicit exit code.

    Kept for back-compat while command modules migrate to
    :class:`CommandException` (see TD-2263). New code should raise the
    bound exception via ``<Cmd>ErrorCode.<CODE>.exception(...)`` instead.
    """

    def __init__(self, exit_code: int, message: str = "") -> None:
        super().__init__(message)
        self.exit_code = exit_code

    def __str__(self) -> str:
        message = super().__str__()
        if message:
            return f"[exit {self.exit_code}] {message}"
        return f"[exit {self.exit_code}]"


class CommandSpecificError(CommandError):
    """Legacy command-specific error with an explicit exit code.

    Kept for back-compat. New code should declare per-site exception
    classes in ``commands/<name>/error.py`` and raise via the matching
    ``<Cmd>ErrorCode`` enum.

    Raises:
        ValueError: if ``exit_code`` is below
            :data:`COMMAND_SPECIFIC_MIN_EXIT_CODE`. Codes ``1..9`` are
            reserved by the command-basics spec.
    """

    def __init__(self, exit_code: int, message: str = "") -> None:
        if exit_code < COMMAND_SPECIFIC_MIN_EXIT_CODE:
            raise ValueError(
                "command-specific exit codes must be >= "
                f"{COMMAND_SPECIFIC_MIN_EXIT_CODE}, got {exit_code}"
            )
        super().__init__(exit_code, message)
