#
# Copyright 2026 Tabs Data Inc.
#

"""Exit-code-bearing exceptions for tdrunner commands.

Exit-code semantics follow `docs/01-CommandBasics.md`:

* 1 — invalid or missing options.
* 2 — undefined environment variable used in an `env:` option.
* 3..9 — reserved.
* 10+ — command-specific.
"""

from __future__ import annotations

EXIT_INVALID_OPTIONS: int = 1
EXIT_UNDEFINED_ENV: int = 2
COMMAND_SPECIFIC_MIN_EXIT_CODE: int = 10


class CommandError(Exception):
    """Base class for errors that map to a process exit code."""

    def __init__(self, exit_code: int, message: str = "") -> None:
        super().__init__(message)
        self.exit_code = exit_code

    def __str__(self) -> str:
        message = super().__str__()
        if message:
            return f"[exit {self.exit_code}] {message}"
        return f"[exit {self.exit_code}]"


class InvalidOptionsError(CommandError):
    """Invalid or missing command-line options (exit 1)."""

    def __init__(self, message: str = "") -> None:
        super().__init__(EXIT_INVALID_OPTIONS, message)


class UndefinedEnvError(CommandError):
    """An `env:<VAR>` reference whose environment variable is not set (exit 2)."""

    def __init__(self, message: str = "") -> None:
        super().__init__(EXIT_UNDEFINED_ENV, message)


class CommandSpecificError(CommandError):
    """A command-specific error with an explicit exit code.

    Raises:
        ValueError: if `exit_code` is below
            :data:`COMMAND_SPECIFIC_MIN_EXIT_CODE`. Codes 1–9 are reserved
            by the command-basics spec.
    """

    def __init__(self, exit_code: int, message: str = "") -> None:
        if exit_code < COMMAND_SPECIFIC_MIN_EXIT_CODE:
            raise ValueError(
                "command-specific exit codes must be >= "
                f"{COMMAND_SPECIFIC_MIN_EXIT_CODE}, got {exit_code}"
            )
        super().__init__(exit_code, message)
