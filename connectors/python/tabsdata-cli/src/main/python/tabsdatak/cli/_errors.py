#
# Copyright 2026 Tabsdata Inc.
#

"""Classification of every tdk failure into a typed error with a fixed exit
code. Humans and programs get the same exit codes; a machine run
(:mod:`tabsdatak.cli._machine`) also gets the classified error itself."""

from collections.abc import Mapping
from dataclasses import dataclass, field
from enum import IntEnum
from typing import Any

import rich_click as click


class ExitCode(IntEnum):
    OK = 0
    FAILURE = 1
    USAGE = 2
    AUTH = 3
    FORBIDDEN = 4
    NOT_FOUND = 5
    CONFLICT = 6
    WAIT_TIMEOUT = 7
    RUN_FAILED = 8


ERROR_TYPES: dict[str, ExitCode] = {
    "server_error": ExitCode.FAILURE,
    "unavailable": ExitCode.FAILURE,
    "rate_limited": ExitCode.FAILURE,
    "unreachable": ExitCode.FAILURE,
    "invalid_request": ExitCode.FAILURE,
    "internal": ExitCode.FAILURE,
    "checks_failed": ExitCode.FAILURE,
    "version_mismatch": ExitCode.FAILURE,
    "aborted": ExitCode.FAILURE,
    "usage": ExitCode.USAGE,
    "missing_argument": ExitCode.USAGE,
    "no_project": ExitCode.USAGE,
    "path_not_allowed": ExitCode.USAGE,
    "ambiguous_credentials": ExitCode.USAGE,
    "auth_required": ExitCode.AUTH,
    "auth_expired": ExitCode.AUTH,
    "forbidden": ExitCode.FORBIDDEN,
    "service_token_unsupported": ExitCode.FORBIDDEN,
    "session_unsupported": ExitCode.FORBIDDEN,
    "capability_denied": ExitCode.FORBIDDEN,
    "not_found": ExitCode.NOT_FOUND,
    "conflict": ExitCode.CONFLICT,
    "wait_timeout": ExitCode.WAIT_TIMEOUT,
    "run_failed": ExitCode.RUN_FAILED,
}

_CONFLICT_CODES = frozenset({"SqlError::1", "SqlError::2"})
_LIST_HINT = (
    "Check the fields in '--filter' and '--order-by': the message names the one "
    "this listing does not support."
)
_FORBIDDEN_CODES = {
    "RbacError::3001": "service_token_unsupported",
    "RbacError::3002": "session_unsupported",
}


@dataclass(frozen=True)
class TdkError:
    type: str
    message: str
    status: int | None = None
    code: str | None = None
    hint: str | None = None
    retry_after: float | None = None
    extra: Mapping[str, Any] = field(default_factory=dict, compare=False)

    @property
    def exit_code(self) -> ExitCode:
        return ERROR_TYPES[self.type]


class TdkException(click.ClickException):
    """A failure whose type is known where it is raised. ``extra`` carries
    fields for a machine run's error document, such as ``resume`` and
    ``state``."""

    def __init__(self, type: str, message: str, hint: str | None = None, **extra):
        super().__init__(message)
        self.error = TdkError(type=type, message=message, hint=hint, extra=extra)
        self.exit_code = self.error.exit_code


def _api_error_type(status: int | None, error: str | None, code: str | None) -> str:
    if status == 400:
        if error == "not_allowed" or code in _CONFLICT_CODES:
            return "conflict"
        return "invalid_request"
    if status == 401:
        return "auth_required"
    if status == 403:
        return _FORBIDDEN_CODES.get(code, "forbidden")
    if status == 404:
        return "not_found"
    if status in (409, 423):
        return "conflict"
    if status == 426:
        return "version_mismatch"
    if status == 429:
        return "rate_limited"
    if status == 503:
        return "unavailable"
    return "server_error"


def _causes(exc: BaseException):
    seen = set()
    while exc is not None and id(exc) not in seen:
        seen.add(id(exc))
        yield exc
        exc = exc.__cause__ or (None if exc.__suppress_context__ else exc.__context__)


def classify(exc: BaseException, list_options_used: bool = False) -> TdkError:
    """``list_options_used``: the command took '--filter' or '--order-by',
    which a listing error then hints at."""
    import requests

    # noinspection PyProtectedMember
    from tabsdatak.sdk._apiserver import (
        APIServerError,
        NoProjectSelectedError,
        NotFound,
    )

    message = exc.format_message() if isinstance(exc, click.ClickException) else None
    message = message or str(exc) or type(exc).__name__
    for cause in _causes(exc):
        if isinstance(cause, (TdkException, ClassifiedException)):
            return cause.error
        if isinstance(cause, click.Abort):
            return TdkError(type="aborted", message="Aborted.")
        if isinstance(cause, click.UsageError):
            return TdkError(type="usage", message=cause.format_message())
        if isinstance(cause, NoProjectSelectedError):
            return TdkError(type="no_project", message=message)
        if isinstance(cause, NotFound):
            return TdkError(type="not_found", message=message)
        if isinstance(cause, APIServerError):
            return TdkError(
                type=_api_error_type(cause.status, cause.error, cause.code),
                message=message,
                status=cause.status,
                code=cause.code,
                hint=(
                    _LIST_HINT
                    if list_options_used
                    and (cause.code or "").startswith("ListError::")
                    else None
                ),
                retry_after=cause.retry_after,
            )
        if isinstance(
            cause, (requests.exceptions.ConnectionError, requests.exceptions.Timeout)
        ):
            return TdkError(type="unreachable", message=message)
    if isinstance(exc, click.ClickException) and (
        exc.__context__ is None or exc.__suppress_context__
    ):
        return TdkError(type="invalid_request", message=message)
    return TdkError(type="internal", message=message)


class ClassifiedException(click.ClickException):
    """Carries a classified failure to click's own error rendering."""

    def __init__(self, error: TdkError):
        super().__init__(error.message)
        self.error = error
        self.exit_code = error.exit_code
