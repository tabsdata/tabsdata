#
# Copyright 2026 Tabsdata Inc.
#

"""Error model for the fetch-request command (exit codes 10..)."""

from __future__ import annotations

from tabsdatak.error import ErrorCode, ErrorDef
from tabsdatak.server._tdrunner.function._lib.errors import (
    CommandRuntimeException,
    WriteFailedError as _WriteFailedError,
)


class HttpFetchError(CommandRuntimeException):
    """GET against ``--url`` failed (non-2xx response or network error)."""

    exit_code = 10


class WriteFailedError(_WriteFailedError):
    """``--out`` could not be written (missing parent dir, permission, ...)."""

    exit_code = 11


class BadPayloadError(CommandRuntimeException):
    """The body returned by the resolver wasn't valid JSON, or was JSON
    but didn't carry the expected ``{"data": ...}`` envelope shape."""

    exit_code = 12


class FetchRequestErrorCode(ErrorCode):
    FR_1 = ErrorDef(
        HttpFetchError,
        "HTTP {status} from {url}: {detail}",
    )
    # Raised when ``urllib`` returns an ``HTTPError`` (server reachable
    # but responded non-2xx). ``status`` is the HTTP status code.

    FR_2 = ErrorDef(
        HttpFetchError,
        "network error contacting {url}: {detail}",
    )
    # Raised when ``urllib`` returns a non-HTTP ``URLError`` (DNS,
    # connection refused, TLS, ...). Same exit code as FR_1 because
    # both are "the fetch didn't succeed".

    FR_3 = ErrorDef(
        WriteFailedError,
        "failed to write {path}: {detail}",
    )
    # Raised when ``Path.write_bytes`` hits an ``OSError``.

    FR_4 = ErrorDef(
        BadPayloadError,
        "response from {url} is not a valid {expected} envelope: {detail}",
    )
    # Raised when the resolver body fails ``json.loads`` (``detail`` is
    # the JSON parser message) or parses to JSON without the expected
    # ``data`` field (``detail`` describes the missing field).
