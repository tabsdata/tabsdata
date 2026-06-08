#
# Copyright 2026 Tabs Data Inc.
#

"""Error model for the fetch-request command (exit codes 10..)."""

from __future__ import annotations

from tabsdatak.error import ErrorCode, ErrorDef
from tabsdatak.server._tdrunner.function._lib.errors import (
    CommandRuntimeException,
)


class HttpFetchError(CommandRuntimeException):
    """GET against ``--url`` failed (non-2xx response or network error)."""

    exit_code = 10


class WriteFailedError(CommandRuntimeException):
    """``--out`` could not be written (missing parent dir, permission, ...)."""

    exit_code = 11


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
