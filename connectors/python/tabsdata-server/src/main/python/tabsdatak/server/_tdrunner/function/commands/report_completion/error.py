#
# Copyright 2026 Tabs Data Inc.
#

"""Error model for the report-completion command (exit codes 10..)."""

from __future__ import annotations

from tabsdatak.error import ErrorCode, ErrorDef
from tabsdatak.server._tdrunner.function._lib.errors import (
    BadManifestError as _BadManifestError,
    BadRequestError as _BadRequestError,
    CommandRuntimeException,
    ManifestMismatchError as _ManifestMismatchError,
    WriteFailedError as _WriteFailedError,
)


class BadRequestError(_BadRequestError):
    exit_code = 10


class HttpPostError(CommandRuntimeException):
    """POST against ``--url`` failed (non-2xx response or network
    error)."""

    exit_code = 11


class WriteFailedError(_WriteFailedError):
    """``--out`` (the response YAML) could not be written."""

    exit_code = 12


class BadManifestError(_BadManifestError):
    """``--outputs-manifest`` file missing, unreadable, or malformed."""

    exit_code = 13


class ManifestMismatchError(_ManifestMismatchError):
    """A declared ``request.output`` table has no entry in the
    outputs manifest."""

    exit_code = 14


class ReportCompletionErrorCode(ErrorCode):
    RC_1 = ErrorDef(
        BadRequestError,
        "unreadable request.yaml {path}: {detail}",
    )
    # ``load_request_or_raise``.

    RC_2 = ErrorDef(
        BadManifestError,
        "could not load outputs manifest {path}: {detail}",
    )
    # ``load_outputs_manifest_or_raise``.

    RC_3 = ErrorDef(
        ManifestMismatchError,
        (
            "output table {table!r} declared in request has no entry "
            "in the outputs manifest"
        ),
    )
    # ``_build_response_payload`` declared-table-not-in-manifest.

    RC_4 = ErrorDef(
        WriteFailedError,
        "failed to write {path}: {detail}",
    )
    # ``write_response`` ``OSError``.

    RC_5 = ErrorDef(
        HttpPostError,
        "HTTP {status} from {url}: {detail}",
    )
    # ``_post`` ``HTTPError``.

    RC_6 = ErrorDef(
        HttpPostError,
        "network error contacting {url}: {detail}",
    )
    # ``_post`` non-HTTP ``URLError``. Same exit code as RC_5.
