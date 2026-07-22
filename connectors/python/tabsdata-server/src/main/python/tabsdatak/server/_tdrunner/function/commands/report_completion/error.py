#
# Copyright 2026 Tabsdata Inc.
#

"""Error model for the report-completion command (exit codes 10..)."""

from __future__ import annotations

from tabsdatak.error import ErrorCode, ErrorDef
from tabsdatak.server._tdrunner.function._lib.errors import (
    BadManifestError as _BadManifestError,
    BadRequestError as _BadRequestError,
    ManifestMismatchError as _ManifestMismatchError,
    WriteFailedError as _WriteFailedError,
)


class BadRequestError(_BadRequestError):
    """``--request`` (request.yaml) missing, unreadable, or malformed."""

    exit_code = 10


class BadManifestError(_BadManifestError):
    """``--outputs-manifest`` file missing, unreadable, or malformed."""

    exit_code = 11


class ManifestMismatchError(_ManifestMismatchError):
    """A declared ``request.output`` table has no entry in the
    outputs manifest."""

    exit_code = 12


class WriteFailedError(_WriteFailedError):
    """``--out`` (the response YAML) or the journal could not be written."""

    exit_code = 13


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
    # ``Response.from_fn_response`` declared-table-not-in-manifest.

    RC_4 = ErrorDef(
        WriteFailedError,
        "failed to write {path}: {detail}",
    )
    # ``write_response`` / ``Journal.save`` ``OSError``.
