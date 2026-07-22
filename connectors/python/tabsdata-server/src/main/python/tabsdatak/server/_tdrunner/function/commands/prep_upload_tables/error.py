#
# Copyright 2026 Tabsdata Inc.
#

"""Error model for the prep-upload-tables command (exit codes 10..)."""

from __future__ import annotations

from tabsdatak.error import ErrorCode, ErrorDef
from tabsdatak.server._tdrunner.function._lib.errors import (
    BadEndpointSchemeError as _BadEndpointSchemeError,
    BadManifestError as _BadManifestError,
    BadRequestError as _BadRequestError,
    ManifestMismatchError as _ManifestMismatchError,
    WriteFailedError as _WriteFailedError,
)


class BadRequestError(_BadRequestError):
    exit_code = 10


class BadManifestError(_BadManifestError):
    """``--outputs-manifest`` file missing, unreadable, or malformed."""

    exit_code = 11


class ManifestMismatchError(_ManifestMismatchError):
    """The outputs manifest is missing an entry for a declared
    output table."""

    exit_code = 12


class BadEndpointSchemeError(_BadEndpointSchemeError):
    """An output-table destination uses a URI scheme this command
    doesn't know how to upload to."""

    exit_code = 13


class WriteFailedError(_WriteFailedError):
    """``--out`` could not be written (missing parent dir, permission,
    ...)."""

    exit_code = 14


class PrepUploadTablesErrorCode(ErrorCode):
    PUT_1 = ErrorDef(
        BadRequestError,
        "unreadable request.yaml {path}: {detail}",
    )
    # ``load_request_or_raise``.

    PUT_2 = ErrorDef(
        BadManifestError,
        "could not load outputs manifest {path}: {detail}",
    )
    # ``load_outputs_manifest_or_raise``.

    PUT_3 = ErrorDef(
        ManifestMismatchError,
        "outputs manifest is missing an entry for a declared table: {detail}",
    )
    # ``build_upload_tables_request`` ``OutputTableNotInManifestError``.

    PUT_4 = ErrorDef(
        BadEndpointSchemeError,
        "unsupported scheme in an output-table location: {detail}",
    )
    # ``build_upload_tables_request`` ``UnsupportedSchemeError``.

    PUT_5 = ErrorDef(
        WriteFailedError,
        "failed to write {path}: {detail}",
    )
    # ``write_transporter_request`` ``OSError``.
