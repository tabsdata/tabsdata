#
# Copyright 2026 Tabsdata Inc.
#

"""Error model for the prep-upload-raw command (exit codes 10..)."""

from __future__ import annotations

from tabsdatak.error import ErrorCode, ErrorDef
from tabsdatak.server._tdrunner.function._lib.errors import (
    BadEndpointSchemeError as _BadEndpointSchemeError,
    BadManifestError as _BadManifestError,
    BadRequestError as _BadRequestError,
    WriteFailedError as _WriteFailedError,
)


class BadRequestError(_BadRequestError):
    exit_code = 10


class BadManifestError(_BadManifestError):
    """``--enriched-inputs-manifest`` missing, unreadable, or malformed."""

    exit_code = 11


class BadEndpointSchemeError(_BadEndpointSchemeError):
    """The raw-data destination uses a URI scheme this command doesn't
    know how to upload to."""

    exit_code = 12


class WriteFailedError(_WriteFailedError):
    """``--out`` could not be written (missing parent dir, permission,
    ...)."""

    exit_code = 13


class PrepUploadRawErrorCode(ErrorCode):
    PUR_1 = ErrorDef(
        BadRequestError,
        "unreadable request.yaml {path}: {detail}",
    )
    # ``load_request_or_raise``.

    PUR_2 = ErrorDef(
        BadManifestError,
        "could not load enriched inputs manifest {path}: {detail}",
    )
    # ``load_inputs_manifest_or_raise``.

    PUR_3 = ErrorDef(
        BadEndpointSchemeError,
        "unsupported scheme in the raw-data location: {detail}",
    )
    # ``build_upload_raw_request`` ``UnsupportedSchemeError``.

    PUR_4 = ErrorDef(
        WriteFailedError,
        "failed to write {path}: {detail}",
    )
    # ``write_transporter_request`` ``OSError``.
