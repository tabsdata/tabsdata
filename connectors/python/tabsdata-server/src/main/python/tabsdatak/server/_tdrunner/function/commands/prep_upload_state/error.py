#
# Copyright 2026 Tabsdata Inc.
#

"""Error model for the prep-upload-state command (exit codes 10..)."""

from __future__ import annotations

from tabsdatak.error import ErrorCode, ErrorDef
from tabsdatak.server._tdrunner.function._lib.errors import (
    BadEndpointSchemeError as _BadEndpointSchemeError,
    BadRequestError as _BadRequestError,
    WriteFailedError as _WriteFailedError,
)


class BadRequestError(_BadRequestError):
    exit_code = 10


class BadEndpointSchemeError(_BadEndpointSchemeError):
    """A ``system_output`` location uses a URI scheme this command
    doesn't know how to upload to."""

    exit_code = 11


class WriteFailedError(_WriteFailedError):
    """``--out`` could not be written (missing parent dir, permission,
    ...)."""

    exit_code = 12


class PrepUploadStateErrorCode(ErrorCode):
    PUS_1 = ErrorDef(
        BadRequestError,
        "unreadable request.yaml {path}: {detail}",
    )
    # ``load_request_or_raise``.

    PUS_2 = ErrorDef(
        BadEndpointSchemeError,
        "unsupported scheme in a system_output location: {detail}",
    )
    # ``build_upload_state_request`` ``UnsupportedSchemeError``.

    PUS_3 = ErrorDef(
        WriteFailedError,
        "failed to write {path}: {detail}",
    )
    # ``write_transporter_request`` ``OSError``.
