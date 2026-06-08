#
# Copyright 2026 Tabs Data Inc.
#

"""Error model for the prep-upload-state command (exit codes 10..)."""

from __future__ import annotations

from tabsdatak.error import ErrorCode, ErrorDef
from tabsdatak.server._tdrunner.function._lib.errors import (
    CommandRuntimeException,
)


class BadRequestError(CommandRuntimeException):
    """``--request`` file missing, unreadable, or malformed."""

    exit_code = 10


class BadEndpointSchemeError(CommandRuntimeException):
    """A ``system_output`` location uses a URI scheme this command
    doesn't know how to upload to."""

    exit_code = 11


class WriteFailedError(CommandRuntimeException):
    """``--out`` could not be written (missing parent dir, permission,
    ...)."""

    exit_code = 12


class PrepUploadStateErrorCode(ErrorCode):
    PUS_1 = ErrorDef(
        BadRequestError,
        "unreadable request.yaml: {detail}",
    )
    # Raised by ``main()`` when ``load_request`` fails (file missing or
    # malformed YAML / Pydantic validation error).

    PUS_2 = ErrorDef(
        BadEndpointSchemeError,
        "unsupported scheme in a system_output location: {detail}",
    )
    # Raised when ``build_upload_state_request`` rejects a location URI
    # (``_lib.endpoint.UnsupportedSchemeError``).

    PUS_3 = ErrorDef(
        WriteFailedError,
        "failed to write {path}: {detail}",
    )
    # Raised when ``write_transporter_request`` hits an ``OSError``.
