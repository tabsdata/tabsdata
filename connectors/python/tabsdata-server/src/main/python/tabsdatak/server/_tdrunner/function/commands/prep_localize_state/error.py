#
# Copyright 2026 Tabs Data Inc.
#

"""Error model for the prep-localize-state command (exit codes 10..)."""

from __future__ import annotations

from tabsdatak.error import ErrorCode, ErrorDef
from tabsdatak.server._tdrunner.function._lib.errors import (
    CommandRuntimeException,
)


class BadRequestError(CommandRuntimeException):
    """``--request`` file missing, unreadable, or malformed."""

    exit_code = 10


class BadEndpointSchemeError(CommandRuntimeException):
    """An input or state-in location uses a URI scheme this command
    doesn't know how to fetch from."""

    exit_code = 11


class WriteFailedError(CommandRuntimeException):
    """``--out`` (the transporter request YAML) could not be written."""

    exit_code = 12


class InputsManifestWriteFailedError(CommandRuntimeException):
    """``--inputs-manifest`` could not be written."""

    exit_code = 13


class StateManifestWriteFailedError(CommandRuntimeException):
    """``--state-manifest`` could not be written."""

    exit_code = 14


class PrepLocalizeStateErrorCode(ErrorCode):
    PLS_1 = ErrorDef(
        BadRequestError,
        "unreadable request.yaml {path}: {detail}",
    )
    # ``load_request_or_raise``.

    PLS_2 = ErrorDef(
        BadEndpointSchemeError,
        "unsupported scheme in an input/state location: {detail}",
    )
    # ``build_localize_state_request`` ``UnsupportedSchemeError``.

    PLS_3 = ErrorDef(
        WriteFailedError,
        "failed to write {path}: {detail}",
    )
    # ``write_transporter_request`` ``OSError`` on ``--out``.

    PLS_4 = ErrorDef(
        InputsManifestWriteFailedError,
        "failed to write {path}: {detail}",
    )
    # ``write_inputs_manifest`` ``OSError``.

    PLS_5 = ErrorDef(
        StateManifestWriteFailedError,
        "failed to write {path}: {detail}",
    )
    # ``write_state_manifest`` ``OSError``.
