#
# Copyright 2026 Tabs Data Inc.
#

"""Error model for the run-manifest-trigger sidecar (exit codes 80..)."""

from __future__ import annotations

from tabsdatak.error import ErrorCode, ErrorDef
from tabsdatak.server._tdrunner.function._lib.errors import (
    BadRequestError as _BadRequestError,
    CommandRuntimeException,
)


class BadRequestError(_BadRequestError):
    """``--request`` points at an unreadable / malformed request.yaml."""

    exit_code = 80


class UnsupportedStageSchemeError(CommandRuntimeException):
    """``info.function_stage.uri`` uses a scheme the sidecar does not
    yet support (today: only ``file://``).

    The sidecar lives in the same pod as the streamer; cloud schemes
    will land once the streamer pod's deployment requires them, in
    lockstep with ``StreamerPluginCtx``'s own scheme support.
    """

    exit_code = 81


class RunManifestTriggerErrorCode(ErrorCode):
    MT_1 = ErrorDef(
        BadRequestError,
        "unreadable request.yaml {path}: {detail}",
    )
    # ``load_request_or_raise`` failure on startup.

    MT_2 = ErrorDef(
        UnsupportedStageSchemeError,
        "sidecar cannot resolve function_stage URI {uri} (scheme {scheme!r}): "
        "only file:// is supported today",
    )
    # Raised when ``info.function_stage.uri`` is not file://.
