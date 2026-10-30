#
# Copyright 2026 Tabsdata Inc.
#

"""Error model for the manifest-trigger sidecar (exit codes 80..)."""

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


class UnknownTriggerOutcomeError(CommandRuntimeException):
    """``fn_trigger`` answered 200 with a ``TriggerOutcome`` this sidecar
    does not know, so whether the manifest's files may be deleted is
    undecided. The sidecar stops instead of guessing."""

    exit_code = 82


class MalformedTriggerAnswerError(CommandRuntimeException):
    """``fn_trigger`` answered 200 with a body that carries no
    ``data.outcome``: the answer's shape changed, which no retry mends."""

    exit_code = 83


class RunManifestTriggerErrorCode(ErrorCode):
    MT_1 = ErrorDef(
        BadRequestError,
        "unreadable request.yaml {path}: {detail}",
    )
    # ``Request.load`` failure on startup.

    MT_2 = ErrorDef(
        UnsupportedStageSchemeError,
        "sidecar cannot resolve function_stage URI {uri} (scheme {scheme!r}): "
        "only file:// is supported today",
    )
    # Raised when ``info.function_stage.uri`` is not file://.

    MT_3 = ErrorDef(
        UnknownTriggerOutcomeError,
        "fn_trigger answered unknown outcome {outcome!r}: the apiserver's "
        "TriggerOutcome has a variant this sidecar does not handle",
    )
    # Raised when a 200 names an outcome missing from ``TriggerOutcome``.

    MT_4 = ErrorDef(
        MalformedTriggerAnswerError,
        "fn_trigger answered 200 without a data.outcome; body starts {body!r}",
    )
    # Raised when a 200 body is not JSON or lacks ``data.outcome``.
