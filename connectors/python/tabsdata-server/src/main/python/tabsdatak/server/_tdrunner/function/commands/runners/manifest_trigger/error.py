#
# Copyright 2026 Tabsdata Inc.
#

from __future__ import annotations

from tabsdatak.error import ErrorCode, ErrorDef
from tabsdatak.server._tdrunner.function._lib.errors import (
    BadRequestError as CommandBadRequestError,
    CommandRuntimeException,
)


class BadRequestError(CommandBadRequestError):
    exit_code = 80


class UnsupportedStageSchemeError(CommandRuntimeException):
    exit_code = 81


class UnknownTriggerOutcomeError(CommandRuntimeException):
    exit_code = 82


class MalformedTriggerAnswerError(CommandRuntimeException):
    exit_code = 83


class RunManifestTriggerErrorCode(ErrorCode):
    MT_1 = ErrorDef(
        BadRequestError,
        "Cannot read the request {path}: {detail}",
    )
    MT_2 = ErrorDef(
        UnsupportedStageSchemeError,
        "Cannot open the function stage {uri}: no stage supports its scheme {scheme!r}",
    )
    MT_3 = ErrorDef(
        UnknownTriggerOutcomeError,
        (
            "Cannot act on the trigger outcome {outcome!r}: the apiserver's "
            "TriggerOutcome has a variant this sidecar does not handle"
        ),
    )
    MT_4 = ErrorDef(
        MalformedTriggerAnswerError,
        "Cannot read the trigger answer: it has no data.outcome; body starts {body!r}",
    )
