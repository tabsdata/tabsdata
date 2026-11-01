#
# Copyright 2026 Tabsdata Inc.
#

from __future__ import annotations

from tabsdatak.error import ErrorCode, ErrorDef
from tabsdatak.server._tdrunner.function._commons.error.command import (
    CommandRuntimeException,
)


class CronTriggerConnectionError(CommandRuntimeException):
    exit_code = 82


class CronTriggerRejectedError(CommandRuntimeException):
    exit_code = 83


class RunCronTriggerErrorCode(ErrorCode):
    CT_1 = ErrorDef(
        CronTriggerConnectionError,
        "Cannot reach the execute endpoint {url}: {detail}",
    )
    CT_2 = ErrorDef(
        CronTriggerRejectedError,
        "Cannot schedule the execution: {url} answered {status}: {body}",
    )
