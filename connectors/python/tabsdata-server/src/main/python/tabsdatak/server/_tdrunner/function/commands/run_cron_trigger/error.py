#
# Copyright 2026 Tabsdata Inc.
#

"""Error model for the run-cron-trigger command (exit codes 82..)."""

from __future__ import annotations

from tabsdatak.error import ErrorCode, ErrorDef
from tabsdatak.server._tdrunner.function._lib.errors import (
    CommandRuntimeException,
)


class CronTriggerConnectionError(CommandRuntimeException):
    """The POST to the function execute endpoint could not reach the
    apiserver (connection refused, DNS failure, timeout) within the retry
    budget. The CronJob fire is marked failed so Kubernetes surfaces it; the
    next tick fires again."""

    exit_code = 82


class CronTriggerRejectedError(CommandRuntimeException):
    """The apiserver returned a non-2xx response to the execute POST. The
    fire is marked failed so the rejection is visible rather than silently
    swallowed (bad token scope, disabled function, server error, ...)."""

    exit_code = 83


class RunCronTriggerErrorCode(ErrorCode):
    CT_1 = ErrorDef(
        CronTriggerConnectionError,
        "cron trigger POST to {url} failed to connect: {detail}",
    )
    # HttpRequestError raised by invoke().

    CT_2 = ErrorDef(
        CronTriggerRejectedError,
        "cron trigger POST to {url} rejected (status {status}): {body}",
    )
    # Non-2xx response from the execute endpoint.
