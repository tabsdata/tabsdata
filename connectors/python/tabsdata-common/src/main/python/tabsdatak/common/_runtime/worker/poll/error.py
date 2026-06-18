#
# Copyright 2026 Tabs Data Inc.
#

from __future__ import annotations

from tabsdatak.common._runtime.worker.error import (
    AbortedError,
    ExitCode,
    WorkerRuntimeException,
)
from tabsdatak.error import ErrorCode, ErrorDef


class PollTimeoutError(WorkerRuntimeException):
    exit_code = ExitCode.RUN


class PollErrorCode(ErrorCode):
    POLL_1 = ErrorDef(
        PollTimeoutError,
        "The condition did not become ready within {timeout} seconds.",
    )
    POLL_2 = ErrorDef(
        AbortedError,
        "The poll was aborted by signal {signal} before becoming ready.",
    )
