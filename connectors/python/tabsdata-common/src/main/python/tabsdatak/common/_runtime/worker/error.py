#
# Copyright 2026 Tabsdata Inc.
#

from __future__ import annotations

from enum import IntEnum

from tabsdatak.error import ErrorCode, ErrorDef, TabsdataException


class ExitCode(IntEnum):
    SUCCESS = 0
    INPUT = 1
    RUN = 2
    OUTPUT = 3
    INCONSISTENT = 4
    PREMATURE = 5
    UNEXPECTED = 6
    ABORTED = 7


class WorkerException(TabsdataException):
    exit_code: ExitCode


class WorkerInputException(WorkerException):
    pass


class WorkerRuntimeException(WorkerException):
    pass


class WorkerInternalException(WorkerException):
    pass


class SubprocessError(WorkerRuntimeException):
    exit_code = ExitCode.RUN


class AbortedError(WorkerInternalException):
    exit_code = ExitCode.ABORTED


class WorkerErrorCode(ErrorCode):
    WORKER_1 = ErrorDef(
        SubprocessError,
        "The subprocess {argv!r} could not be launched: {detail}.",
    )
    WORKER_2 = ErrorDef(
        SubprocessError,
        "The subprocess {argv!r} exited with status {status}.",
    )
    WORKER_3 = ErrorDef(
        SubprocessError,
        "The subprocess {argv!r} timed out after {timeout} seconds.",
    )
    WORKER_4 = ErrorDef(
        AbortedError,
        "The worker was aborted by signal {signal} before completion.",
    )
