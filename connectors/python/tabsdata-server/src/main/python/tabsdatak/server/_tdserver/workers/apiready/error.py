#
# Copyright 2026 Tabs Data Inc.
#

from __future__ import annotations

from tabsdatak.common._runtime.worker.error import ExitCode, WorkerInputException
from tabsdatak.error import ErrorCode, ErrorDef


class MissingAddressesError(WorkerInputException):
    exit_code = ExitCode.INPUT


class ApiReadyErrorCode(ErrorCode):
    API_READY_1 = ErrorDef(
        MissingAddressesError,
        "No '{key}' entry was found in the apiserver configuration.",
    )
