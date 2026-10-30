#
# Copyright 2026 Tabsdata Inc.
#

from __future__ import annotations

from tabsdatak.common._runtime.worker.error import ExitCode, WorkerRuntimeException
from tabsdatak.error import ErrorCode, ErrorDef


class ResourceExtractionError(WorkerRuntimeException):
    exit_code = ExitCode.OUTPUT


class CxAssetsErrorCode(ErrorCode):
    CX_ASSETS_1 = ErrorDef(
        ResourceExtractionError,
        "Could not extract {count} connector icon(s): {details}.",
    )
