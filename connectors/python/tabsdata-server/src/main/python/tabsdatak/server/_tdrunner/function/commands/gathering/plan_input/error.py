#
# Copyright 2026 Tabsdata Inc.
#

from __future__ import annotations

from tabsdatak.error import ErrorCode, ErrorDef
from tabsdatak.server._tdrunner.function._commons.error.command import (
    BadRequestError as CommandBadRequestError,
    ManifestWriteFailedError as CommandManifestWriteFailedError,
)


class BadRequestError(CommandBadRequestError):
    exit_code = 10


class InputsManifestWriteFailedError(CommandManifestWriteFailedError):
    exit_code = 13


class PlanInputErrorCode(ErrorCode):
    PI_1 = ErrorDef(
        BadRequestError,
        "Cannot read the request {path}: {detail}",
    )
    PI_4 = ErrorDef(
        InputsManifestWriteFailedError,
        "Cannot write the inputs manifest {path}: {detail}",
    )
