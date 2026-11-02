#
# Copyright 2026 Tabsdata Inc.
#

from __future__ import annotations

from tabsdatak.error import ErrorCode, ErrorDef
from tabsdatak.server._tdrunner.function._commons.error.command import (
    BadRequestError as CommandBadRequestError,
)


class BadRequestError(CommandBadRequestError):
    exit_code = 10


class ConnectionProbeErrorCode(ErrorCode):
    CP_1 = ErrorDef(
        BadRequestError,
        "Cannot read the probe request {path}: {detail}",
    )
