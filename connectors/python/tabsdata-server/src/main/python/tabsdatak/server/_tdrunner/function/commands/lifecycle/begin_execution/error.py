#
# Copyright 2026 Tabsdata Inc.
#

from __future__ import annotations

from tabsdatak.error import ErrorCode, ErrorDef
from tabsdatak.server._tdrunner.function._commons.error.command import (
    WriteFailedError as CommandWriteFailedError,
)


class WriteFailedError(CommandWriteFailedError):
    exit_code = 10


class BeginExecutionErrorCode(ErrorCode):
    BE_1 = ErrorDef(
        WriteFailedError,
        "Cannot write the journal {path}: {detail}",
    )
