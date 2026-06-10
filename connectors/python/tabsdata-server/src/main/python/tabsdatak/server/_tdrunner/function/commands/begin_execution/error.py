#
# Copyright 2026 Tabs Data Inc.
#

"""Error model for the begin-execution command (exit codes 10..)."""

from __future__ import annotations

from tabsdatak.error import ErrorCode, ErrorDef
from tabsdatak.server._tdrunner.function._lib.errors import (
    WriteFailedError as _WriteFailedError,
)


class WriteFailedError(_WriteFailedError):
    """``--execution-meta-file`` could not be written (missing parent
    dir, permission, ...)."""

    exit_code = 10


class BeginExecutionErrorCode(ErrorCode):
    BE_1 = ErrorDef(
        WriteFailedError,
        "failed to write {path}: {detail}",
    )
    # Raised when ``Journal.save`` hits an ``OSError``.
