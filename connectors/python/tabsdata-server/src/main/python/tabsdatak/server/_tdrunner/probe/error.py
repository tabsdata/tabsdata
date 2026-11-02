#
# Copyright 2026 Tabsdata Inc.
#

"""Error model for the probe command (exit codes 10..)."""

from __future__ import annotations

from tabsdatak.error import ErrorCode, ErrorDef
from tabsdatak.server._tdrunner.function._lib.errors import (
    BadRequestError as _BadRequestError,
)


class BadProbeRequestError(_BadRequestError):
    """``--request`` is unreadable or not a probe request."""

    exit_code = 10


class ProbeErrorCode(ErrorCode):
    PRB_1 = ErrorDef(
        BadProbeRequestError,
        "invalid probe request {path}: {detail}",
    )
