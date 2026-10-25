#
# Copyright 2026 Tabsdata Inc.
#

from __future__ import annotations

from tabsdatak.error import ErrorCode, ErrorDef
from tabsdatak.server._tdrunner.function._lib.errors import CommandRuntimeException


class TransporterFailedError(CommandRuntimeException):
    exit_code = 12


class LegacyLocalizeFnBundleErrorCode(ErrorCode):
    LFB_2 = ErrorDef(
        TransporterFailedError,
        "td_transporter not found: {detail}",
    )
    LFB_3 = ErrorDef(
        TransporterFailedError,
        "td_transporter exited {rc} fetching bundle",
    )
