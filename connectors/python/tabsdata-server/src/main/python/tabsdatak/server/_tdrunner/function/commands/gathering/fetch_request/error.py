#
# Copyright 2026 Tabsdata Inc.
#

from __future__ import annotations

from tabsdatak.error import ErrorCode, ErrorDef
from tabsdatak.server._tdrunner.function._commons.error.command import (
    CommandRuntimeException,
    WriteFailedError as CommandWriteFailedError,
)


class HttpFetchError(CommandRuntimeException):
    exit_code = 10


class WriteFailedError(CommandWriteFailedError):
    exit_code = 11


class BadPayloadError(CommandRuntimeException):
    exit_code = 12


class FetchRequestErrorCode(ErrorCode):
    FR_1 = ErrorDef(
        HttpFetchError,
        "Cannot fetch the request from {url}: HTTP {status}: {detail}",
    )
    FR_2 = ErrorDef(
        HttpFetchError,
        "Cannot reach {url}: {detail}",
    )
    FR_3 = ErrorDef(
        WriteFailedError,
        "Cannot write the request {path}: {detail}",
    )
    FR_4 = ErrorDef(
        BadPayloadError,
        "Cannot read the {expected} envelope answered by {url}: {detail}",
    )
