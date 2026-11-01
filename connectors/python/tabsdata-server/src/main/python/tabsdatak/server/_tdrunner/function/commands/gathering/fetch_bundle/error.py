#
# Copyright 2026 Tabsdata Inc.
#

from __future__ import annotations

from tabsdatak.error import ErrorCode, ErrorDef
from tabsdatak.server._tdrunner.function._commons.error.command import (
    BadRequestError as CommandBadRequestError,
    CommandRuntimeException,
)


class BadRequestError(CommandBadRequestError):
    exit_code = 10


class StorageConfigurationError(CommandRuntimeException):
    exit_code = 11


class DownloadFailedError(CommandRuntimeException):
    exit_code = 12


class ExtractFailedError(CommandRuntimeException):
    exit_code = 13


class FetchBundleErrorCode(ErrorCode):
    FB_1 = ErrorDef(
        BadRequestError,
        "Cannot read the request {path}: {detail}",
    )
    FB_2 = ErrorDef(
        StorageConfigurationError,
        "Cannot resolve the storage of the bundle: {detail}",
    )
    FB_3 = ErrorDef(
        DownloadFailedError,
        "Cannot download the bundle: {detail}",
    )
    FB_4 = ErrorDef(
        ExtractFailedError,
        "Cannot extract the bundle: {detail}",
    )
