#
# Copyright 2026 Tabsdata Inc.
#

from __future__ import annotations

from tabsdatak.error import ErrorCode, ErrorDef
from tabsdatak.server._tdrunner.function._lib.errors import (
    BadBundleError as CommandBadBundleError,
    BadRequestError as CommandBadRequestError,
    BadUserCodeError as CommandBadUserCodeError,
    CommandRuntimeException,
    NoPluginError as CommandNoPluginError,
    PluginFailedError as CommandPluginFailedError,
    WriteFailedError as CommandWriteFailedError,
    WrongKindError as CommandWrongKindError,
)


class BadRequestError(CommandBadRequestError):
    exit_code = 70


class BadBundleError(CommandBadBundleError):
    exit_code = 71


class BadUserCodeError(CommandBadUserCodeError):
    exit_code = 72


class WrongKindError(CommandWrongKindError):
    exit_code = 73


class NoPluginError(CommandNoPluginError):
    exit_code = 74


class PluginFailedError(CommandPluginFailedError):
    exit_code = 75


class UnsupportedStageSchemeError(CommandRuntimeException):
    exit_code = 76


class WriteFailedError(CommandWriteFailedError):
    exit_code = 77


class BadConnectionError(CommandRuntimeException):
    exit_code = 78


class RunStreamSourcePluginErrorCode(ErrorCode):
    RSSP_1 = ErrorDef(
        BadRequestError,
        "Cannot read the request {path}: {detail}",
    )
    RSSP_2 = ErrorDef(
        BadBundleError,
        "Cannot load the function definition from {bundle_folder}: {detail}",
    )
    RSSP_3 = ErrorDef(
        BadUserCodeError,
        "Cannot resolve the user function from {bundle_folder}: {detail}",
    )
    RSSP_4 = ErrorDef(
        WrongKindError,
        "Cannot stream a function of kind {kind!r} from {manifest}; expected {allowed}",
    )
    RSSP_5 = ErrorDef(
        NoPluginError,
        "Cannot find a stream source plugin for {src_type}: {detail}",
    )
    RSSP_6 = ErrorDef(
        PluginFailedError,
        "Cannot complete the stream source plugin: {detail}",
    )
    RSSP_7 = ErrorDef(
        UnsupportedStageSchemeError,
        "Cannot open the function stage {uri}: no stage supports its scheme {scheme!r}",
    )
    RSSP_8 = ErrorDef(
        WriteFailedError,
        "Cannot write the journal {path}: {detail}",
    )
    RSSP_9 = ErrorDef(
        BadConnectionError,
        "Cannot load the connection {path}: {detail}",
    )
