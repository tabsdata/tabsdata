#
# Copyright 2026 Tabsdata Inc.
#

from __future__ import annotations

from tabsdatak.error import ErrorCode, ErrorDef
from tabsdatak.server._tdrunner.function._commons.error.command import (
    BadBundleError as CommandBadBundleError,
    BadRequestError as CommandBadRequestError,
    BadStateInError as CommandBadStateInError,
    BadStateOutError as CommandBadStateOutError,
    BadUserCodeError as CommandBadUserCodeError,
    CommandRuntimeException,
    ManifestWriteFailedError as CommandManifestWriteFailedError,
    NoPluginError as CommandNoPluginError,
    PluginFailedError as CommandPluginFailedError,
    WrongKindError as CommandWrongKindError,
)


class BadRequestError(CommandBadRequestError):
    exit_code = 10


class BadBundleError(CommandBadBundleError):
    exit_code = 11


class BadUserCodeError(CommandBadUserCodeError):
    exit_code = 12


class WrongKindError(CommandWrongKindError):
    exit_code = 13


class NoPluginError(CommandNoPluginError):
    exit_code = 14


class PluginFailedError(CommandPluginFailedError):
    exit_code = 15


class ManifestWriteFailedError(CommandManifestWriteFailedError):
    exit_code = 16


class BadStateInError(CommandBadStateInError):
    exit_code = 17


class BadStateOutError(CommandBadStateOutError):
    exit_code = 17


class UnsupportedStageSchemeError(CommandRuntimeException):
    exit_code = 18


class BadConnectionError(CommandRuntimeException):
    exit_code = 19


class RunSourcePluginErrorCode(ErrorCode):
    RSP_1 = ErrorDef(
        BadRequestError,
        "Cannot read the request {path}: {detail}",
    )
    RSP_2 = ErrorDef(
        BadBundleError,
        "Cannot load the function definition from {bundle_folder}: {detail}",
    )
    RSP_3 = ErrorDef(
        BadUserCodeError,
        "Cannot resolve the user function from {bundle_folder}: {detail}",
    )
    RSP_4 = ErrorDef(
        WrongKindError,
        "Cannot run a function of kind {kind!r} from {manifest}; expected {allowed}",
    )
    RSP_5 = ErrorDef(
        NoPluginError,
        "Cannot find a source plugin for {source_type}: {detail}",
    )
    RSP_6 = ErrorDef(
        PluginFailedError,
        "Cannot read the inputs through the source plugin: {detail}",
    )
    RSP_7 = ErrorDef(
        PluginFailedError,
        (
            "Cannot use the {returned} input slot(s) the source plugin returned; "
            "the source declares {expected}"
        ),
    )
    RSP_8 = ErrorDef(
        ManifestWriteFailedError,
        "Cannot write the inputs manifest {path}: {detail}",
    )
    RSP_9 = ErrorDef(
        BadStateInError,
        "Cannot read the previous state {state_file}: {detail}",
    )
    RSP_10 = ErrorDef(
        BadStateOutError,
        "Cannot write the state {state_file}: {detail}",
    )
    RSP_11 = ErrorDef(
        UnsupportedStageSchemeError,
        ("Cannot read the stream stage {uri}: its scheme {scheme!r} is not supported"),
    )
    RSP_12 = ErrorDef(
        BadConnectionError,
        "Cannot load the connection {path}: {detail}",
    )
