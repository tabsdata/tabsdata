#
# Copyright 2026 Tabsdata Inc.
#

from __future__ import annotations

from tabsdatak.error import ErrorCode, ErrorDef
from tabsdatak.server._tdrunner.function._commons.error.command import (
    BadBundleError as CommandBadBundleError,
    BadInputParquetError as CommandBadInputParquetError,
    BadManifestError as CommandBadManifestError,
    BadRequestError as CommandBadRequestError,
    BadStateInError as CommandBadStateInError,
    BadStateOutError as CommandBadStateOutError,
    BadUserCodeError as CommandBadUserCodeError,
    CommandRuntimeException,
    ManifestWriteFailedError as CommandManifestWriteFailedError,
    NoPluginError as CommandNoPluginError,
    PluginFailedError as CommandPluginFailedError,
    WriteFailedError as CommandWriteFailedError,
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


class BadManifestError(CommandBadManifestError):
    exit_code = 16


class OutputCopyFailedError(CommandWriteFailedError):
    exit_code = 17


class OutputReadFailedError(CommandBadInputParquetError):
    exit_code = 18


class BadConnectionError(CommandRuntimeException):
    exit_code = 19


class BadStateInError(CommandBadStateInError):
    exit_code = 20


class BadStateOutError(CommandBadStateOutError):
    exit_code = 21


class OutputsManifestWriteFailedError(CommandManifestWriteFailedError):
    exit_code = 22


class RunDestinationPluginErrorCode(ErrorCode):
    RDP_1 = ErrorDef(
        BadRequestError,
        "Cannot read the request {path}: {detail}",
    )
    RDP_2 = ErrorDef(
        BadBundleError,
        "Cannot load the function definition from {bundle_folder}: {detail}",
    )
    RDP_3 = ErrorDef(
        BadUserCodeError,
        "Cannot resolve the user function from {bundle_folder}: {detail}",
    )
    RDP_4 = ErrorDef(
        WrongKindError,
        "Cannot run a function of kind {kind!r} from {manifest}; expected {allowed}",
    )
    RDP_5 = ErrorDef(
        NoPluginError,
        "Cannot find a destination plugin for {destination_type}: {detail}",
    )
    RDP_6 = ErrorDef(
        PluginFailedError,
        "Cannot write the outputs through the destination plugin: {detail}",
    )
    RDP_7 = ErrorDef(
        BadManifestError,
        "Cannot read the outputs manifest {path}: {detail}",
    )
    RDP_8 = ErrorDef(
        BadManifestError,
        (
            "Cannot hand over output {slot} ({name!r}): the subscriber returned no "
            "frame for it, and a destination needs one for every output"
        ),
    )
    RDP_9 = ErrorDef(
        OutputCopyFailedError,
        "Cannot copy the landed output {slot} from {path}: {detail}",
    )
    RDP_10 = ErrorDef(
        OutputReadFailedError,
        "Cannot read the landed output {slot} from {path}: {detail}",
    )
    RDP_11 = ErrorDef(
        BadConnectionError,
        "Cannot load the connection {path}: {detail}",
    )
    RDP_12 = ErrorDef(
        BadStateInError,
        "Cannot read the state {state_file}: {detail}",
    )
    RDP_13 = ErrorDef(
        BadStateOutError,
        "Cannot write the state {state_file}: {detail}",
    )
    RDP_14 = ErrorDef(
        BadStateOutError,
        "Cannot store the state {state_file} at {path}: {detail}",
    )
    RDP_15 = ErrorDef(
        OutputsManifestWriteFailedError,
        "Cannot write the outputs manifest {path}: {detail}",
    )
