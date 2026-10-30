#
# Copyright 2026 Tabsdata Inc.
#

from __future__ import annotations

from tabsdatak.error import ErrorCode, ErrorDef
from tabsdatak.server._tdrunner.function._lib.errors import (
    BadBundleError as CommandBadBundleError,
    BadManifestError as CommandBadManifestError,
    BadRequestError as CommandBadRequestError,
    BadUserCodeError as CommandBadUserCodeError,
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
