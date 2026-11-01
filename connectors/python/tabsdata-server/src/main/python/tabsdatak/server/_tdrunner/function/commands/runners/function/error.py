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
    NoPluginError as CommandNoPluginError,
)


class BadRequestError(CommandBadRequestError):
    exit_code = 10


class BadBundleError(CommandBadBundleError):
    exit_code = 11


class BadUserCodeError(CommandBadUserCodeError):
    exit_code = 12


class BadManifestError(CommandBadManifestError):
    exit_code = 14


class BadInputParquetError(CommandBadInputParquetError):
    exit_code = 15


class NoPluginError(CommandNoPluginError):
    exit_code = 16


class UserFunctionFailedError(CommandRuntimeException):
    exit_code = 17


class OutputShapeMismatchError(CommandRuntimeException):
    exit_code = 18


class OutputWriteFailedError(CommandRuntimeException):
    exit_code = 19


class OutputsManifestWriteFailedError(CommandRuntimeException):
    exit_code = 20


class BadStateInError(CommandBadStateInError):
    exit_code = 21


class BadStateOutError(CommandBadStateOutError):
    exit_code = 21


class RunFunctionErrorCode(ErrorCode):
    RF_1 = ErrorDef(
        BadRequestError,
        "Cannot read the request {path}: {detail}",
    )
    RF_2 = ErrorDef(
        BadBundleError,
        "Cannot load the function definition from {bundle_folder}: {detail}",
    )
    RF_3 = ErrorDef(
        BadUserCodeError,
        "Cannot resolve the user function from {bundle_folder}: {detail}",
    )
    RF_4 = ErrorDef(
        BadBundleError,
        "Cannot run a function of kind {kind!r} from {manifest}; expected {allowed}",
    )
    RF_5 = ErrorDef(
        BadManifestError,
        "Cannot read the inputs manifest {path}: {detail}",
    )
    RF_6 = ErrorDef(
        BadUserCodeError,
        "Cannot resolve the type hints of the user function: {detail}",
    )
    RF_7 = ErrorDef(
        BadUserCodeError,
        (
            "Cannot tell the input mode of parameter {parameter!r}: it must be "
            "annotated TableFrameSpec or TableFramesSpec"
        ),
    )
    RF_8 = ErrorDef(
        NoPluginError,
        "Cannot find a destination plugin for {destination_type}: {detail}",
    )
    RF_9 = ErrorDef(
        BadInputParquetError,
        "Cannot scan the input {path}: {detail}",
    )
    RF_10 = ErrorDef(
        BadManifestError,
        (
            "Cannot match the {entries} inputs manifest entries with the {slots} "
            "input slots the user function declares"
        ),
    )
    RF_11 = ErrorDef(
        BadManifestError,
        "Cannot fill a single-table input slot with {count} files",
    )
    RF_12 = ErrorDef(
        UserFunctionFailedError,
        "Cannot complete the user function: {detail}",
    )
    RF_13 = ErrorDef(
        OutputShapeMismatchError,
        "Cannot use the user function's result: a tuple is expected, not {actual_type}",
    )
    RF_14 = ErrorDef(
        OutputShapeMismatchError,
        (
            "Cannot use the {returned} value(s) the user function returned; it "
            "declares {expected} output table(s) ({tables})"
        ),
    )
    RF_15 = ErrorDef(
        OutputShapeMismatchError,
        (
            "Cannot use output {slot} ({table!r}): a TableFrame or None is "
            "expected, not {actual_type}"
        ),
    )
    RF_16 = ErrorDef(
        OutputWriteFailedError,
        "Cannot write output {table!r} to {path}: {detail}",
    )
    RF_17 = ErrorDef(
        OutputsManifestWriteFailedError,
        "Cannot write the outputs manifest {path}: {detail}",
    )
    RF_18 = ErrorDef(
        BadStateInError,
        "Cannot read the previous state {state_file}: {detail}",
    )
    RF_19 = ErrorDef(
        BadStateOutError,
        "Cannot write the state {state_file}: {detail}",
    )
    RF_20 = ErrorDef(
        BadRequestError,
        "Cannot write output {table!r}: the request gives it no location",
    )
    RF_21 = ErrorDef(
        OutputWriteFailedError,
        "Cannot read the stats of output {table!r} at {path}: {detail}",
    )
