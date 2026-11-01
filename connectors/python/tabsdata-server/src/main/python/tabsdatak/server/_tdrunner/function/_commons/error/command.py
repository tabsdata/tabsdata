#
# Copyright 2026 Tabsdata Inc.
#

from __future__ import annotations

from tabsdatak.error import (
    ErrorCode,
    ErrorDef,
    TabsdataException,
)

EXIT_INVALID_OPTIONS: int = 1
EXIT_UNDEFINED_ENV: int = 2
EXIT_UNEXPECTED: int = 3
EXIT_INCONSISTENT: int = 4
EXIT_PREMATURE: int = 5
EXIT_BAD_CONFIG: int = 6
EXIT_STALE_REPORT: int = 7
EXIT_BAD_STREAM_STATE: int = 8


class CommandException(TabsdataException):
    exit_code: int


class CommandInitException(CommandException):
    pass


class CommandRuntimeException(CommandException):
    pass


class CommandInternalException(CommandException):
    pass


class InvalidOptionsError(CommandInitException):
    exit_code = EXIT_INVALID_OPTIONS


class UndefinedEnvError(CommandRuntimeException):
    exit_code = EXIT_UNDEFINED_ENV


class StaleReportError(CommandRuntimeException):
    exit_code = EXIT_STALE_REPORT


class UnexpectedError(CommandInternalException):
    exit_code = EXIT_UNEXPECTED


class InconsistentError(CommandInternalException):
    exit_code = EXIT_INCONSISTENT


class PrematureError(CommandInternalException):
    exit_code = EXIT_PREMATURE


class BadRequestError(CommandRuntimeException):
    pass


class BadBundleError(CommandRuntimeException):
    pass


class BadUserCodeError(CommandInitException):
    pass


class WrongKindError(CommandRuntimeException):
    pass


class BadManifestError(CommandRuntimeException):
    pass


class ManifestMismatchError(CommandRuntimeException):
    pass


class ManifestWriteFailedError(CommandRuntimeException):
    pass


class WriteFailedError(CommandRuntimeException):
    pass


class NoPluginError(CommandRuntimeException):
    pass


class PluginFailedError(CommandRuntimeException):
    pass


class BadInputParquetError(CommandRuntimeException):
    pass


class BadStateInError(CommandRuntimeException):
    pass


class BadStateOutError(CommandRuntimeException):
    pass


class BadStreamStateError(CommandRuntimeException):
    exit_code = EXIT_BAD_STREAM_STATE


class BadStorageConfigError(CommandRuntimeException):
    exit_code = EXIT_BAD_CONFIG


class CommandErrorCode(ErrorCode):
    CMD_1 = ErrorDef(
        InvalidOptionsError,
        "invalid or missing options: {detail}",
    )
    CMD_2 = ErrorDef(
        UndefinedEnvError,
        "environment variable {var!r} referenced via env: but not defined",
    )
    CMD_3 = ErrorDef(
        BadStorageConfigError,
        "The storage configuration at '{path}' could not be read: {detail}.",
    )
    CMD_4 = ErrorDef(
        BadStorageConfigError,
        "The cloud location '{uri}' references the mount '{mount}', which is "
        "not present in the storage configuration.",
    )
    CMD_5 = ErrorDef(
        UnexpectedError,
        "The transporter mode '{mode}' is not supported.",
    )
    CMD_6 = ErrorDef(
        BadStreamStateError,
        "The stream state {file} could not be read ({detail}); the stream does not "
        "start, since a failed read cannot be told from a first run. Fix the cause "
        "and restart, or start anew by resetting the function's stage "
        "(tdk fn update --reset-stage)",
    )
    CMD_7 = ErrorDef(
        BadStreamStateError,
        "The stream state {file} is not what the streamer writes ({detail}); the "
        "stream does not start from it. To start anew, reset the function's stage "
        "(tdk fn update --reset-stage)",
    )
    CMD_8 = ErrorDef(
        StaleReportError,
        "The apiserver answered the {status} report as stale; the attempt was "
        "superseded or had already ended, so this worker stops.",
    )
