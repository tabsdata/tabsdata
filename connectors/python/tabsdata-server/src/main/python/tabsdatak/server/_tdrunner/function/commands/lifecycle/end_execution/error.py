#
# Copyright 2026 Tabsdata Inc.
#

from __future__ import annotations

from tabsdatak.error import ErrorCode, ErrorDef
from tabsdatak.server._tdrunner.function._commons.error.command import (
    BadManifestError as CommandBadManifestError,
    BadRequestError as CommandBadRequestError,
    ManifestMismatchError as CommandManifestMismatchError,
    WriteFailedError as CommandWriteFailedError,
)


class BadRequestError(CommandBadRequestError):
    exit_code = 10


class BadManifestError(CommandBadManifestError):
    exit_code = 11


class ManifestMismatchError(CommandManifestMismatchError):
    exit_code = 12


class WriteFailedError(CommandWriteFailedError):
    exit_code = 13


class EndExecutionErrorCode(ErrorCode):
    EE_1 = ErrorDef(
        BadRequestError,
        "Cannot read the request {path}: {detail}",
    )
    EE_2 = ErrorDef(
        BadManifestError,
        "Cannot read the outputs manifest {path}: {detail}",
    )
    EE_3 = ErrorDef(
        ManifestMismatchError,
        (
            "Cannot report output table {table!r}: the request declares it, but "
            "the outputs manifest has no entry for it"
        ),
    )
    EE_4 = ErrorDef(
        WriteFailedError,
        "Cannot write {path}: {detail}",
    )
