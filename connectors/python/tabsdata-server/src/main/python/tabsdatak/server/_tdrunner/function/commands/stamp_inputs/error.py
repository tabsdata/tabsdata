#
# Copyright 2026 Tabsdata Inc.
#

"""Error model for the stamp-inputs command (exit codes 10..)."""

from __future__ import annotations

from tabsdatak.error import ErrorCode, ErrorDef
from tabsdatak.server._tdrunner.function._lib.errors import (
    BadInputParquetError as _BadInputParquetError,
    BadManifestError as _BadManifestError,
    BadRequestError as _BadRequestError,
    CommandRuntimeException,
    ManifestWriteFailedError as _ManifestWriteFailedError,
)


class BadRequestError(_BadRequestError):
    exit_code = 10


class BadManifestError(_BadManifestError):
    """``--inputs-manifest`` file missing, unreadable, or malformed."""

    exit_code = 11


class BadInputParquetError(_BadInputParquetError):
    """An input parquet listed in the manifest can't be read."""

    exit_code = 12


class StampFailedError(CommandRuntimeException):
    """Stamping system columns failed, or writing the enriched parquet
    failed."""

    exit_code = 13


class ManifestWriteFailedError(_ManifestWriteFailedError):
    """``--enriched-inputs-manifest`` could not be written."""

    exit_code = 14


class StampInputsErrorCode(ErrorCode):
    SI_1 = ErrorDef(
        BadRequestError,
        "unreadable request.yaml {path}: {detail}",
    )
    # ``load_request_or_raise``.

    SI_2 = ErrorDef(
        BadManifestError,
        "could not load inputs manifest {path}: {detail}",
    )
    # ``load_inputs_manifest_or_raise``.

    SI_3 = ErrorDef(
        BadInputParquetError,
        "failed to read input parquet {path}: {detail}",
    )
    # ``_stamp_file`` ``pl.read_parquet`` failure.

    SI_4 = ErrorDef(
        StampFailedError,
        "failed to stamp system columns on {path}: {detail}",
    )
    # ``_stamp_file`` ``stamp_system_columns`` failure.

    SI_5 = ErrorDef(
        StampFailedError,
        "failed to write enriched parquet {path}: {detail}",
    )
    # ``_stamp_file`` ``stamped.write_parquet`` ``OSError``. Same
    # exit code as SI_4 because both are "the stamp step failed".

    SI_6 = ErrorDef(
        ManifestWriteFailedError,
        "failed to write enriched inputs manifest {path}: {detail}",
    )
    # ``write_inputs_manifest`` ``OSError``.
