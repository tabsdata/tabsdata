#
# Copyright 2026 Tabs Data Inc.
#

from __future__ import annotations

from tabsdatak.conn.common.error import (
    ConnException,
    ConnInitException,
)
from tabsdatak.error import (
    ErrorCode,
    ErrorDef,
)


class ManifestStageException(ConnException):
    """Runtime failure while reading a stream-stage manifest."""


class ManifestStageErrorCode(ErrorCode):
    MANIFEST_STAGE_1 = ErrorDef(
        ConnInitException,
        "ManifestStageConn requires exactly one backend connection "
        "(local / s3 / az / gcs) to be set; got {count}",
    )  # _active_backend
    MANIFEST_STAGE_2 = ErrorDef(
        ManifestStageException,
        "Manifest-stage credential resolution failed: {reason}",
    )  # run_env resolution
    MANIFEST_STAGE_3 = ErrorDef(
        ManifestStageException,
        "td_transporter binary not found while fetching the stream manifest",
    )  # binary lookup
    MANIFEST_STAGE_4 = ErrorDef(
        ManifestStageException,
        "Failed to fetch the stream manifest matching '{pattern}'",
    )  # wildcard-copy run failure
    MANIFEST_STAGE_5 = ErrorDef(
        ManifestStageException,
        "Invalid transporter report fetching the stream manifest matching '{pattern}'",
    )  # wildcard-copy report parse failure
    MANIFEST_STAGE_6 = ErrorDef(
        ManifestStageException,
        "Stream manifest '{path}' is not valid JSON",
    )  # manifest read / json parse
    MANIFEST_STAGE_7 = ErrorDef(
        ManifestStageException,
        "Stream manifest '{path}' must be a JSON list of lists of file paths",
    )  # manifest shape validation
    MANIFEST_STAGE_8 = ErrorDef(
        ManifestStageException,
        "Failed to fetch the data files listed in stream manifest '{path}'",
    )  # data-file copy run failure
    MANIFEST_STAGE_9 = ErrorDef(
        ManifestStageException,
        "Invalid transporter report fetching the data files listed in "
        "stream manifest '{path}'",
    )  # data-file copy report parse failure
