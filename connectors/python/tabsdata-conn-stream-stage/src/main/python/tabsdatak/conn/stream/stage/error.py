#
# Copyright 2026 Tabsdata Inc.
#

from __future__ import annotations

from tabsdatak.conn.common.error import ConnException
from tabsdatak.error import (
    ErrorCode,
    ErrorDef,
)


class ManifestStageException(ConnException):
    """Runtime failure while reading a stream-stage manifest.

    :categories: error
    """


class ManifestStageErrorCode(ErrorCode):
    """Error codes for the stream-stage connector.

    :categories: error

    One variant per validation / runtime site. Raise via
    `ManifestStageErrorCode.MANIFEST_STAGE_N.exception`.
    """

    MANIFEST_STAGE_4 = ErrorDef(
        ManifestStageException,
        "Cannot list the stream manifests matching '{pattern}': {reason}",
    )  # manifest listing
    MANIFEST_STAGE_6 = ErrorDef(
        ManifestStageException,
        "Stream manifest '{path}' is not valid JSON",
    )  # manifest read / json parse
    MANIFEST_STAGE_7 = ErrorDef(
        ManifestStageException,
        "Stream manifest '{path}' must be a JSON list of lists of file paths",
    )  # manifest shape validation
