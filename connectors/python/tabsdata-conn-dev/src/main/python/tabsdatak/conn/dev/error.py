#
# Copyright 2026 Tabsdata Inc.
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


class DevValidateException(ConnException):
    """Cross-field validation failure for the Development connectors.

    :categories: error
    """


class DevRuntimeException(ConnException):
    """Runtime failure (plugin / I/O) for the Development connectors.

    :categories: error
    """


class DevErrorCode(ErrorCode):
    """Error codes for the Development connectors.

    :categories: error

    One variant per validation / runtime site. Raise via
    `DevErrorCode.DEV_N.exception`.
    """

    DEV_1 = ErrorDef(
        ConnInitException,
        "DataGenSrcConn validation failed",
    )  # DataGenSrcConn
    DEV_2 = ErrorDef(
        ConnInitException,
        "DataGenSrc validation failed",
    )  # DataGenSrc
    DEV_3 = ErrorDef(
        ConnInitException,
        "TrashDestConn validation failed",
    )  # TrashDestConn
    DEV_4 = ErrorDef(
        ConnInitException,
        "TrashDest validation failed",
    )  # TrashDest
    DEV_5 = ErrorDef(
        DevValidateException,
        "Data expects a polars or pandas DataFrame (got {type})",
    )  # Data.__init__
    DEV_6 = ErrorDef(
        DevValidateException,
        "Data.chunk_size must be a positive integer (got {value})",
    )  # Data.__init__
    DEV_7 = ErrorDef(
        DevValidateException,
        "DataGenSrc data is {size} bytes as base64 parquet, "
        "over the {limit} byte budget",
    )  # DataGenSrc.validate()
    DEV_8 = ErrorDef(
        DevRuntimeException,
        "DataGenSrc read failed (slot={slot})",
    )  # DataGenSrcPlugin.read_in
