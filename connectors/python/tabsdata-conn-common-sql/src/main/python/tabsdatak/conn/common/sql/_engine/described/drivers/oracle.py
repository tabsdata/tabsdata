#
# Copyright 2026 Tabsdata Inc.
#

"""Oracle. oracledb and cx_Oracle both describe a column by a ``DbType``,
named ``DB_TYPE_<type>``, and a NUMBER by its precision and scale. oracledb's
description entries add whether a column holds JSON -- a VARCHAR2 checked
``IS JSON`` comes back as parsed documents, like a JSON column -- and a
VECTOR's element format."""

from __future__ import annotations

from enum import IntEnum
from typing import TYPE_CHECKING, Any

import polars as pl

if TYPE_CHECKING:
    # noinspection PyProtectedMember
    from tabsdatak.conn.common.sql._engine.described import Column

FLOAT_SCALE = -127
PREFIX = "DB_TYPE_"

# The narrowest integer that holds every value of a NUMBER(p) with p digits.
INTEGERS = [(2, "TINYINT"), (4, "SMALLINT"), (9, "INTEGER"), (18, "BIGINT")]

RENAMED = {
    # An Oracle DATE carries a time of day.
    "DATE": "TIMESTAMP",
    # oracledb returns both as naive datetimes: with a zone, the zone is
    # dropped; with the local zone, in the session's zone.
    "TIMESTAMP_TZ": "TIMESTAMP",
    "TIMESTAMP_LTZ": "TIMESTAMP",
    "LONG_NVARCHAR": "LONG",
    "LONG_RAW": "LONG RAW",
    "INTERVAL_DS": "INTERVAL DAY TO SECOND",
    "BINARY_INTEGER": "BIGINT",
}
UNNAMED = {"OBJECT", "CURSOR", "BFILE"}

INTERVAL_YM = pl.Struct({"years": pl.Int64(), "months": pl.Int64()})


class VectorFormat(IntEnum):
    """oracledb's VECTOR element formats."""

    FLOAT32 = 2
    FLOAT64 = 3
    INT8 = 4
    BINARY = 5


VECTORS = {
    VectorFormat.FLOAT32: pl.List(pl.Float32()),
    VectorFormat.FLOAT64: pl.List(pl.Float64()),
    VectorFormat.INT8: pl.List(pl.Int8()),
    VectorFormat.BINARY: pl.List(pl.UInt8()),
}


def integer(digits: int) -> str:
    return next(
        (name for most, name in INTEGERS if digits <= most), f"NUMBER({digits}, 0)"
    )


def number(precision: Any, scale: Any) -> str | None:
    if not isinstance(precision, int) or not isinstance(scale, int):
        return None
    if precision <= 0:
        # A NUMBER declared bare, or computed: its values come back as int or
        # Decimal one by one.
        return None
    if scale == FLOAT_SCALE:
        return f"FLOAT({precision})"
    if scale <= 0:
        return integer(precision - scale)
    return f"NUMBER({max(precision, scale)}, {scale})"


def describe(column: Column) -> str | pl.DataType | None:
    kind = getattr(column.code, "name", None)
    if not isinstance(kind, str) or not kind.startswith(PREFIX):
        return None
    kind = kind.removeprefix(PREFIX)
    if getattr(column.entry, "is_json", False):
        return "JSON"
    if kind == "NUMBER":
        return number(column.precision, column.scale)
    if kind == "VECTOR":
        return VECTORS.get(
            getattr(column.entry, "vector_format", None), pl.List(pl.Float64())
        )
    if kind == "INTERVAL_YM":
        return INTERVAL_YM
    if kind in UNNAMED:
        return None
    return RENAMED.get(kind, kind)
