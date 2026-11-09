#
# Copyright 2026 Tabsdata Inc.
#

"""Oracle. oracledb and cx_Oracle both describe a column by a ``DbType``,
named ``DB_TYPE_<type>``, and a NUMBER by its precision and scale."""

from __future__ import annotations

from typing import TYPE_CHECKING, Any

if TYPE_CHECKING:
    # noinspection PyProtectedMember
    from tabsdatak.conn.common.sql._engine.described import Column

FLOAT_SCALE = -127
INT64_DIGITS = 18
PREFIX = "DB_TYPE_"

RENAMED = {
    # An Oracle DATE carries a time of day.
    "DATE": "TIMESTAMP",
    "LONG_NVARCHAR": "LONG",
    "LONG_RAW": "LONG RAW",
    "INTERVAL_DS": "INTERVAL DAY TO SECOND",
    "BINARY_INTEGER": "BIGINT",
}
# Times with a zone come back in the session's zone, which is not always UTC.
UNNAMED = {"TIMESTAMP_TZ", "TIMESTAMP_LTZ", "INTERVAL_YM", "JSON", "OBJECT"}


def number(precision: Any, scale: Any) -> str | None:
    if not isinstance(precision, int) or not isinstance(scale, int):
        return None
    if scale == FLOAT_SCALE:
        # FLOAT(b) has a precision; a NUMBER declared bare has none, and its
        # values come back as int or Decimal one by one.
        return f"FLOAT({precision})" if precision > 0 else None
    if scale == 0 and 0 < precision <= INT64_DIGITS:
        return "BIGINT"
    return f"NUMBER({precision}, {scale})"


def describe(column: Column) -> str | None:
    kind = getattr(column.code, "name", None)
    if not isinstance(kind, str) or not kind.startswith(PREFIX):
        return None
    kind = kind.removeprefix(PREFIX)
    if kind == "NUMBER":
        return number(column.precision, column.scale)
    if kind in UNNAMED:
        return None
    return RENAMED.get(kind, kind)
