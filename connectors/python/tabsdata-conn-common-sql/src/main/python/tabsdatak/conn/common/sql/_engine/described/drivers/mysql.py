#
# Copyright 2026 Tabsdata Inc.
#

"""MySQL and MariaDB. Every driver describes a column by its MySQL field type
code; what else it reports differs:

* mysql-connector adds the column's flags and charset,
  which tell a signed integer from an unsigned one and text from bytes.
* MariaDB Connector/Python adds the flags but not the charset: text
  and bytes are left to inference.
* pymysql and mysqlclient add neither: an integer is named one size wider, so
  an unsigned value still fits, and BIGINT, text and bytes are left to
  inference.

No driver gives a DECIMAL's real precision, so a DECIMAL is left to inference.
"""

from __future__ import annotations

from enum import IntEnum
from typing import TYPE_CHECKING, Any

if TYPE_CHECKING:
    # noinspection PyProtectedMember
    from tabsdatak.conn.common.sql._engine.described import Column

BINARY_CHARSET = 63
UNSIGNED_FLAG = 32


class FieldType(IntEnum):
    """MySQL field type codes, the same for every driver."""

    TINY = 1
    SHORT = 2
    LONG = 3
    FLOAT = 4
    DOUBLE = 5
    TIMESTAMP = 7
    LONGLONG = 8
    INT24 = 9
    DATE = 10
    TIME = 11
    DATETIME = 12
    YEAR = 13
    NEWDATE = 14
    VARCHAR = 15
    ENUM = 247
    TINY_BLOB = 249
    MEDIUM_BLOB = 250
    LONG_BLOB = 251
    BLOB = 252
    VAR_STRING = 253
    STRING = 254


TEXTUAL = {
    FieldType.VARCHAR,
    FieldType.VAR_STRING,
    FieldType.STRING,
    FieldType.TINY_BLOB,
    FieldType.MEDIUM_BLOB,
    FieldType.LONG_BLOB,
    FieldType.BLOB,
}

INTEGERS = {
    FieldType.TINY: "TINYINT",
    FieldType.SHORT: "SMALLINT",
    FieldType.INT24: "MEDIUMINT",
    FieldType.LONG: "INT",
    FieldType.LONGLONG: "BIGINT",
}

WIDER = {
    FieldType.TINY: "SMALLINT",
    FieldType.SHORT: "INT",
    FieldType.INT24: "BIGINT",
    FieldType.LONG: "BIGINT",
}

OTHERS = {
    FieldType.YEAR: "YEAR",
    FieldType.FLOAT: "REAL",
    FieldType.DOUBLE: "DOUBLE",
    FieldType.DATE: "DATE",
    FieldType.NEWDATE: "DATE",
    FieldType.DATETIME: "DATETIME",
    FieldType.TIMESTAMP: "DATETIME",
    # Drivers return a TIME as a timedelta: it holds -838:59:59 to 838:59:59,
    # not only a time of day.
    FieldType.TIME: "INTERVAL",
    FieldType.ENUM: "ENUM",
}


def _extra(column: Column, index: int) -> Any:
    return column.extra[index] if len(column.extra) > index else None


def _integer(column: Column) -> str:
    flags = _extra(column, 0)
    unsigned = isinstance(flags, int) and flags & UNSIGNED_FLAG
    return INTEGERS[column.code] + (" UNSIGNED" if unsigned else "")


def mysqlconnector(column: Column) -> str | None:
    """Flags first and charset second, after the standard fields."""
    if column.code in INTEGERS:
        return _integer(column)
    if column.code in TEXTUAL:
        binary = _extra(column, 1) == BINARY_CHARSET
        return "VARBINARY" if binary else "VARCHAR"
    return OTHERS.get(column.code)


def mariadbconnector(column: Column) -> str | None:
    """Flags first, after the standard fields."""
    if column.code in INTEGERS:
        return _integer(column)
    return OTHERS.get(column.code)


def flagless(column: Column) -> str | None:
    """pymysql and mysqlclient (MySQLdb)."""
    if column.code in WIDER:
        return WIDER[column.code]
    return OTHERS.get(column.code)
