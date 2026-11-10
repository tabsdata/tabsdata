#
# Copyright 2026 Tabsdata Inc.
#

"""MySQL and MariaDB. Every driver describes a column by its MySQL field type
code; what else it reports differs, so the columns are completed first to
what :func:`describe` reads -- the flags and the charset after the standard
fields, a DECIMAL's real precision and scale:

* pymysql describes a DECIMAL by its display length and its scale; its
  cursor's field list adds the flags and the charset, and the length gives the
  precision back exactly (:func:`from_fields`).
* mysql-connector describes the flags and the charset but no DECIMAL precision
  or scale; the database gives them, from a temporary table the query creates
  with no rows (:func:`from_database`).
* MariaDB Connector/Python adds the flags but not the charset: text and bytes
  are left to inference.
* mysqlclient adds neither: an integer is named one size wider, so an unsigned
  value still fits, and BIGINT, text and bytes are left to inference.

A column whose precision the description reports as is -- a display length,
never less than the real precision -- is named by it, so it holds every value
even when a digit wider than declared.
"""

from __future__ import annotations

import re
import uuid
from enum import IntEnum
from typing import TYPE_CHECKING, Any

if TYPE_CHECKING:
    # noinspection PyProtectedMember
    from tabsdatak.conn.common.sql._engine.described import Column, Query

BINARY_CHARSET = 63
UNSIGNED_FLAG = 32


class FieldType(IntEnum):
    """MySQL field type codes, the same for every driver."""

    DECIMAL = 0
    TINY = 1
    SHORT = 2
    LONG = 3
    FLOAT = 4
    DOUBLE = 5
    NULL = 6
    TIMESTAMP = 7
    LONGLONG = 8
    INT24 = 9
    DATE = 10
    TIME = 11
    DATETIME = 12
    YEAR = 13
    NEWDATE = 14
    VARCHAR = 15
    BIT = 16
    JSON = 245
    NEWDECIMAL = 246
    ENUM = 247
    SET = 248
    TINY_BLOB = 249
    MEDIUM_BLOB = 250
    LONG_BLOB = 251
    BLOB = 252
    VAR_STRING = 253
    STRING = 254
    GEOMETRY = 255


DECIMALS = {FieldType.DECIMAL, FieldType.NEWDECIMAL}

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
    FieldType.NULL: "NULL",
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
    # mysql-connector returns a BIT as an int, pymysql as its bytes; both are
    # read as the unsigned number the bits spell.
    FieldType.BIT: "BIGINT UNSIGNED",
    # Every driver returns a JSON document as its text, and a geometry as its
    # binary form.
    FieldType.JSON: "JSON",
    FieldType.GEOMETRY: "VARBINARY",
    FieldType.ENUM: "VARCHAR",
    FieldType.SET: "VARCHAR",
}


def _extra(column: Column, index: int) -> Any:
    return column.extra[index] if len(column.extra) > index else None


def describe(column: Column) -> str | None:
    """Flags first and charset second, after the standard fields, as far as
    the driver gives them."""
    code = column.code
    flags = _extra(column, 0)
    if code in INTEGERS:
        if not isinstance(flags, int):
            return WIDER.get(code)
        return INTEGERS[code] + (" UNSIGNED" if flags & UNSIGNED_FLAG else "")
    if code in DECIMALS:
        precision, scale = column.precision, column.scale
        if isinstance(precision, int) and isinstance(scale, int):
            return f"DECIMAL({precision}, {scale})"
        return None
    if code in TEXTUAL:
        charset = _extra(column, 1)
        if not isinstance(charset, int):
            return None
        return "VARBINARY" if charset == BINARY_CHARSET else "VARCHAR"
    return OTHERS.get(code)


def from_fields(query: Query, columns: list[Column]) -> list[Column]:
    """pymysql: the flags and the charset from its cursor's field list, and a
    DECIMAL's precision from its display length, which counts a sign unless the
    column is unsigned and a point when it has a scale."""
    fields = query.cursor._result.fields
    if len(fields) != len(columns):
        return columns
    completed = []
    for column, field in zip(columns, fields):
        precision = column.precision
        if field.type_code in DECIMALS:
            unsigned = field.flags & UNSIGNED_FLAG
            precision = field.length - (field.scale > 0) - (0 if unsigned else 1)
        completed.append(
            column._replace(
                precision=precision,
                scale=field.scale,
                extra=(field.flags, field.charsetnr),
            )
        )
    return completed


_DECLARED = re.compile(r"^(?:decimal|numeric)\((\d+),(\d+)\)", re.IGNORECASE)


def from_database(query: Query, columns: list[Column]) -> list[Column]:
    """mysql-connector: a DECIMAL's precision and scale as the database types
    the query's columns -- expressions included -- in a temporary table it
    creates from the query with no rows. Only done when there is a DECIMAL to
    type; the result is buffered, so its connection is free for it."""
    if not any(column.code in DECIMALS for column in columns):
        return columns
    from sqlalchemy import text

    table = f"td_described_{uuid.uuid4().hex[:12]}"
    statement = query.text.strip().rstrip(";")
    connection = query.connection
    connection.execute(
        text(
            f"CREATE TEMPORARY TABLE {table}"
            f" AS SELECT * FROM ({statement}) AS q LIMIT 0"
        ),
        dict(query.parameters),
    )
    try:
        declared = [
            row[1] for row in connection.execute(text(f"SHOW COLUMNS FROM {table}"))
        ]
    finally:
        connection.execute(text(f"DROP TEMPORARY TABLE {table}"))
    if len(declared) != len(columns):
        return columns
    completed = []
    for column, kind in zip(columns, declared):
        if isinstance(kind, bytes):
            kind = kind.decode()
        match = _DECLARED.match(str(kind))
        if column.code in DECIMALS and match:
            column = column._replace(precision=int(match[1]), scale=int(match[2]))
        completed.append(column)
    return completed
