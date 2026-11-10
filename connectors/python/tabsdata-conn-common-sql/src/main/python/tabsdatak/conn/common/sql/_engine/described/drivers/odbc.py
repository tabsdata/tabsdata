#
# Copyright 2026 Tabsdata Inc.
#

"""ODBC, through pyodbc -- SQL Server's driver, and any other database reached
over ODBC. A column is described by the Python class its values come back as,
with its precision and scale. An integer's precision is its
size in digits, which tells SMALLINT, INT and BIGINT apart; a float's is its
binary digits, 24 for REAL. A three-digit integer is named SMALLINT: SQL
Server's TINYINT runs 0 to 255, other databases' -128 to 127.

SQL Server's DATETIMEOFFSET is described as text but comes back, through
SQLAlchemy, as a datetime with its offset: it is the text column with a scale,
its fractional seconds. One with no fractional seconds is displayed in 26
characters, as a VARCHAR(26) is, so that size is left to inference."""

from __future__ import annotations

import datetime as dt
import uuid
from decimal import Decimal
from typing import TYPE_CHECKING

if TYPE_CHECKING:
    # noinspection PyProtectedMember
    from tabsdatak.conn.common.sql._engine.described import Column

INTEGERS = {3: "SMALLINT", 5: "SMALLINT", 10: "INT", 19: "BIGINT"}
REAL_PRECISION = 24
OFFSET_SIZE = 26

CLASSES = {
    bool: "BOOLEAN",
    str: "VARCHAR",
    bytes: "VARBINARY",
    bytearray: "VARBINARY",
    dt.datetime: "DATETIME2",
    dt.date: "DATE",
    dt.time: "TIME",
    uuid.UUID: "UUID",
}


def describe(column: Column) -> str | None:
    code, precision, scale = column.code, column.precision, column.scale
    if code is int:
        return INTEGERS.get(precision, "BIGINT")
    if code is float:
        return "REAL" if precision == REAL_PRECISION else "FLOAT"
    if code is Decimal:
        if isinstance(precision, int) and isinstance(scale, int):
            return f"DECIMAL({precision}, {scale})"
        return None
    if code is str:
        if isinstance(scale, int) and scale > 0:
            return "TIMESTAMP WITH TIME ZONE"
        if column.internal_size == OFFSET_SIZE:
            return None
    return CLASSES.get(code)
