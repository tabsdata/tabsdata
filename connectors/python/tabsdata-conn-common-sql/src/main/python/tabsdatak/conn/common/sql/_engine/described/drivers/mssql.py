#
# Copyright 2026 Tabsdata Inc.
#

"""SQL Server, through pymssql: a column is described only by a coarse group
-- STRING, BINARY, NUMBER, DATETIME or DECIMAL -- with no precision or scale.
NUMBER and DATETIME each stand for several types and DECIMAL for any
precision, so only text and bytes are named; the rest is left to inference.
pyodbc, SQL Server's usual driver, is described in :mod:`.odbc`."""

from __future__ import annotations

from enum import IntEnum
from typing import TYPE_CHECKING

if TYPE_CHECKING:
    # noinspection PyProtectedMember
    from tabsdatak.conn.common.sql._engine.described import Column


class Group(IntEnum):
    """pymssql's type groups."""

    STRING = 1
    BINARY = 2
    NUMBER = 3
    DATETIME = 4
    DECIMAL = 5


NAMED = {
    Group.STRING: "VARCHAR",
    Group.BINARY: "VARBINARY",
}


def pymssql(column: Column) -> str | None:
    return NAMED.get(column.code)
