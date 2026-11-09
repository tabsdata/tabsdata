#
# Copyright 2026 Tabsdata Inc.
#

"""Db2, through ibm_db_sa: a column is described by a type group, which
compares equal to the name of each type it stands for, and a DECIMAL by its
precision and scale. A group can stand for several types
-- INTEGER and SMALLINT share one, DOUBLE, REAL and DECFLOAT another -- so each
is named by a type all of its members fit in."""

from __future__ import annotations

from typing import TYPE_CHECKING

if TYPE_CHECKING:
    # noinspection PyProtectedMember
    from tabsdatak.conn.common.sql._engine.described import Column

# One member of each group, which is also the name the group is given.
GROUPS = [
    "VARCHAR",
    "CLOB",
    "XML",
    "BLOB",
    "INTEGER",
    "BIGINT",
    "DOUBLE",
    "DATE",
    "TIME",
    "TIMESTAMP",
    "BOOLEAN",
]


def describe(column: Column) -> str | None:
    code = column.code
    if code == "DECIMAL":
        precision, scale = column.precision, column.scale
        if isinstance(precision, int) and isinstance(scale, int):
            return f"DECIMAL({precision}, {scale})"
        return None
    return next((name for name in GROUPS if code == name), None)
