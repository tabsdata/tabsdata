#
# Copyright 2026 Tabsdata Inc.
#

"""Db2, through ibm_db_sa: a column is described by a type group, which
compares equal to the name of each type it stands for, with the type's
precision and scale. A group can stand for several types; the precision tells
them apart:

* INTEGER: a BOOLEAN is precision 1, a SMALLINT 5, an INTEGER 10.
* DOUBLE: a REAL is precision 7, a DOUBLE 15, a DECFLOAT 16 or 34 -- read as a
  double, as close as a float comes to a decimal floating point.
* VARCHAR: text and bytes (``FOR BIT DATA``, BINARY) share the group; a
  column displayed in twice its precision is either bytes or double-byte
  GRAPHIC text, so it is left to inference.
"""

from __future__ import annotations

from typing import TYPE_CHECKING

if TYPE_CHECKING:
    # noinspection PyProtectedMember
    from tabsdatak.conn.common.sql._engine.described import Column

INTEGERS = {1: "BOOLEAN", 5: "SMALLINT", 10: "INTEGER"}
FLOATS = {7: "REAL", 15: "DOUBLE"}

# One member of each other group, which is also the name the group is given.
GROUPS = ["CLOB", "XML", "BLOB", "BIGINT", "DATE", "TIME", "TIMESTAMP", "BOOLEAN"]


def describe(column: Column) -> str | None:
    code, precision, scale = column.code, column.precision, column.scale
    if code == "DECIMAL":
        if isinstance(precision, int) and isinstance(scale, int):
            return f"DECIMAL({precision}, {scale})"
        return None
    if code == "INTEGER":
        return INTEGERS.get(precision, "INTEGER")
    if code == "DOUBLE":
        return FLOATS.get(precision, "DOUBLE")
    if code == "VARCHAR":
        return "VARCHAR" if column.display_size == precision else None
    return next((name for name in GROUPS if code == name), None)
