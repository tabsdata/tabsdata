#
# Copyright 2026 Tabsdata Inc.
#

"""Postgres. psycopg2, psycopg2cffi, psycopg and pg8000 all describe a column
by its type OID, and a NUMERIC by its precision and scale when declared."""

from __future__ import annotations

from typing import TYPE_CHECKING

if TYPE_CHECKING:
    # noinspection PyProtectedMember
    from tabsdatak.conn.common.sql._engine.described import Column

NUMERIC = 1700

BY_OID: dict[int, str] = {
    16: "BOOLEAN",
    17: "BYTEA",
    18: "CHAR",
    19: "NAME",
    20: "BIGINT",
    21: "SMALLINT",
    23: "INTEGER",
    25: "TEXT",
    26: "OID",
    700: "REAL",
    701: "DOUBLE PRECISION",
    1042: "BPCHAR",
    1043: "VARCHAR",
    1082: "DATE",
    1083: "TIME",
    1114: "TIMESTAMP",
    1184: "TIMESTAMPTZ",
    1186: "INTERVAL",
    2950: "UUID",
}


def describe(column: Column) -> str | None:
    if column.code == NUMERIC:
        precision, scale = column.precision, column.scale
        if isinstance(precision, int) and isinstance(scale, int):
            return f"NUMERIC({precision}, {scale})"
        return None
    return BY_OID.get(column.code)
