#
# Copyright 2026 Tabsdata Inc.
#

"""Postgres. psycopg2, psycopg2cffi, psycopg and pg8000 all describe a column
by its type OID; psycopg2 adds a NUMERIC's declared precision and scale, which
pg8000 does not report.

A type whose drivers return different Python values -- JSON as a ``dict``,
UUIDs and network addresses as objects or text, a ``timetz`` as a ``time`` or
text -- is named by the text it reads as; the reader writes each value as that
text. Arrays, ranges and the geometric types are left to inference: an array's
dimensions are not in its description, and the geometric types come back as
text from one driver and as tuples from another."""

from __future__ import annotations

from typing import TYPE_CHECKING

if TYPE_CHECKING:
    # noinspection PyProtectedMember
    from tabsdatak.conn.common.sql._engine.described import Column

NUMERIC = 1700
# What a NUMERIC declared with no precision describes as.
UNCONSTRAINED = 65535
# A scale is kept in the type modifier's low 11 bits, as a two's complement
# since Postgres 15 allows a negative one.
SCALE_BITS = 2048
SCALE_SIGN = 1024

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
    114: "JSON",
    142: "XML",
    650: "TEXT",  # cidr
    700: "REAL",
    701: "DOUBLE PRECISION",
    774: "TEXT",  # macaddr8
    790: "TEXT",  # money, formatted by the server's locale
    829: "TEXT",  # macaddr
    869: "TEXT",  # inet
    1042: "BPCHAR",
    1043: "VARCHAR",
    1082: "DATE",
    1083: "TIME",
    1114: "TIMESTAMP",
    1184: "TIMESTAMPTZ",
    1186: "INTERVAL",
    1266: "TEXT",  # timetz
    1560: "TEXT",  # bit
    1562: "TEXT",  # varbit
    2950: "UUID",
    3614: "TEXT",  # tsvector
    3802: "JSON",  # jsonb
}


def numeric(precision: object, scale: object) -> str | None:
    """A NUMERIC by what it holds: one with a negative scale holds integers of
    ``precision - scale`` digits, one with a scale beyond its precision
    fractions of ``scale`` digits."""
    if not isinstance(precision, int) or not isinstance(scale, int):
        return None
    if precision <= 0 or precision == UNCONSTRAINED:
        return None
    if scale >= SCALE_SIGN:
        scale -= SCALE_BITS
    if scale < 0:
        return f"NUMERIC({precision - scale}, 0)"
    return f"NUMERIC({max(precision, scale)}, {scale})"


def describe(column: Column) -> str | None:
    if column.code == NUMERIC:
        return numeric(column.precision, column.scale)
    return BY_OID.get(column.code)
