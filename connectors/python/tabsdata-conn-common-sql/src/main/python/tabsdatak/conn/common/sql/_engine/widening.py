#
# Copyright 2026 Tabsdata Inc.
#

"""Number columns a batch has outgrown, for the SQL destinations.

Types are compared as each database spells them, which is uniform where the
SQLAlchemy classes are not (Oracle reflects every integer as ``NUMBER(p, 0)``,
DB2 reflects ``DOUBLE`` as a ``Numeric``). Exact numbers -- integers and
decimals -- compare by their digits, floats by their width. Anything else, and
a pair from two different families, is left to the database: a boolean that a
database stores as a small integer is not a number the batch has outgrown.
"""

from __future__ import annotations

import re

_EXACT_DIGITS = {
    "TINYINT": 3,
    "SMALLINT": 5,
    "MEDIUMINT": 7,
    "INT": 10,
    "INTEGER": 10,
    "BIGINT": 19,
}
_DECIMALS = ("DECIMAL", "NUMERIC", "NUMBER")
_FLOAT4 = ("REAL", "BINARY_FLOAT")
_FLOAT8 = ("DOUBLE", "DOUBLE PRECISION", "BINARY_DOUBLE")
_TYPE = re.compile(
    r"^\s*([A-Z_]+(?: PRECISION)?)\s*(?:\(\s*(\d+)\s*(?:,\s*(\d+)\s*)?\))?"
)


def kind(spelling: str, dialect: str) -> tuple[str, int, int] | None:
    """``("exact", precision, scale)`` or ``("float", bytes, 0)`` for a number
    type, ``None`` for anything else. A bare ``FLOAT`` is single precision in
    MySQL and double everywhere else."""
    match = _TYPE.match(spelling.upper())
    if match is None:
        return None
    base, first, second = match[1], match[2], match[3]
    if base == "TINYINT" and first == "1":
        return None
    if base in _EXACT_DIGITS:
        return "exact", _EXACT_DIGITS[base], 0
    if base in _DECIMALS:
        if first is None:
            return None
        return "exact", int(first), int(second or 0)
    if base in _FLOAT4:
        return "float", 4, 0
    if base in _FLOAT8:
        return "float", 8, 0
    if base == "FLOAT":
        if first is None:
            return "float", 4 if dialect == "mysql" else 8, 0
        return "float", 8 if int(first) > 24 else 4, 0
    return None


def outgrows(stored: str, batch: str, dialect: str) -> bool:
    """Whether a column spelled ``stored`` needs widening to hold ``batch``."""
    old, new = kind(stored, dialect), kind(batch, dialect)
    if old is None or new is None or old[0] != new[0] or old == new:
        return False
    if old[0] == "float":
        return new[1] > old[1]
    _, old_precision, old_scale = old
    _, new_precision, new_scale = new
    widens = (
        new_precision - new_scale >= old_precision - old_scale
        and new_scale >= old_scale
    )
    return widens
