#
# Copyright 2026 Tabsdata Inc.
#

"""SQL type names, and the Python classes a driver reports for its values,
mapped to Polars dtypes. Names are matched whatever their case and spacing,
with their parameters (``VARCHAR(20)``, ``DECIMAL(10, 2)``) and an
``UNSIGNED`` suffix. A decimal wider than a Polars decimal holds maps to
text, which keeps every digit; JSON maps to its text. A name with no single
correct dtype -- a time zone that is not UTC -- maps to ``None``."""

from __future__ import annotations

import datetime as dt
import re
from decimal import Decimal
from typing import Any

import polars as pl

MAX_DECIMAL_PRECISION = 38

_SIGNED: dict[str, pl.DataType] = {
    "TINYINT": pl.Int8(),
    "INT1": pl.Int8(),
    "SMALLINT": pl.Int16(),
    "INT2": pl.Int16(),
    "MEDIUMINT": pl.Int32(),
    "INT": pl.Int32(),
    "INTEGER": pl.Int32(),
    "INT4": pl.Int32(),
    "BIGINT": pl.Int64(),
    "INT8": pl.Int64(),
}
_UNSIGNED: dict[str, pl.DataType] = {
    "TINYINT": pl.UInt8(),
    "SMALLINT": pl.UInt16(),
    "MEDIUMINT": pl.UInt32(),
    "INT": pl.UInt32(),
    "INTEGER": pl.UInt32(),
    "BIGINT": pl.UInt64(),
}
_DECIMALS = {"DECIMAL", "NUMERIC", "DEC", "NUM", "NUMBER"}
_STRINGS = {
    "CHAR",
    "CHARACTER",
    "NCHAR",
    "VARCHAR",
    "VARCHAR2",
    "NVARCHAR",
    "NVARCHAR2",
    "CHARACTER VARYING",
    "CHAR VARYING",
    "BPCHAR",
    "NAME",
    "TEXT",
    "TINYTEXT",
    "MEDIUMTEXT",
    "LONGTEXT",
    "NTEXT",
    "CLOB",
    "NCLOB",
    "DBCLOB",
    "CHARACTER LARGE OBJECT",
    "CHAR LARGE OBJECT",
    "LONG",
    "LONG VARCHAR",
    "STRING",
    "ENUM",
    "UUID",
    "UNIQUEIDENTIFIER",
    "ROWID",
    "UROWID",
    "XML",
    "XMLTYPE",
    "JSON",
    "JSONB",
}
_BINARIES = {
    "BINARY",
    "VARBINARY",
    "BLOB",
    "TINYBLOB",
    "MEDIUMBLOB",
    "LONGBLOB",
    "BYTEA",
    "RAW",
    "LONG RAW",
    "IMAGE",
    "BINARY LARGE OBJECT",
}
_OTHERS: dict[str, pl.DataType] = {
    "NULL": pl.Null(),
    "BOOL": pl.Boolean(),
    "BOOLEAN": pl.Boolean(),
    "BIT": pl.Boolean(),
    "YEAR": pl.Int16(),
    "OID": pl.Int64(),
    "REAL": pl.Float32(),
    "FLOAT4": pl.Float32(),
    "BINARY_FLOAT": pl.Float32(),
    "DOUBLE": pl.Float64(),
    "DOUBLE PRECISION": pl.Float64(),
    "FLOAT8": pl.Float64(),
    "BINARY_DOUBLE": pl.Float64(),
    "DECFLOAT": pl.Float64(),
    "DATE": pl.Date(),
    "TIME": pl.Time(),
    "TIME WITHOUT TIME ZONE": pl.Time(),
    "DATETIME": pl.Datetime("us"),
    "DATETIME2": pl.Datetime("us"),
    "SMALLDATETIME": pl.Datetime("us"),
    "TIMESTAMP": pl.Datetime("us"),
    "TIMESTAMP WITHOUT TIME ZONE": pl.Datetime("us"),
    "TIMESTAMPTZ": pl.Datetime("us", "UTC"),
    "TIMESTAMP WITH TIME ZONE": pl.Datetime("us", "UTC"),
    "INTERVAL": pl.Duration("us"),
    "INTERVAL DAY TO SECOND": pl.Duration("us"),
}

_NAME = re.compile(
    r"^\s*(?P<base>[A-Z_][A-Z0-9_ ]*?)\s*"
    r"(?:\(\s*(?P<first>\d+)\s*(?:,\s*(?P<second>-?\d+)\s*)?\))?"
    r"\s*(?P<unsigned>UNSIGNED)?\s*$"
)

# BIT(n) with n > 1 is a bit string, not a flag.
_SINGLE_BIT = 1


def decimal(precision: Any, scale: Any) -> pl.DataType | None:
    """``Decimal(precision, scale)``; text when ``precision`` is more than a
    Polars decimal holds; ``None`` when they are not usable."""
    if not isinstance(precision, int) or not isinstance(scale, int):
        return None
    if precision <= 0 or not 0 <= scale <= precision:
        return None
    if precision > MAX_DECIMAL_PRECISION:
        return pl.String()
    return pl.Decimal(precision, scale)


def too_wide(name: str, precision: Any = None) -> bool:
    """Whether SQL type ``name`` is a decimal wider than a Polars decimal."""
    match = _NAME.match(name.upper())
    if match is None or " ".join(match["base"].split()) not in _DECIMALS:
        return False
    declared = int(match["first"]) if match["first"] is not None else precision
    return isinstance(declared, int) and declared > MAX_DECIMAL_PRECISION


def dtype(name: str, precision: Any = None, scale: Any = None) -> pl.DataType | None:
    """The dtype of SQL type ``name``. The parameters in the name win over
    ``precision`` and ``scale``, which come from the cursor description."""
    match = _NAME.match(name.upper())
    if match is None:
        return None
    base = " ".join(match["base"].split())
    first = int(match["first"]) if match["first"] is not None else None
    second = int(match["second"]) if match["second"] is not None else None
    if base in _SIGNED:
        return (_UNSIGNED if match["unsigned"] else _SIGNED).get(base)
    if base in _DECIMALS:
        if first is not None:
            return decimal(first, second if second is not None else 0)
        return decimal(precision, scale)
    if base == "FLOAT":
        # FLOAT(n) holds n binary digits: up to 24 is single precision.
        return pl.Float32() if first is not None and first <= 24 else pl.Float64()
    if base == "BIT" and first is not None and first != _SINGLE_BIT:
        return None
    if base in _STRINGS:
        return pl.String()
    if base in _BINARIES:
        return pl.Binary()
    return _OTHERS.get(base)


def from_class(
    code: type, precision: Any = None, scale: Any = None
) -> pl.DataType | None:
    """The dtype of a column whose driver reports the Python class of its
    values."""
    if code is bool:
        return pl.Boolean()
    if code is Decimal:
        return decimal(precision, scale)
    by_class: dict[type, pl.DataType] = {
        int: pl.Int64(),
        float: pl.Float64(),
        str: pl.String(),
        bytes: pl.Binary(),
        bytearray: pl.Binary(),
        dt.datetime: pl.Datetime("us"),
        dt.date: pl.Date(),
        dt.time: pl.Time(),
        dt.timedelta: pl.Duration("us"),
    }
    return by_class.get(code)
