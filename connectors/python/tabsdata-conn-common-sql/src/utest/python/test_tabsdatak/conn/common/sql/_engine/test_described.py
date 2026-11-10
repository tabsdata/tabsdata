#
# Copyright 2026 Tabsdata Inc.
#

"""The typing of a SQL source's columns from its cursor description, offline:
the name parser, every driver's describer against that driver's own constants,
the registry, and the reader over SQLite."""

from __future__ import annotations

import datetime as dt
import uuid
from decimal import Decimal
from types import SimpleNamespace

import polars as pl
import pytest
from sqlalchemy import create_engine, text

# noinspection PyProtectedMember
from tabsdatak.conn.common.sql._engine import described, sqlalchemy_reader as sar

# noinspection PyProtectedMember
from tabsdatak.conn.common.sql._engine.described import Column, Driver

# noinspection PyProtectedMember
from tabsdatak.conn.common.sql._engine.described.commons import names

# noinspection PyProtectedMember
from tabsdatak.conn.common.sql._engine.described.drivers import (
    db2,
    mssql,
    mysql,
    odbc,
    oracle,
    postgres,
)

pytestmark = pytest.mark.sql_common


def _column(code, precision=None, scale=None, *extra, name="c") -> Column:
    return Column.of((name, code, None, None, precision, scale, True, *extra))


# --- names ------------------------------------------------------------------


@pytest.mark.parametrize(
    "name,dtype",
    [
        ("TINYINT", pl.Int8),
        ("INT1", pl.Int8),
        ("tinyint unsigned", pl.UInt8),
        ("SMALLINT", pl.Int16),
        ("int2", pl.Int16),
        ("SMALLINT UNSIGNED", pl.UInt16),
        ("MEDIUMINT", pl.Int32),
        ("MEDIUMINT UNSIGNED", pl.UInt32),
        ("INT", pl.Int32),
        ("integer", pl.Int32),
        ("INT4", pl.Int32),
        ("INT UNSIGNED", pl.UInt32),
        ("BIGINT", pl.Int64),
        ("int8", pl.Int64),
        ("BIGINT UNSIGNED", pl.UInt64),
        ("DECIMAL(10, 2)", pl.Decimal(10, 2)),
        ("numeric(10,2)", pl.Decimal(10, 2)),
        ("DEC(5, 1)", pl.Decimal(5, 1)),
        ("NUMBER(5)", pl.Decimal(5, 0)),
        ("DECIMAL(38, 38)", pl.Decimal(38, 38)),
        ("DECIMAL(39, 2)", pl.String),
        ("NUMERIC(1000, 500)", pl.String),
        ("DECIMAL(5, 6)", None),
        ("REAL", pl.Float32),
        ("FLOAT4", pl.Float32),
        ("FLOAT(24)", pl.Float32),
        ("FLOAT(25)", pl.Float64),
        ("FLOAT(53)", pl.Float64),
        ("FLOAT", pl.Float64),
        ("DOUBLE", pl.Float64),
        ("DOUBLE PRECISION", pl.Float64),
        ("FLOAT8", pl.Float64),
        ("BINARY_FLOAT", pl.Float32),
        ("BINARY_DOUBLE", pl.Float64),
        ("DECFLOAT", pl.Float64),
        ("CHAR(3)", pl.String),
        ("VARCHAR(20)", pl.String),
        ("VARCHAR2", pl.String),
        ("NVARCHAR", pl.String),
        ("character  varying", pl.String),
        ("BPCHAR", pl.String),
        ("TEXT", pl.String),
        ("LONGTEXT", pl.String),
        ("CLOB", pl.String),
        ("NCLOB", pl.String),
        ("DBCLOB", pl.String),
        ("CHARACTER LARGE OBJECT", pl.String),
        ("LONG", pl.String),
        ("ENUM", pl.String),
        ("UUID", pl.String),
        ("UNIQUEIDENTIFIER", pl.String),
        ("ROWID", pl.String),
        ("XML", pl.String),
        ("XMLTYPE", pl.String),
        ("JSON", pl.String),
        ("JSONB", pl.String),
        ("BINARY(4)", pl.Binary),
        ("VARBINARY", pl.Binary),
        ("BLOB", pl.Binary),
        ("LONGBLOB", pl.Binary),
        ("BYTEA", pl.Binary),
        ("RAW", pl.Binary),
        ("LONG RAW", pl.Binary),
        ("IMAGE", pl.Binary),
        ("BINARY LARGE OBJECT", pl.Binary),
        ("BOOLEAN", pl.Boolean),
        ("BOOL", pl.Boolean),
        ("BIT", pl.Boolean),
        ("BIT(1)", pl.Boolean),
        ("BIT(8)", None),
        ("YEAR", pl.Int16),
        ("OID", pl.Int64),
        ("NULL", pl.Null),
        ("DATE", pl.Date),
        ("TIME", pl.Time),
        ("TIME WITHOUT TIME ZONE", pl.Time),
        ("DATETIME", pl.Datetime("us")),
        ("DATETIME2", pl.Datetime("us")),
        ("SMALLDATETIME", pl.Datetime("us")),
        ("TIMESTAMP", pl.Datetime("us")),
        ("timestamp without time zone", pl.Datetime("us")),
        ("TIMESTAMPTZ", pl.Datetime("us", "UTC")),
        ("timestamp with time zone", pl.Datetime("us", "UTC")),
        ("INTERVAL", pl.Duration("us")),
        ("INTERVAL DAY TO SECOND", pl.Duration("us")),
        ("GEOMETRY", None),
        ("TIME WITH TIME ZONE", None),
        ("", None),
        ("not a type!", None),
    ],
)
def test_a_type_name_maps_to_its_dtype(name, dtype):
    assert names.dtype(name) == dtype


def test_a_bare_decimal_takes_the_described_precision_and_scale():
    assert names.dtype("DECIMAL", 12, 3) == pl.Decimal(12, 3)
    assert names.dtype("NUMERIC") is None
    assert names.dtype("DECIMAL", "12", 3) is None


def test_the_parameters_in_a_name_win_over_the_described_ones():
    assert names.dtype("DECIMAL(10, 2)", 12, 3) == pl.Decimal(10, 2)


@pytest.mark.parametrize(
    "precision,scale,dtype",
    [
        (1, 0, pl.Decimal(1, 0)),
        (1, 1, pl.Decimal(1, 1)),
        (10, 2, pl.Decimal(10, 2)),
        (38, 0, pl.Decimal(38, 0)),
        (38, 38, pl.Decimal(38, 38)),
        (39, 0, pl.String),
        (65, 30, pl.String),
        (0, 0, None),
        (10, -1, None),
        (10, 11, None),
        (None, 2, None),
        (10, None, None),
    ],
)
def test_a_decimal_is_held_by_a_decimal_or_else_by_text(precision, scale, dtype):
    assert names.decimal(precision, scale) == dtype


@pytest.mark.parametrize(
    "name,precision,wide",
    [
        ("DECIMAL(39, 2)", None, True),
        ("NUMERIC(38, 2)", None, False),
        ("DECIMAL", 39, True),
        ("DECIMAL", 38, False),
        ("NUMBER(65, 30)", None, True),
        ("VARCHAR(100)", None, False),
        ("BIGINT", 99, False),
    ],
)
def test_a_decimal_wider_than_a_decimal_column_is_told_apart(name, precision, wide):
    assert names.too_wide(name, precision) is wide


@pytest.mark.parametrize(
    "code,precision,scale,dtype",
    [
        (bool, None, None, pl.Boolean),
        (int, None, None, pl.Int64),
        (float, None, None, pl.Float64),
        (Decimal, 10, 2, pl.Decimal(10, 2)),
        (Decimal, None, None, None),
        (str, None, None, pl.String),
        (bytes, None, None, pl.Binary),
        (bytearray, None, None, pl.Binary),
        (dt.datetime, None, None, pl.Datetime("us")),
        (dt.date, None, None, pl.Date),
        (dt.time, None, None, pl.Time),
        (dt.timedelta, None, None, pl.Duration("us")),
        (dict, None, None, None),
    ],
)
def test_a_reported_class_maps_to_its_dtype(code, precision, scale, dtype):
    assert names.from_class(code, precision, scale) == dtype


# --- Column -----------------------------------------------------------------


def test_a_column_names_the_standard_fields_and_keeps_the_rest():
    entry = ("id", 3, 11, 4, 10, 0, False, 32, 63)
    column = Column.of(entry)
    assert column.name == "id"
    assert column.code == 3
    assert (column.display_size, column.internal_size) == (11, 4)
    assert (column.precision, column.scale, column.null_ok) == (10, 0, False)
    assert column.extra == (32, 63)
    assert column.entry is entry


def test_a_short_entry_leaves_the_missing_fields_empty():
    column = Column.of(("id", 3))
    assert (column.precision, column.scale, column.extra) == (None, None, ())


# --- MySQL ------------------------------------------------------------------

UNSIGNED = mysql.UNSIGNED_FLAG
BINARY = mysql.BINARY_CHARSET
UTF8MB4 = 255


def test_the_mysql_field_types_are_the_drivers_own():
    from mysql.connector.constants import FieldType as Connector
    from pymysql.constants import FIELD_TYPE

    for member in mysql.FieldType:
        assert getattr(Connector, member.name) == member.value
        assert getattr(FIELD_TYPE, member.name) == member.value


@pytest.mark.parametrize(
    "code,flags,charset,name",
    [
        (mysql.FieldType.TINY, 0, BINARY, "TINYINT"),
        (mysql.FieldType.TINY, UNSIGNED, BINARY, "TINYINT UNSIGNED"),
        (mysql.FieldType.SHORT, UNSIGNED, BINARY, "SMALLINT UNSIGNED"),
        (mysql.FieldType.INT24, 0, BINARY, "MEDIUMINT"),
        (mysql.FieldType.INT24, UNSIGNED, BINARY, "MEDIUMINT UNSIGNED"),
        (mysql.FieldType.LONG, UNSIGNED, BINARY, "INT UNSIGNED"),
        (mysql.FieldType.LONGLONG, 0, BINARY, "BIGINT"),
        (mysql.FieldType.LONGLONG, UNSIGNED | 1, BINARY, "BIGINT UNSIGNED"),
        (mysql.FieldType.VARCHAR, 0, UTF8MB4, "VARCHAR"),
        (mysql.FieldType.VAR_STRING, 0, UTF8MB4, "VARCHAR"),
        (mysql.FieldType.VAR_STRING, 128, BINARY, "VARBINARY"),
        (mysql.FieldType.STRING, 0, 33, "VARCHAR"),
        (mysql.FieldType.BLOB, 16, UTF8MB4, "VARCHAR"),
        (mysql.FieldType.BLOB, 144, BINARY, "VARBINARY"),
        (mysql.FieldType.LONG_BLOB, 144, BINARY, "VARBINARY"),
        (mysql.FieldType.YEAR, 0, BINARY, "YEAR"),
        (mysql.FieldType.FLOAT, 0, BINARY, "REAL"),
        (mysql.FieldType.DOUBLE, 0, BINARY, "DOUBLE"),
        (mysql.FieldType.DATE, 0, BINARY, "DATE"),
        (mysql.FieldType.DATETIME, 0, BINARY, "DATETIME"),
        (mysql.FieldType.TIMESTAMP, 0, BINARY, "DATETIME"),
        (mysql.FieldType.TIME, 0, BINARY, "INTERVAL"),
        (mysql.FieldType.BIT, UNSIGNED, BINARY, "BIGINT UNSIGNED"),
        (mysql.FieldType.JSON, 144, BINARY, "JSON"),
        (mysql.FieldType.GEOMETRY, 144, BINARY, "VARBINARY"),
        (mysql.FieldType.NULL, 128, BINARY, "NULL"),
        (mysql.FieldType.ENUM, 0, UTF8MB4, "VARCHAR"),
        (mysql.FieldType.SET, 0, UTF8MB4, "VARCHAR"),
        (mysql.FieldType.NEWDECIMAL, 0, BINARY, None),
        (242, 0, BINARY, None),
    ],
)
def test_mysql_names_by_type_flags_and_charset(code, flags, charset, name):
    assert mysql.describe(_column(code, None, None, flags, charset)) == name


@pytest.mark.parametrize(
    "code,name",
    [
        (mysql.FieldType.TINY, "SMALLINT"),
        (mysql.FieldType.SHORT, "INT"),
        (mysql.FieldType.INT24, "BIGINT"),
        (mysql.FieldType.LONG, "BIGINT"),
        (mysql.FieldType.LONGLONG, None),
        (mysql.FieldType.VAR_STRING, None),
        (mysql.FieldType.BLOB, None),
        (mysql.FieldType.DATETIME, "DATETIME"),
        (mysql.FieldType.TIME, "INTERVAL"),
        (mysql.FieldType.JSON, "JSON"),
    ],
)
def test_mysql_without_flags_widens_integers_and_leaves_text_and_bytes(code, name):
    assert mysql.describe(_column(code)) == name


def test_mysql_with_flags_but_no_charset_leaves_text_and_bytes():
    assert mysql.describe(_column(mysql.FieldType.SHORT, None, None, UNSIGNED)) == (
        "SMALLINT UNSIGNED"
    )
    assert mysql.describe(_column(mysql.FieldType.BLOB, None, None, 128)) is None


@pytest.mark.parametrize(
    "precision,scale,name",
    [
        (10, 2, "DECIMAL(10, 2)"),
        (65, 30, "DECIMAL(65, 30)"),
        (None, 2, None),
        (12, None, None),
    ],
)
def test_mysql_names_a_decimal_by_its_precision_and_scale(precision, scale, name):
    column = _column(mysql.FieldType.NEWDECIMAL, precision, scale, 0, BINARY)
    assert mysql.describe(column) == name


DECIMAL_SHAPES = [
    (precision, scale, unsigned)
    for precision in (1, 2, 5, 9, 10, 18, 19, 30, 38, 39, 50, 65)
    for scale in sorted({0, 1, 2, precision // 2, min(precision, 30)})
    if scale <= min(precision, 30)
    for unsigned in (False, True)
]


def _field(type_code, length, scale, flags, charset=BINARY):
    return SimpleNamespace(
        type_code=type_code, length=length, scale=scale, flags=flags, charsetnr=charset
    )


def _pymysql_query(*fields):
    cursor = SimpleNamespace(_result=SimpleNamespace(fields=list(fields)))
    return described.Query(None, cursor, "SELECT c", {})


@pytest.mark.parametrize("precision,scale,unsigned", DECIMAL_SHAPES)
def test_pymysql_gets_a_decimals_precision_back_from_its_length(
    precision, scale, unsigned
):
    length = precision + (scale > 0) + (0 if unsigned else 1)
    flags = UNSIGNED if unsigned else 0
    field = _field(mysql.FieldType.NEWDECIMAL, length, scale, flags)
    entry = ("c", mysql.FieldType.NEWDECIMAL, None, length, length, scale, True)
    [column] = mysql.from_fields(_pymysql_query(field), [Column.of(entry)])
    assert (column.precision, column.scale) == (precision, scale)
    assert column.extra == (flags, BINARY)
    expected = names.decimal(precision, scale)
    assert described.dtypes("pymysql", [entry], _pymysql_query(field))["c"] == expected


def test_pymysql_takes_the_flags_and_charset_of_every_field():
    fields = [
        _field(mysql.FieldType.LONGLONG, 20, 0, UNSIGNED),
        _field(mysql.FieldType.VAR_STRING, 80, 0, 0, UTF8MB4),
        _field(mysql.FieldType.VAR_STRING, 4, 0, 128),
    ]
    entries = [
        (name, field.type_code, None, field.length, field.length, 0, True)
        for name, field in zip("abc", fields)
    ]
    dtypes = described.dtypes("pymysql", entries, _pymysql_query(*fields))
    assert dtypes == {"a": pl.UInt64(), "b": pl.String(), "c": pl.Binary()}


def test_pymysql_fields_that_do_not_match_the_description_are_not_used():
    entries = [("a", mysql.FieldType.LONG, None, 11, 11, 0, True)]
    query = _pymysql_query()
    assert described.dtypes("pymysql", entries, query) == {"a": pl.Int64()}


class _Database:
    """A connection whose database declares the given types for a query's
    columns, recording what it was asked."""

    def __init__(self, *declared, refuse=False):
        self.declared = declared
        self.refuse = refuse
        self.statements: list[str] = []

    def execute(self, statement, parameters=None):
        self.statements.append(str(statement))
        if self.refuse and "CREATE" in str(statement):
            raise RuntimeError("CREATE TEMPORARY TABLES denied")
        if "SHOW COLUMNS" in str(statement):
            return [(f"c{index}", kind) for index, kind in enumerate(self.declared)]
        return None


def _connector_entry(name, code, flags=0, charset=BINARY):
    return (name, code, None, None, None, None, True, flags, charset)


@pytest.mark.parametrize(
    "declared,dtype",
    [
        ("decimal(10,2)", pl.Decimal(10, 2)),
        ("decimal(5,2) unsigned", pl.Decimal(5, 2)),
        ("decimal(1,1)", pl.Decimal(1, 1)),
        ("decimal(38,0)", pl.Decimal(38, 0)),
        ("decimal(65,30)", pl.String),
        (b"decimal(12,3)", pl.Decimal(12, 3)),
    ],
)
def test_mysqlconnector_gets_a_decimal_as_its_database_declares_it(declared, dtype):
    database = _Database(declared)
    query = described.Query(database, None, "SELECT d FROM t;", {"x": 1})
    entries = [_connector_entry("d", mysql.FieldType.NEWDECIMAL)]
    assert described.dtypes("mysqlconnector", entries, query) == {"d": dtype}
    create, show, drop = database.statements
    assert "CREATE TEMPORARY TABLE" in create
    assert "(SELECT d FROM t) AS q LIMIT 0" in create
    assert show.startswith("SHOW COLUMNS FROM td_described_")
    assert drop.startswith("DROP TEMPORARY TABLE td_described_")


def test_mysqlconnector_asks_its_database_only_for_a_decimal():
    database = _Database("int")
    query = described.Query(database, None, "SELECT i FROM t", {})
    entries = [_connector_entry("i", mysql.FieldType.LONG)]
    assert described.dtypes("mysqlconnector", entries, query) == {"i": pl.Int32()}
    assert database.statements == []


def test_mysqlconnector_leaves_a_decimal_to_inference_when_its_database_refuses():
    database = _Database("decimal(10,2)", refuse=True)
    query = described.Query(database, None, "SELECT d FROM t", {})
    entries = [_connector_entry("d", mysql.FieldType.NEWDECIMAL)]
    assert described.dtypes("mysqlconnector", entries, query) == {}


# --- Postgres -----------------------------------------------------------------


@pytest.mark.parametrize(
    "oid,name",
    [
        (16, "BOOLEAN"),
        (17, "BYTEA"),
        (18, "CHAR"),
        (19, "NAME"),
        (20, "BIGINT"),
        (21, "SMALLINT"),
        (23, "INTEGER"),
        (25, "TEXT"),
        (26, "OID"),
        (114, "JSON"),
        (142, "XML"),
        (650, "TEXT"),
        (700, "REAL"),
        (701, "DOUBLE PRECISION"),
        (774, "TEXT"),
        (790, "TEXT"),
        (829, "TEXT"),
        (869, "TEXT"),
        (1042, "BPCHAR"),
        (1043, "VARCHAR"),
        (1082, "DATE"),
        (1083, "TIME"),
        (1114, "TIMESTAMP"),
        (1184, "TIMESTAMPTZ"),
        (1186, "INTERVAL"),
        (1266, "TEXT"),
        (1560, "TEXT"),
        (1562, "TEXT"),
        (2950, "UUID"),
        (3614, "TEXT"),
        (3802, "JSON"),
        (600, None),
        (1007, None),
        (3904, None),
    ],
)
def test_postgres_names_a_column_by_its_oid(oid, name):
    assert postgres.describe(_column(oid)) == name


NUMERIC_SHAPES = [
    (precision, scale)
    for precision in (1, 2, 5, 10, 18, 19, 38, 39, 100, 1000)
    for scale in sorted({0, 1, precision // 2, precision})
]


@pytest.mark.parametrize("precision,scale", NUMERIC_SHAPES)
def test_postgres_names_a_numeric_by_its_precision_and_scale(precision, scale):
    name = postgres.describe(_column(postgres.NUMERIC, precision, scale))
    assert name == f"NUMERIC({precision}, {scale})"
    assert names.dtype(name) == names.decimal(precision, scale)


@pytest.mark.parametrize(
    "precision,described_scale,name",
    [
        (5, 2046, "NUMERIC(7, 0)"),
        (5, 2048 - 5, "NUMERIC(10, 0)"),
        (36, 2046, "NUMERIC(38, 0)"),
        (37, 2046, "NUMERIC(39, 0)"),
        (3, 5, "NUMERIC(5, 5)"),
        (1, 38, "NUMERIC(38, 38)"),
    ],
)
def test_postgres_names_a_numeric_by_what_a_negative_or_wide_scale_holds(
    precision, described_scale, name
):
    assert postgres.describe(_column(postgres.NUMERIC, precision, described_scale)) == (
        name
    )


@pytest.mark.parametrize(
    "precision,scale", [(65535, 65535), (None, None), (0, 0), ("10", 2)]
)
def test_postgres_leaves_an_unconstrained_numeric_to_inference(precision, scale):
    assert postgres.describe(_column(postgres.NUMERIC, precision, scale)) is None


def test_the_postgres_oids_are_psycopg2s_own():
    from psycopg2.extensions import string_types

    for oid in postgres.BY_OID:
        if oid in string_types:
            assert string_types[oid].values == (oid,) or oid in string_types[oid].values


# --- ODBC and pymssql ---------------------------------------------------------


@pytest.mark.parametrize(
    "code,display,precision,scale,name",
    [
        (int, 3, 3, 0, "SMALLINT"),
        (int, 5, 5, 0, "SMALLINT"),
        (int, 10, 10, 0, "INT"),
        (int, 19, 19, 0, "BIGINT"),
        (int, 7, 7, 0, "BIGINT"),
        (float, 24, 24, None, "REAL"),
        (float, 53, 53, None, "FLOAT"),
        (Decimal, 10, 10, 2, "DECIMAL(10, 2)"),
        (Decimal, 19, 19, 4, "DECIMAL(19, 4)"),
        (Decimal, 38, 38, 38, "DECIMAL(38, 38)"),
        (Decimal, None, None, None, None),
        (bool, 1, 1, 0, "BOOLEAN"),
        (str, 20, 20, 0, "VARCHAR"),
        (str, 0, 0, 0, "VARCHAR"),
        (str, 34, 34, 7, "TIMESTAMP WITH TIME ZONE"),
        (str, 30, 30, 3, "TIMESTAMP WITH TIME ZONE"),
        (str, 26, 26, 0, None),
        (bytes, 4, 4, 0, "VARBINARY"),
        (bytearray, 4, 4, 0, "VARBINARY"),
        (dt.datetime, 27, 27, 7, "DATETIME2"),
        (dt.date, 10, 10, 0, "DATE"),
        (dt.time, 16, 16, 7, "TIME"),
        (uuid.UUID, 36, 36, 0, "UUID"),
        (object, None, None, None, None),
    ],
)
def test_odbc_names_a_column_by_its_class_and_size(
    code, display, precision, scale, name
):
    column = Column.of(("c", code, display, display, precision, scale, True))
    assert odbc.describe(column) == name


@pytest.mark.parametrize(
    "group,name",
    [
        (mssql.Group.STRING, "VARCHAR"),
        (mssql.Group.BINARY, "VARBINARY"),
        (mssql.Group.NUMBER, None),
        (mssql.Group.DATETIME, None),
        (mssql.Group.DECIMAL, None),
    ],
)
def test_pymssql_names_only_text_and_bytes(group, name):
    assert mssql.pymssql(_column(int(group))) == name


# --- Oracle -------------------------------------------------------------------


@pytest.mark.parametrize(
    "kind,precision,scale,name",
    [
        ("DB_TYPE_NUMBER", 10, 2, "NUMBER(10, 2)"),
        ("DB_TYPE_NUMBER", 38, 38, "NUMBER(38, 38)"),
        ("DB_TYPE_NUMBER", 3, 5, "NUMBER(5, 5)"),
        ("DB_TYPE_NUMBER", 126, -127, "FLOAT(126)"),
        ("DB_TYPE_NUMBER", 10, -127, "FLOAT(10)"),
        ("DB_TYPE_NUMBER", 0, -127, None),
        ("DB_TYPE_NUMBER", 0, 0, None),
        ("DB_TYPE_NUMBER", None, None, None),
        ("DB_TYPE_BINARY_FLOAT", None, None, "BINARY_FLOAT"),
        ("DB_TYPE_BINARY_DOUBLE", None, None, "BINARY_DOUBLE"),
        ("DB_TYPE_BINARY_INTEGER", None, None, "BIGINT"),
        ("DB_TYPE_BOOLEAN", None, None, "BOOLEAN"),
        ("DB_TYPE_CHAR", None, None, "CHAR"),
        ("DB_TYPE_NCHAR", None, None, "NCHAR"),
        ("DB_TYPE_VARCHAR", None, None, "VARCHAR"),
        ("DB_TYPE_NVARCHAR", None, None, "NVARCHAR"),
        ("DB_TYPE_LONG", None, None, "LONG"),
        ("DB_TYPE_LONG_NVARCHAR", None, None, "LONG"),
        ("DB_TYPE_CLOB", None, None, "CLOB"),
        ("DB_TYPE_NCLOB", None, None, "NCLOB"),
        ("DB_TYPE_ROWID", None, None, "ROWID"),
        ("DB_TYPE_UROWID", None, None, "UROWID"),
        ("DB_TYPE_RAW", None, None, "RAW"),
        ("DB_TYPE_LONG_RAW", None, None, "LONG RAW"),
        ("DB_TYPE_BLOB", None, None, "BLOB"),
        ("DB_TYPE_DATE", None, None, "TIMESTAMP"),
        ("DB_TYPE_TIMESTAMP", None, None, "TIMESTAMP"),
        ("DB_TYPE_TIMESTAMP_TZ", None, None, "TIMESTAMP"),
        ("DB_TYPE_TIMESTAMP_LTZ", None, None, "TIMESTAMP"),
        ("DB_TYPE_INTERVAL_DS", None, None, "INTERVAL DAY TO SECOND"),
        ("DB_TYPE_INTERVAL_YM", None, None, oracle.INTERVAL_YM),
        ("DB_TYPE_JSON", None, None, "JSON"),
        ("DB_TYPE_XMLTYPE", None, None, "XMLTYPE"),
        ("DB_TYPE_OBJECT", None, None, None),
        ("DB_TYPE_CURSOR", None, None, None),
        ("DB_TYPE_BFILE", None, None, None),
    ],
)
def test_oracle_names_a_column_by_its_db_type(kind, precision, scale, name):
    import oracledb

    code = getattr(oracledb, kind)
    assert oracle.describe(_column(code, precision, scale)) == name


@pytest.mark.parametrize("precision", range(1, 39))
@pytest.mark.parametrize("scale", [0, -1, -2, -10])
def test_oracle_names_a_whole_number_by_the_narrowest_integer_that_holds_it(
    precision, scale
):
    import oracledb

    digits = precision - scale
    dtype = names.dtype(
        oracle.describe(_column(oracledb.DB_TYPE_NUMBER, precision, scale))
    )
    expected = (
        pl.Int8
        if digits <= 2
        else pl.Int16
        if digits <= 4
        else pl.Int32
        if digits <= 9
        else pl.Int64
        if digits <= 18
        else names.decimal(digits, 0)
    )
    assert dtype == expected


@pytest.mark.parametrize(
    "format_,dtype",
    [
        (2, pl.List(pl.Float32)),
        (3, pl.List(pl.Float64)),
        (4, pl.List(pl.Int8)),
        (5, pl.List(pl.UInt8)),
        (None, pl.List(pl.Float64)),
    ],
)
def test_oracle_names_a_vector_by_its_element_format(format_, dtype):
    import oracledb

    entry = SimpleNamespace(is_json=False, vector_format=format_)
    column = _column(oracledb.DB_TYPE_VECTOR)._replace(entry=entry)
    assert oracle.describe(column) == dtype


@pytest.mark.parametrize("kind", ["DB_TYPE_VARCHAR", "DB_TYPE_CLOB", "DB_TYPE_JSON"])
def test_oracle_names_a_column_holding_json_by_its_text(kind):
    import oracledb

    entry = SimpleNamespace(is_json=True, vector_format=None)
    column = _column(getattr(oracledb, kind))._replace(entry=entry)
    assert oracle.describe(column) == "JSON"


def test_oracle_ignores_a_code_that_is_not_a_db_type():
    assert oracle.describe(_column(5)) is None
    assert oracle.describe(_column(SimpleNamespace(name="NUMBER"))) is None


# --- Db2 ----------------------------------------------------------------------


def _db2(group, precision=None, scale=None, display=None):
    import ibm_db_dbi

    code = getattr(ibm_db_dbi, group)
    return Column.of(("c", code, display, display, precision, scale, True))


@pytest.mark.parametrize(
    "group,precision,scale,display,name",
    [
        ("NUMBER", 1, 0, 1, "BOOLEAN"),
        ("NUMBER", 5, 0, 6, "SMALLINT"),
        ("NUMBER", 10, 0, 11, "INTEGER"),
        ("NUMBER", None, None, None, "INTEGER"),
        ("BIGINT", 19, 0, 20, "BIGINT"),
        ("FLOAT", 7, 0, 13, "REAL"),
        ("FLOAT", 15, 0, 22, "DOUBLE"),
        ("FLOAT", 16, 0, 23, "DOUBLE"),
        ("FLOAT", 34, 0, 42, "DOUBLE"),
        ("DECIMAL", 10, 2, 12, "DECIMAL(10, 2)"),
        ("DECIMAL", 31, 31, 33, "DECIMAL(31, 31)"),
        ("DECIMAL", None, None, None, None),
        ("STRING", 20, 0, 20, "VARCHAR"),
        ("STRING", 32700, 0, 32700, "VARCHAR"),
        ("STRING", 2, 0, 4, None),
        ("STRING", 20, 0, 40, None),
        ("TEXT", 1048576, 0, 1048576, "CLOB"),
        ("XML", 0, 0, 0, "XML"),
        ("BINARY", 1048576, 0, 2097152, "BLOB"),
        ("DATE", 10, 0, 10, "DATE"),
        ("TIME", 8, 0, 8, "TIME"),
        ("DATETIME", 26, 6, 26, "TIMESTAMP"),
        ("BOOLEAN", 1, 0, 1, "BOOLEAN"),
        ("ROWID", None, None, None, None),
    ],
)
def test_db2_names_a_column_by_its_group_and_precision(
    group, precision, scale, display, name
):
    assert db2.describe(_db2(group, precision, scale, display)) == name


def test_every_db2_group_member_fits_the_type_it_is_named_by():
    import ibm_db_dbi

    for member in ("SMALLINT", "INT", "INTEGER"):
        assert ibm_db_dbi.NUMBER == member
    for member in ("DOUBLE", "REAL", "DECFLOAT", "FLOAT"):
        assert ibm_db_dbi.FLOAT == member


# --- registry -----------------------------------------------------------------


@pytest.fixture
def registry(monkeypatch):
    registered: dict = {}
    points: dict = {}
    monkeypatch.setattr(described, "_registered", registered)
    monkeypatch.setattr(
        described,
        "entry_points",
        lambda group, name: [points[name]] if name in points else [],
    )
    return SimpleNamespace(registered=registered, points=points)


def _point(load, value="pkg.mod:describe"):
    return SimpleNamespace(load=load, value=value)


def _integer(column):
    return "INTEGER"


def _text(column):
    return "TEXT"


@pytest.mark.parametrize("driver", list(Driver))
def test_every_built_in_driver_has_its_describer(registry, driver):
    assert described.describer(driver) is not None


def test_the_drivers_are_named_as_sqlalchemy_names_them():
    from sqlalchemy import create_engine as engine_for

    for url, driver in [
        ("mysql+mysqlconnector://u:p@h/d", Driver.MYSQLCONNECTOR),
        ("mysql+pymysql://u:p@h/d", Driver.PYMYSQL),
        ("postgresql+psycopg2://u:p@h/d", Driver.PSYCOPG2),
        ("oracle+oracledb://u:p@h/d", Driver.ORACLEDB),
        ("db2+ibm_db://u:p@h/d", Driver.IBM_DB),
    ]:
        assert engine_for(url).dialect.driver == driver


def test_a_driver_without_a_describer_has_none(registry):
    assert described.describer("pysqlite") is None


def test_a_new_driver_is_registered(registry):
    described.register("duckdb_engine", _integer)
    assert described.describer("duckdb_engine") is _integer


def test_a_built_in_driver_cannot_be_registered(registry, caplog):
    described.register(Driver.PSYCOPG2, _text)
    assert described.describer(Driver.PSYCOPG2) is postgres.describe
    assert "SQL driver 'psycopg2' already has a describer" in caplog.text


def test_a_registered_driver_cannot_be_registered_again(registry, caplog):
    described.register("duckdb_engine", _integer)
    described.register("duckdb_engine", _text)
    assert described.describer("duckdb_engine") is _integer
    assert "SQL driver 'duckdb_engine' already has a describer" in caplog.text


def test_a_driver_with_an_entry_point_cannot_be_registered(registry, caplog):
    registry.points["duckdb_engine"] = _point(lambda: _integer)
    described.register("duckdb_engine", _text)
    assert described.describer("duckdb_engine") is _integer
    assert "already has a describer" in caplog.text


def test_an_entry_point_describes_a_driver_without_a_built_in(registry):
    registry.points["duckdb_engine"] = _point(lambda: _integer)
    assert described.describer("duckdb_engine") is _integer


def test_an_entry_point_for_a_built_in_driver_is_ignored(registry, caplog):
    registry.points[Driver.PYODBC] = _point(lambda: _text)
    assert described.describer(Driver.PYODBC) is odbc.describe
    assert "SQL driver 'pyodbc' has a built-in describer" in caplog.text


@pytest.mark.parametrize("error", [ImportError("missing"), AttributeError("gone")])
def test_an_entry_point_that_cannot_load_leaves_the_driver_undescribed(
    registry, caplog, error
):
    def broken():
        raise error

    registry.points["duckdb_engine"] = _point(broken, "broken.mod:describe")
    assert described.describer("duckdb_engine") is None
    assert "Cannot load the describer 'broken.mod:describe'" in caplog.text


def test_an_entry_point_that_fails_otherwise_is_not_hidden(registry):
    def broken():
        raise RuntimeError("bug in the plugin")

    registry.points["duckdb_engine"] = _point(broken)
    with pytest.raises(RuntimeError, match="bug in the plugin"):
        described.describer("duckdb_engine")


# --- dtypes -------------------------------------------------------------------


def test_dtypes_types_each_described_column_by_name(registry):
    description = [
        ("id", 20, None, None, None, None, False),
        ("amount", 1700, None, None, 10, 2, True),
        ("payload", 3802, None, None, None, None, True),
        ("spot", 600, None, None, None, None, True),
    ]
    assert described.dtypes(Driver.PSYCOPG2, description) == {
        "id": pl.Int64,
        "amount": pl.Decimal(10, 2),
        "payload": pl.String,
    }


def test_dtypes_warns_once_about_every_column_it_cannot_type(registry, caplog):
    description = [
        ("id", 20, None, None, None, None, False),
        ("spot", 600, None, None, None, None, True),
        ("tags", 1009, None, None, None, None, True),
    ]
    described.dtypes(Driver.PSYCOPG2, description)
    warnings = [r.getMessage() for r in caplog.records if r.levelname == "WARNING"]
    assert warnings == [
        "The description of SQL driver 'psycopg2' gives no type for column(s)"
        " 'spot', 'tags'; they are typed from their values in each batch"
    ]


def test_dtypes_does_not_warn_when_every_column_is_typed(registry, caplog):
    described.dtypes(Driver.PSYCOPG2, [("id", 20, None, None, None, None, False)])
    assert not [r for r in caplog.records if r.levelname == "WARNING"]


def test_dtypes_takes_what_a_driver_without_a_describer_reports(registry):
    description = [
        ("named", "DECIMAL", None, None, 10, 2, True),
        ("named_with_parameters", "VARCHAR(20)", None, None, None, None, True),
        ("classed", dt.date, None, None, None, None, True),
        ("opaque", 7, None, None, None, None, True),
        ("unknown", "GEOMETRY", None, None, None, None, True),
    ]
    assert described.dtypes("duckdb_engine", description) == {
        "named": pl.Decimal(10, 2),
        "named_with_parameters": pl.String,
        "classed": pl.Date,
    }


def test_dtypes_takes_what_the_driver_reports_when_its_describer_cannot_tell(
    registry,
):
    description = [("named", "BIGINT", None, None, None, None, True)]
    assert described.dtypes(Driver.PYMSSQL, description) == {"named": pl.Int64}


def test_dtypes_prefers_the_describer_to_what_the_driver_reports(registry):
    described.register("duckdb_engine", lambda column: "SMALLINT")
    description = [("n", "BIGINT", None, None, None, None, True)]
    assert described.dtypes("duckdb_engine", description) == {"n": pl.Int16}


def test_dtypes_of_no_description_is_empty(registry):
    assert described.dtypes(Driver.PSYCOPG2, None) == {}
    assert described.dtypes(Driver.PSYCOPG2, []) == {}


# --- reading ------------------------------------------------------------------


@pytest.fixture
def engine(tmp_path):
    engine = create_engine(f"sqlite:///{tmp_path / 'described.db'}")
    with engine.begin() as conn:
        conn.execute(text("CREATE TABLE t (id INTEGER, n INTEGER, s TEXT)"))
        conn.execute(
            text("INSERT INTO t (id) VALUES (:id)"), [{"id": i} for i in range(150)]
        )
        conn.execute(text("INSERT INTO t VALUES (150, 7, 'x')"))
    yield engine
    engine.dispose()


def _read(engine, chunk_size=1000, overrides=None, query="SELECT * FROM t"):
    with engine.connect() as conn:
        return list(
            sar._polars_read(
                connection=conn,
                query=query,
                chunk_size=chunk_size,
                schema_overrides=overrides,
            )
        )


def _null_warnings(caplog) -> list[str]:
    return sorted(
        r.getMessage()
        for r in caplog.records
        if r.levelname == "WARNING" and "NULL in every row" in r.getMessage()
    )


def test_a_value_after_more_than_a_hundred_nulls_is_read(engine, registry):
    [batch] = _read(engine)
    assert batch.schema["n"] == pl.Int64
    assert batch.schema["s"] == pl.String
    assert batch.filter(pl.col("id") == 150).row(0) == (150, 7, "x")


def test_a_described_column_has_one_dtype_in_every_batch(engine, registry):
    described.register("pysqlite", lambda column: {"n": "SMALLINT"}.get(column.name))
    batches = _read(engine, chunk_size=100)
    assert [batch.height for batch in batches] == [100, 51]
    assert [batch.schema["n"] for batch in batches] == [pl.Int16, pl.Int16]


def test_a_described_column_null_in_a_whole_batch_is_not_warned_about(
    engine, registry, caplog
):
    described.register(
        "pysqlite",
        lambda column: {"n": "SMALLINT", "s": "TEXT"}.get(column.name),
    )
    _read(engine, chunk_size=100)
    assert _null_warnings(caplog) == []


def test_an_untyped_column_null_in_a_whole_batch_is_warned_about_once(
    engine, registry, caplog
):
    batches = _read(engine, chunk_size=50)
    assert [batch.schema["n"] for batch in batches] == [
        pl.Null,
        pl.Null,
        pl.Null,
        pl.Int64,
    ]
    assert _null_warnings(caplog) == [
        f"Column {name!r} has no type in the query's description and is NULL in"
        f" every row of a batch, so it is typed Null there"
        for name in ("n", "s")
    ]


def test_the_schema_overrides_win_over_the_description(engine, registry):
    described.register("pysqlite", lambda column: {"n": "SMALLINT"}.get(column.name))
    [batch] = _read(engine, overrides={"n": pl.Float64, "absent": pl.Int8})
    assert batch.schema["n"] == pl.Float64
    assert "absent" not in batch.columns


def test_a_value_the_described_dtype_cannot_hold_fails_the_read(engine, registry):
    described.register("pysqlite", lambda column: {"s": "BIGINT"}.get(column.name))
    with pytest.raises(pl.exceptions.InvalidOperationError):
        _read(engine)


def test_an_empty_result_yields_no_batch(engine, registry):
    assert _read(engine, query="SELECT * FROM t WHERE id < 0") == []


def test_the_query_parameters_reach_the_query(engine, registry):
    with engine.connect() as conn:
        [batch] = list(
            sar._polars_read(
                connection=conn,
                query="SELECT id FROM t WHERE id = :wanted",
                chunk_size=10,
                schema_overrides=None,
                parameters={"wanted": 150},
            )
        )
    assert batch["id"].to_list() == [150]


def test_a_column_is_typed_by_the_name_its_driver_describes(engine, registry):
    described.register("pysqlite", lambda column: {"N": "SMALLINT"}.get(column.name))
    [batch] = _read(engine, query='SELECT id, n AS "N" FROM t')
    assert batch.schema["N"] == pl.Int16


class _Result:
    def __init__(self, description, keys):
        self.cursor = type("Cursor", (), {"description": description})()
        self._keys = keys

    def keys(self):
        return self._keys


def test_the_columns_are_named_as_the_driver_describes_them():
    description = [("EMP_NO", 20), ("FIRST_NAME", 25)]
    result = _Result(description, ["emp_no", "first_name"])
    assert sar._columns(result) == (["EMP_NO", "FIRST_NAME"], description)


def test_the_columns_fall_back_to_the_result_keys_without_a_description():
    assert sar._columns(_Result(None, ["id"])) == (["id"], None)
