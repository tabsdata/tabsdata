#
# Copyright 2026 Tabsdata Inc.
#

"""The typing of a SQL source's columns from its cursor description, offline:
the name parser, every driver's describer against that driver's own constants,
the registry, and the reader over SQLite."""

from __future__ import annotations

import datetime as dt
from decimal import Decimal
from types import SimpleNamespace

import polars as pl
import pytest
from sqlalchemy import create_engine, text

# noinspection PyProtectedMember
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
        ("DECIMAL(39, 2)", None),
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
        ("JSON", None),
        ("JSONB", None),
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
        (10, 2, pl.Decimal(10, 2)),
        (38, 0, pl.Decimal(38, 0)),
        (39, 0, None),
        (0, 0, None),
        (10, -1, None),
        (10, 11, None),
        (None, 2, None),
        (10, None, None),
    ],
)
def test_a_decimal_needs_a_precision_and_scale_polars_can_hold(precision, scale, dtype):
    assert names.decimal(precision, scale) == dtype


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
    column = Column.of(("id", 3, 11, 4, 10, 0, False, 32, 63))
    assert column.name == "id"
    assert column.code == 3
    assert (column.display_size, column.internal_size) == (11, 4)
    assert (column.precision, column.scale, column.null_ok) == (10, 0, False)
    assert column.extra == (32, 63)


def test_a_short_entry_leaves_the_missing_fields_empty():
    column = Column.of(("id", 3))
    assert (column.precision, column.scale, column.extra) == (None, None, ())


# --- MySQL ------------------------------------------------------------------


def test_the_mysql_field_types_are_the_drivers_own():
    from mysql.connector.constants import FieldType as Connector
    from pymysql.constants import FIELD_TYPE

    for member in mysql.FieldType:
        assert getattr(Connector, member.name) == member.value
        assert getattr(FIELD_TYPE, member.name) == member.value


@pytest.mark.parametrize(
    "code,flags,charset,name",
    [
        (mysql.FieldType.TINY, 0, 63, "TINYINT"),
        (mysql.FieldType.TINY, 32, 63, "TINYINT UNSIGNED"),
        (mysql.FieldType.SHORT, 32, 63, "SMALLINT UNSIGNED"),
        (mysql.FieldType.INT24, 0, 63, "MEDIUMINT"),
        (mysql.FieldType.LONG, 32, 63, "INT UNSIGNED"),
        (mysql.FieldType.LONGLONG, 0, 63, "BIGINT"),
        (mysql.FieldType.LONGLONG, 32 | 1, 63, "BIGINT UNSIGNED"),
        (mysql.FieldType.VARCHAR, 0, 45, "VARCHAR"),
        (mysql.FieldType.VAR_STRING, 0, 45, "VARCHAR"),
        (mysql.FieldType.VAR_STRING, 128, 63, "VARBINARY"),
        (mysql.FieldType.STRING, 0, 33, "VARCHAR"),
        (mysql.FieldType.BLOB, 16, 45, "VARCHAR"),
        (mysql.FieldType.BLOB, 144, 63, "VARBINARY"),
        (mysql.FieldType.LONG_BLOB, 144, 63, "VARBINARY"),
        (mysql.FieldType.YEAR, 0, 63, "YEAR"),
        (mysql.FieldType.FLOAT, 0, 63, "REAL"),
        (mysql.FieldType.DOUBLE, 0, 63, "DOUBLE"),
        (mysql.FieldType.DATE, 0, 63, "DATE"),
        (mysql.FieldType.DATETIME, 0, 63, "DATETIME"),
        (mysql.FieldType.TIMESTAMP, 0, 63, "DATETIME"),
        (mysql.FieldType.TIME, 0, 63, "INTERVAL"),
        (mysql.FieldType.ENUM, 0, 45, "ENUM"),
        (246, 0, 63, None),
        (245, 0, 63, None),
    ],
)
def test_mysqlconnector_names_by_type_flags_and_charset(code, flags, charset, name):
    assert mysql.mysqlconnector(_column(code, None, None, flags, charset)) == name


def test_mysqlconnector_without_flags_or_charset_takes_signed_text():
    assert mysql.mysqlconnector(_column(mysql.FieldType.LONG)) == "INT"
    assert mysql.mysqlconnector(_column(mysql.FieldType.BLOB)) == "VARCHAR"


@pytest.mark.parametrize(
    "code,flags,name",
    [
        (mysql.FieldType.SHORT, 32, "SMALLINT UNSIGNED"),
        (mysql.FieldType.LONGLONG, 0, "BIGINT"),
        (mysql.FieldType.VAR_STRING, 0, None),
        (mysql.FieldType.BLOB, 128, None),
        (mysql.FieldType.DATE, 0, "DATE"),
    ],
)
def test_mariadbconnector_reads_the_flags_but_leaves_text_and_bytes(code, flags, name):
    assert mysql.mariadbconnector(_column(code, None, None, flags)) == name


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
    ],
)
def test_the_flagless_drivers_widen_integers_and_leave_text_and_bytes(code, name):
    assert mysql.flagless(_column(code)) == name


# --- Postgres -----------------------------------------------------------------


@pytest.mark.parametrize(
    "oid,precision,scale,name",
    [
        (16, None, None, "BOOLEAN"),
        (17, None, None, "BYTEA"),
        (20, None, None, "BIGINT"),
        (21, None, None, "SMALLINT"),
        (23, None, None, "INTEGER"),
        (25, None, None, "TEXT"),
        (700, None, None, "REAL"),
        (701, None, None, "DOUBLE PRECISION"),
        (1043, None, None, "VARCHAR"),
        (1082, None, None, "DATE"),
        (1083, None, None, "TIME"),
        (1114, None, None, "TIMESTAMP"),
        (1184, None, None, "TIMESTAMPTZ"),
        (1186, None, None, "INTERVAL"),
        (2950, None, None, "UUID"),
        (1700, 10, 2, "NUMERIC(10, 2)"),
        (1700, None, None, None),
        (114, None, None, None),
        (3802, None, None, None),
        (1007, None, None, None),
    ],
)
def test_postgres_names_a_column_by_its_oid(oid, precision, scale, name):
    assert postgres.describe(_column(oid, precision, scale)) == name


def test_the_postgres_oids_are_psycopg2s_own():
    from psycopg2.extensions import string_types

    for oid in postgres.BY_OID:
        if oid in string_types:
            assert string_types[oid].values == (oid,) or oid in string_types[oid].values


# --- ODBC and pymssql ---------------------------------------------------------


@pytest.mark.parametrize(
    "code,precision,scale,name",
    [
        (int, 3, 0, "SMALLINT"),
        (int, 5, 0, "SMALLINT"),
        (int, 10, 0, "INT"),
        (int, 19, 0, "BIGINT"),
        (int, 7, 0, "BIGINT"),
        (float, 24, None, "REAL"),
        (float, 53, None, "FLOAT"),
        (Decimal, 10, 2, "DECIMAL(10, 2)"),
        (Decimal, None, None, None),
        (bool, 1, 0, "BOOLEAN"),
        (str, 20, 0, "VARCHAR"),
        (bytes, 4, 0, "VARBINARY"),
        (bytearray, 4, 0, "VARBINARY"),
        (dt.datetime, 27, 7, "DATETIME2"),
        (dt.date, 10, 0, "DATE"),
        (dt.time, 16, 7, "TIME"),
        (object, None, None, None),
    ],
)
def test_odbc_names_a_column_by_its_class_and_size(code, precision, scale, name):
    assert odbc.describe(_column(code, precision, scale)) == name


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
        ("DB_TYPE_NUMBER", 10, 0, "BIGINT"),
        ("DB_TYPE_NUMBER", 18, 0, "BIGINT"),
        ("DB_TYPE_NUMBER", 19, 0, "NUMBER(19, 0)"),
        ("DB_TYPE_NUMBER", 10, 2, "NUMBER(10, 2)"),
        ("DB_TYPE_NUMBER", 126, -127, "FLOAT(126)"),
        ("DB_TYPE_NUMBER", 0, -127, None),
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
        ("DB_TYPE_RAW", None, None, "RAW"),
        ("DB_TYPE_LONG_RAW", None, None, "LONG RAW"),
        ("DB_TYPE_BLOB", None, None, "BLOB"),
        ("DB_TYPE_DATE", None, None, "TIMESTAMP"),
        ("DB_TYPE_TIMESTAMP", None, None, "TIMESTAMP"),
        ("DB_TYPE_INTERVAL_DS", None, None, "INTERVAL DAY TO SECOND"),
        ("DB_TYPE_TIMESTAMP_TZ", None, None, None),
        ("DB_TYPE_TIMESTAMP_LTZ", None, None, None),
        ("DB_TYPE_INTERVAL_YM", None, None, None),
        ("DB_TYPE_JSON", None, None, None),
    ],
)
def test_oracle_names_a_column_by_its_db_type(kind, precision, scale, name):
    import oracledb

    code = getattr(oracledb, kind)
    assert oracle.describe(_column(code, precision, scale)) == name


@pytest.mark.parametrize(
    "kind,dtype",
    [
        ("DB_TYPE_NUMBER", pl.Int64),
        ("DB_TYPE_DATE", pl.Datetime("us")),
        ("DB_TYPE_CLOB", pl.String),
        ("DB_TYPE_RAW", pl.Binary),
        ("DB_TYPE_BINARY_FLOAT", pl.Float32),
        ("DB_TYPE_INTERVAL_DS", pl.Duration("us")),
    ],
)
def test_every_name_oracle_gives_has_a_dtype(kind, dtype):
    import oracledb

    name = oracle.describe(_column(getattr(oracledb, kind), 10, 0))
    assert names.dtype(name) == dtype


def test_oracle_ignores_a_code_that_is_not_a_db_type():
    assert oracle.describe(_column(5)) is None
    assert oracle.describe(_column(SimpleNamespace(name="NUMBER"))) is None


# --- Db2 ----------------------------------------------------------------------


@pytest.mark.parametrize(
    "group,precision,scale,name",
    [
        ("STRING", None, None, "VARCHAR"),
        ("TEXT", None, None, "CLOB"),
        ("XML", None, None, "XML"),
        ("BINARY", None, None, "BLOB"),
        ("NUMBER", None, None, "INTEGER"),
        ("BIGINT", None, None, "BIGINT"),
        ("FLOAT", None, None, "DOUBLE"),
        ("DECIMAL", 10, 2, "DECIMAL(10, 2)"),
        ("DECIMAL", None, None, None),
        ("DATE", None, None, "DATE"),
        ("TIME", None, None, "TIME"),
        ("DATETIME", None, None, "TIMESTAMP"),
        ("BOOLEAN", None, None, "BOOLEAN"),
        ("ROWID", None, None, None),
    ],
)
def test_db2_names_a_column_by_its_type_group(group, precision, scale, name):
    import ibm_db_dbi

    code = getattr(ibm_db_dbi, group)
    assert db2.describe(_column(code, precision, scale)) == name


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
    ]
    assert described.dtypes(Driver.PSYCOPG2, description) == {
        "id": pl.Int64,
        "amount": pl.Decimal(10, 2),
    }


def test_dtypes_warns_once_about_every_column_it_cannot_type(registry, caplog):
    description = [
        ("id", 20, None, None, None, None, False),
        ("payload", 3802, None, None, None, None, True),
        ("tags", 1009, None, None, None, None, True),
    ]
    described.dtypes(Driver.PSYCOPG2, description)
    warnings = [r.getMessage() for r in caplog.records if r.levelname == "WARNING"]
    assert warnings == [
        "The description of SQL driver 'psycopg2' gives no type for column(s)"
        " 'payload', 'tags'; they are typed from their values in each batch"
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


def test_a_column_is_typed_by_the_name_sqlalchemy_gives_it(engine, registry):
    def by_upper_name(column):
        return {"N": "SMALLINT"}.get(column.name.upper())

    described.register("pysqlite", by_upper_name)
    [batch] = _read(engine, query='SELECT id, n AS "N" FROM t')
    assert batch.schema["N"] == pl.Int16


def test_the_description_is_named_by_position():
    description = [("ID", 20), ("NAME", 25)]
    assert sar._named(["id", "name"], description) == [("id", 20), ("name", 25)]


def test_a_description_that_does_not_match_the_columns_is_dropped():
    assert sar._named(["id"], [("ID", 20), ("NAME", 25)]) is None
    assert sar._named(["id"], None) is None
