#
# Copyright 2026 Tabs Data Inc.
#

import pytest
import sqlalchemy.types as sa_types

from tabsdata.connector.sql.common._database.db2.dialect import Db2Dialect
from tabsdata.connector.sql.common._database.mssql.dialect import MSSQLDialect
from tabsdata.connector.sql.common._database.mysql.dialect import MySQLDialect
from tabsdata.connector.sql.common._database.oracle.dialect import OracleDialect
from tabsdata.connector.sql.common._database.postgres.dialect import PostgresDialect


@pytest.mark.parametrize(
    "dialect, type_string, expected_type",
    [
        (Db2Dialect(), "INTEGER", sa_types.Integer),
        (Db2Dialect(), "BIGINT", sa_types.BigInteger),
        (Db2Dialect(), "SMALLINT", sa_types.SmallInteger),
        (Db2Dialect(), "BOOLEAN", sa_types.Boolean),
        (Db2Dialect(), "CLOB", sa_types.Text),
        (Db2Dialect(), "DATE", sa_types.Date),
        (Db2Dialect(), "TIMESTAMP", sa_types.DateTime),
        (Db2Dialect(), "TIME", sa_types.Time),
        (Db2Dialect(), "DOUBLE", sa_types.Float),
        (Db2Dialect(), "REAL", sa_types.Float),
        (Db2Dialect(), "FLOAT", sa_types.Float),
        (Db2Dialect(), "BLOB", sa_types.LargeBinary),
        (MSSQLDialect(), "int", sa_types.Integer),
        (MSSQLDialect(), "bigint", sa_types.BigInteger),
        (MSSQLDialect(), "smallint", sa_types.SmallInteger),
        (MSSQLDialect(), "bit", sa_types.Boolean),
        (MSSQLDialect(), "text", sa_types.Text),
        (MSSQLDialect(), "date", sa_types.Date),
        (MSSQLDialect(), "datetime2", sa_types.DateTime),
        (MSSQLDialect(), "time", sa_types.Time),
        (MSSQLDialect(), "float", sa_types.Float),
        (MSSQLDialect(), "real", sa_types.Float),
        (MSSQLDialect(), "uniqueidentifier", sa_types.Uuid),
        (MSSQLDialect(), "image", sa_types.LargeBinary),
        (MSSQLDialect(), "xml", sa_types.Text),
        (MSSQLDialect(), "money", sa_types.Numeric),
        (MSSQLDialect(), "json", sa_types.JSON),
        (MySQLDialect(), "int", sa_types.Integer),
        (MySQLDialect(), "bigint", sa_types.BigInteger),
        (MySQLDialect(), "smallint", sa_types.SmallInteger),
        (MySQLDialect(), "tinyint", sa_types.SmallInteger),
        (MySQLDialect(), "mediumint", sa_types.Integer),
        (MySQLDialect(), "boolean", sa_types.Boolean),
        (MySQLDialect(), "text", sa_types.Text),
        (MySQLDialect(), "date", sa_types.Date),
        (MySQLDialect(), "datetime", sa_types.DateTime),
        (MySQLDialect(), "time", sa_types.Time),
        (MySQLDialect(), "float", sa_types.Float),
        (MySQLDialect(), "double", sa_types.Float),
        (MySQLDialect(), "json", sa_types.JSON),
        (MySQLDialect(), "blob", sa_types.LargeBinary),
        (MySQLDialect(), "year", sa_types.Integer),
        (OracleDialect(), "NUMBER", sa_types.Numeric),
        (OracleDialect(), "DATE", sa_types.Date),
        (OracleDialect(), "TIMESTAMP", sa_types.DateTime),
        (OracleDialect(), "TIMESTAMP WITH TIME ZONE", sa_types.DateTime),
        (OracleDialect(), "FLOAT", sa_types.Float),
        (OracleDialect(), "BINARY_DOUBLE", sa_types.Float),
        (OracleDialect(), "BINARY_FLOAT", sa_types.Float),
        (OracleDialect(), "BLOB", sa_types.LargeBinary),
        (OracleDialect(), "CLOB", sa_types.Text),
        (PostgresDialect(), "integer", sa_types.Integer),
        (PostgresDialect(), "bigint", sa_types.BigInteger),
        (PostgresDialect(), "smallint", sa_types.SmallInteger),
        (PostgresDialect(), "boolean", sa_types.Boolean),
        (PostgresDialect(), "text", sa_types.Text),
        (PostgresDialect(), "date", sa_types.Date),
        (PostgresDialect(), "timestamp with time zone", sa_types.DateTime),
        (PostgresDialect(), "timestamp without time zone", sa_types.DateTime),
        (PostgresDialect(), "time with time zone", sa_types.Time),
        (PostgresDialect(), "double precision", sa_types.Float),
        (PostgresDialect(), "real", sa_types.Float),
        (PostgresDialect(), "json", sa_types.JSON),
        (PostgresDialect(), "jsonb", sa_types.JSON),
        (PostgresDialect(), "uuid", sa_types.Uuid),
        (PostgresDialect(), "bytea", sa_types.LargeBinary),
    ],
    ids=lambda v: repr(v) if isinstance(v, str) else "",
)
def test_ddl_to_sa_base_types(dialect, type_string, expected_type):
    result = dialect.ddl_to_sa(type_string)
    assert isinstance(result, expected_type), (
        f"{type(dialect).__name__}.ddl_to_sa({type_string!r}) "
        f"returned {result!r}, expected instance of {expected_type.__name__}"
    )


@pytest.mark.parametrize(
    "dialect, type_string, expected_precision, expected_scale",
    [
        (Db2Dialect(), "DECIMAL(10,2)", 10, 2),
        (Db2Dialect(), "DECIMAL(8,3)", 8, 3),
        (MSSQLDialect(), "decimal(18,4)", 18, 4),
        (MSSQLDialect(), "decimal(10,2)", 10, 2),
        (MySQLDialect(), "decimal(10,2)", 10, 2),
        (MySQLDialect(), "decimal(8,3)", 8, 3),
        (OracleDialect(), "NUMBER(10,2)", 10, 2),
        (OracleDialect(), "NUMBER(18,4)", 18, 4),
        (PostgresDialect(), "numeric(10,2)", 10, 2),
        (PostgresDialect(), "numeric(18,4)", 18, 4),
    ],
    ids=lambda v: repr(v) if isinstance(v, str) else "",
)
def test_ddl_to_sa_numeric_precision(
    dialect, type_string, expected_precision, expected_scale
):
    result = dialect.ddl_to_sa(type_string)
    assert isinstance(result, sa_types.Numeric)
    assert result.precision == expected_precision
    assert result.scale == expected_scale


@pytest.mark.parametrize(
    "dialect, type_string, expected_length",
    [
        (Db2Dialect(), "VARCHAR(255)", 255),
        (Db2Dialect(), "CHAR(10)", 10),
        (MSSQLDialect(), "varchar(255)", 255),
        (MSSQLDialect(), "nvarchar(100)", 100),
        (MySQLDialect(), "varchar(255)", 255),
        (MySQLDialect(), "char(10)", 10),
        (OracleDialect(), "VARCHAR2(255)", 255),
        (OracleDialect(), "CHAR(10)", 10),
        (PostgresDialect(), "character varying(255)", 255),
        (PostgresDialect(), "character varying(100)", 100),
    ],
    ids=lambda v: repr(v) if isinstance(v, str) else "",
)
def test_ddl_to_sa_string_length(dialect, type_string, expected_length):
    result = dialect.ddl_to_sa(type_string)
    assert isinstance(result, (sa_types.String, sa_types.Unicode))
    assert result.length == expected_length
