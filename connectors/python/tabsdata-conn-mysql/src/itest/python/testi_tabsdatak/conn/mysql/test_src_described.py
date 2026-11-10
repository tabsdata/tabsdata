#
# Copyright 2026 Tabsdata Inc.
#

"""MySQL source columns typed from what the driver describes; see
:mod:`testi_tabsdatak.conn.common.sql.described`. Each case runs under
pymysql, the connector's driver, and mysql-connector, which a URI can name:
pymysql's field list gives a DECIMAL's precision exactly, and mysql-connector
gets it from the database."""

from __future__ import annotations

import datetime as dt
from decimal import Decimal
from pathlib import Path

import polars as pl
import pytest
from sqlalchemy import make_url
from tabsdatax.itest.fixture.backends.mysql import MysqlDomain, mysql_domain
from tabsdatax.itest.fixture.functions.files import PluginSrcCtx
from tabsdatax.itest.fixture.functions.sql import sql_credentials
from testi_tabsdatak.conn.common.sql.described import (
    CHUNK_SIZES,
    TypeCase,
    check,
    decimal_cases,
    shapes,
    table_for,
)
from testi_tabsdatak.conn.common.sql.plugin_io import work_dir
from testi_tabsdatak.conn.mysql.backend import store

# noinspection PyProtectedMember
from tabsdatak.conn.mysql import MySQLSrc, MySQLSrcConn, _plugin as plugin

pytestmark = pytest.mark.mysql

DRIVERS = [
    pytest.param("pymysql", id="pymysql"),
    pytest.param("mysqlconnector", id="mysqlconnector"),
]

CASES = [
    TypeCase("tinyint", "TINYINT", "-128", pl.Int8, -128),
    TypeCase("tinyint_unsigned", "TINYINT UNSIGNED", "255", pl.UInt8, 255),
    TypeCase("bool", "BOOL", "1", pl.Int8, 1),
    TypeCase("smallint", "SMALLINT", "-32768", pl.Int16, -32768),
    TypeCase("smallint_unsigned", "SMALLINT UNSIGNED", "65535", pl.UInt16, 65535),
    TypeCase("mediumint", "MEDIUMINT", "-8388608", pl.Int32, -8388608),
    TypeCase(
        "mediumint_unsigned", "MEDIUMINT UNSIGNED", "16777215", pl.UInt32, 16777215
    ),
    TypeCase("int", "INT", str(-(2**31)), pl.Int32, -(2**31)),
    TypeCase("int_unsigned", "INT UNSIGNED", str(2**32 - 1), pl.UInt32, 2**32 - 1),
    TypeCase("bigint", "BIGINT", str(-(2**63)), pl.Int64, -(2**63)),
    TypeCase(
        "bigint_unsigned", "BIGINT UNSIGNED", str(2**64 - 1), pl.UInt64, 2**64 - 1
    ),
    TypeCase("decimal", "DECIMAL(10, 2)", "12.34", pl.Decimal(10, 2), Decimal("12.34")),
    TypeCase("decimal_default", "DECIMAL", "-7", pl.Decimal(10, 0), Decimal(-7)),
    TypeCase("numeric", "NUMERIC(7, 3)", "-1.5", pl.Decimal(7, 3), Decimal("-1.5")),
    TypeCase("float", "FLOAT", "1.5", pl.Float32, 1.5),
    TypeCase("float_30", "FLOAT(30)", "1.5", pl.Float64, 1.5),
    TypeCase("double", "DOUBLE", "1.5", pl.Float64, 1.5),
    TypeCase("bit_1", "BIT(1)", "b'1'", pl.UInt64, 1),
    TypeCase("bit_64", "BIT(64)", "b'" + "1" * 64 + "'", pl.UInt64, 2**64 - 1),
    TypeCase("year", "YEAR", "2026", pl.Int16, 2026),
    TypeCase("date", "DATE", "'2026-04-19'", pl.Date, dt.date(2026, 4, 19)),
    TypeCase(
        "datetime",
        "DATETIME",
        "'2026-04-19 06:30:24'",
        pl.Datetime("us"),
        dt.datetime(2026, 4, 19, 6, 30, 24),
    ),
    TypeCase(
        "datetime_6",
        "DATETIME(6)",
        "'2026-04-19 06:30:24.123456'",
        pl.Datetime("us"),
        dt.datetime(2026, 4, 19, 6, 30, 24, 123456),
    ),
    TypeCase(
        "timestamp",
        "TIMESTAMP NULL",
        "'2026-04-19 06:30:24'",
        pl.Datetime("us"),
        dt.datetime(2026, 4, 19, 6, 30, 24),
    ),
    TypeCase(
        "time",
        "TIME",
        "'-838:59:59'",
        pl.Duration("us"),
        -dt.timedelta(hours=838, minutes=59, seconds=59),
    ),
    TypeCase("char", "CHAR(3)", "'abc'", pl.String, "abc"),
    TypeCase("varchar", "VARCHAR(20)", "'abç'", pl.String, "abç"),
    TypeCase(
        "varchar_latin1", "VARCHAR(20) CHARACTER SET latin1", "'abç'", pl.String, "abç"
    ),
    TypeCase(
        "varchar_bin", "VARCHAR(20) COLLATE utf8mb4_bin", "'abc'", pl.String, "abc"
    ),
    TypeCase("text", "TEXT", "'abc'", pl.String, "abc"),
    TypeCase("longtext", "LONGTEXT", "'abc'", pl.String, "abc"),
    TypeCase("enum", "ENUM('a', 'b')", "'a'", pl.String, "a"),
    TypeCase("set", "SET('a', 'b')", "'b,a'", pl.String, "a,b"),
    TypeCase("binary", "BINARY(2)", "X'0102'", pl.Binary, b"\x01\x02"),
    TypeCase("varbinary", "VARBINARY(4)", "X'0102'", pl.Binary, b"\x01\x02"),
    TypeCase("blob", "BLOB", "X'0102'", pl.Binary, b"\x01\x02"),
    TypeCase("json", "JSON", "'{\"a\": [1, 2.5]}'", pl.String, '{"a": [1, 2.5]}'),
    TypeCase(
        "geometry",
        "GEOMETRY",
        "ST_GeomFromText('POINT(1 2)')",
        pl.Binary,
        bytes.fromhex("000000000101000000000000000000f03f0000000000000040"),
    ),
]

DECIMALS = decimal_cases(shapes((1, 5, 10, 18, 19, 38, 39, 65), 30)) + decimal_cases(
    shapes((1, 10, 38, 65), 30), unsigned=True
)


def _read(domain: MysqlDomain, tmp_path: Path, driver: str):
    url = make_url(domain.uri).set(drivername=f"mysql+{driver}")
    conn = MySQLSrcConn(
        uri=url.render_as_string(hide_password=False),
        credentials=sql_credentials(domain),
    )

    def read(query: str, size: int):
        src = MySQLSrc(queries=[query], src_cfg={"tabsdata.sql.chunk_size": size})
        [slot] = plugin.MySQLSrcPlugin().read_in(
            PluginSrcCtx(work_dir(tmp_path)), conn, src
        )
        return [table.frame.collect() for table in slot]

    return read


@pytest.mark.parametrize("driver", DRIVERS)
@pytest.mark.parametrize("chunk_size", CHUNK_SIZES)
@mysql_domain("mysql")
def test_src_columns_are_typed_from_the_description(
    domain_mysql: MysqlDomain, tmp_path: Path, chunk_size: int, driver: str, request
):
    domain = domain_mysql
    read = _read(domain, tmp_path, driver)
    check(store(domain)._engine, read, table_for(request), CASES, chunk_size)


@pytest.mark.parametrize("driver", DRIVERS)
@mysql_domain("mysql")
def test_src_decimals_are_typed_with_their_precision_and_scale(
    domain_mysql: MysqlDomain, tmp_path: Path, driver: str, request
):
    domain = domain_mysql
    read = _read(domain, tmp_path, driver)
    check(store(domain)._engine, read, table_for(request), DECIMALS, 100)


@pytest.mark.parametrize("driver", DRIVERS)
@mysql_domain("mysql")
def test_src_expressions_are_typed_as_the_database_types_them(
    domain_mysql: MysqlDomain, tmp_path: Path, driver: str
):
    read = _read(domain_mysql, tmp_path, driver)
    [frame] = read(
        "SELECT CAST(NULL AS DECIMAL(12, 3)) AS cast_null,"
        " CAST(1.5 AS DECIMAL(10, 2)) * 2 AS doubled,"
        " 1.50 AS literal, CAST(255 AS UNSIGNED) AS unsigned_cast,"
        " JSON_EXTRACT('{\"a\": [1]}', '$.a') AS extracted",
        100,
    )
    expected = {
        "cast_null": (pl.Decimal(12, 3), None),
        "doubled": (pl.Decimal(11, 2), Decimal("3.00")),
        "literal": (pl.Decimal(3, 2), Decimal("1.50")),
        "unsigned_cast": (pl.UInt64, 255),
        "extracted": (pl.String, "[1]"),
    }
    read_back = {name: (frame.schema[name], frame[name][0]) for name in expected}
    assert read_back == expected
