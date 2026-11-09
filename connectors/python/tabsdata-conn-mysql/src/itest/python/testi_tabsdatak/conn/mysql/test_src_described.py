#
# Copyright 2026 Tabsdata Inc.
#

"""MySQL source columns typed from what mysql-connector describes; see
:mod:`testi_tabsdatak.conn.common.sql.described`. mysql-connector gives no
DECIMAL precision, so a DECIMAL is the one column typed from its values."""

from __future__ import annotations

import datetime as dt
from decimal import Decimal
from pathlib import Path

import polars as pl
import pytest
from tabsdatax.itest.fixture.backends.mysql import MysqlDomain, mysql_domain
from tabsdatax.itest.fixture.functions.files import PluginSrcCtx
from testi_tabsdatak.conn.common.sql.described import (
    CHUNK_SIZES,
    TypeCase,
    check,
    table_for,
)
from testi_tabsdatak.conn.common.sql.plugin_io import work_dir
from testi_tabsdatak.conn.mysql.backend import src_conn, store

# noinspection PyProtectedMember
from tabsdatak.conn.mysql import MySQLSrc, _plugin as plugin

pytestmark = pytest.mark.mysql

CASES = [
    TypeCase("tinyint", "TINYINT", "-7", pl.Int8, -7),
    TypeCase("tinyint_unsigned", "TINYINT UNSIGNED", "200", pl.UInt8, 200),
    TypeCase("smallint", "SMALLINT", "-7", pl.Int16, -7),
    TypeCase("smallint_unsigned", "SMALLINT UNSIGNED", "60000", pl.UInt16, 60000),
    TypeCase("mediumint", "MEDIUMINT", "-7", pl.Int32, -7),
    TypeCase("int", "INT", "-7", pl.Int32, -7),
    TypeCase("int_unsigned", "INT UNSIGNED", "4000000000", pl.UInt32, 4000000000),
    TypeCase("bigint", "BIGINT", str(-(2**40)), pl.Int64, -(2**40)),
    TypeCase(
        "bigint_unsigned",
        "BIGINT UNSIGNED",
        str(2**63 + 5),
        pl.UInt64,
        2**63 + 5,
    ),
    TypeCase("float", "FLOAT", "1.5", pl.Float32, 1.5),
    TypeCase("double", "DOUBLE", "1.5", pl.Float64, 1.5),
    TypeCase("decimal", "DECIMAL(10, 2)", "12.34", pl.Decimal, Decimal("12.34")),
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
        "timestamp",
        "TIMESTAMP NULL",
        "'2026-04-19 06:30:24'",
        pl.Datetime("us"),
        dt.datetime(2026, 4, 19, 6, 30, 24),
    ),
    TypeCase(
        "time",
        "TIME",
        "'06:30:24'",
        pl.Duration("us"),
        dt.timedelta(hours=6, minutes=30, seconds=24),
    ),
    TypeCase("char", "CHAR(3)", "'abc'", pl.String, "abc"),
    TypeCase("varchar", "VARCHAR(20)", "'abc'", pl.String, "abc"),
    TypeCase("text", "TEXT", "'abc'", pl.String, "abc"),
    TypeCase("enum", "ENUM('a', 'b')", "'a'", pl.String, "a"),
    TypeCase("binary", "BINARY(2)", "X'0102'", pl.Binary, b"\x01\x02"),
    TypeCase("varbinary", "VARBINARY(4)", "X'0102'", pl.Binary, b"\x01\x02"),
    TypeCase("blob", "BLOB", "X'0102'", pl.Binary, b"\x01\x02"),
]


@pytest.mark.parametrize("chunk_size", CHUNK_SIZES)
@mysql_domain("mysql")
def test_src_columns_are_typed_from_the_description(
    domain_mysql: MysqlDomain, tmp_path: Path, chunk_size: int, request
):
    domain = domain_mysql

    def read(query: str, size: int):
        src = MySQLSrc(queries=[query], src_cfg={"tabsdata.sql.chunk_size": size})
        [slot] = plugin.MySQLSrcPlugin().read_in(
            PluginSrcCtx(work_dir(tmp_path)), src_conn(domain), src
        )
        return [table.frame.collect() for table in slot]

    check(store(domain)._engine, read, table_for(request), CASES, chunk_size)
