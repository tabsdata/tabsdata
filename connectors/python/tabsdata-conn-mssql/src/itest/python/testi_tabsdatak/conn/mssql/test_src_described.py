#
# Copyright 2026 Tabsdata Inc.
#

"""SQL Server source columns typed from what pyodbc describes; see
:mod:`testi_tabsdatak.conn.common.sql.described`. A TINYINT is read as
SMALLINT: pyodbc gives only its size, and TINYINT runs 0 to 255 here but -128
to 127 elsewhere over ODBC."""

from __future__ import annotations

import datetime as dt
from decimal import Decimal
from pathlib import Path

import polars as pl
import pytest
from tabsdatax.itest.fixture.backends.mssqlserver import (
    MssqlserverDomain,
    mssqlserver_domain,
)
from tabsdatax.itest.fixture.functions.files import PluginSrcCtx
from testi_tabsdatak.conn.common.sql.described import (
    CHUNK_SIZES,
    TypeCase,
    check,
    decimal_cases,
    shapes,
    table_for,
)
from testi_tabsdatak.conn.common.sql.plugin_io import work_dir
from testi_tabsdatak.conn.mssql.backend import src_conn, store

# noinspection PyProtectedMember
from tabsdatak.conn.mssql import MSSQLSrc, _plugin as plugin

pytestmark = pytest.mark.mssql

CASES = [
    TypeCase("tinyint", "TINYINT", "200", pl.Int16, 200),
    TypeCase("smallint", "SMALLINT", "-7", pl.Int16, -7),
    TypeCase("int", "INT", "-7", pl.Int32, -7),
    TypeCase("bigint", "BIGINT", str(2**40), pl.Int64, 2**40),
    TypeCase("decimal", "DECIMAL(10, 2)", "12.34", pl.Decimal(10, 2), Decimal("12.34")),
    TypeCase("real", "REAL", "1.5", pl.Float32, 1.5),
    TypeCase("float", "FLOAT", "1.5", pl.Float64, 1.5),
    TypeCase("bit", "BIT", "1", pl.Boolean, True),
    TypeCase("char", "CHAR(3)", "'abc'", pl.String, "abc"),
    TypeCase("varchar", "VARCHAR(20)", "'abc'", pl.String, "abc"),
    TypeCase("nvarchar", "NVARCHAR(20)", "N'abc'", pl.String, "abc"),
    TypeCase("varbinary", "VARBINARY(4)", "0x0102", pl.Binary, b"\x01\x02"),
    TypeCase("date", "DATE", "'2026-04-19'", pl.Date, dt.date(2026, 4, 19)),
    TypeCase("time", "TIME(0)", "'06:30:24'", pl.Time, dt.time(6, 30, 24)),
    TypeCase(
        "datetime2",
        "DATETIME2",
        "'2026-04-19 06:30:24'",
        pl.Datetime("us"),
        dt.datetime(2026, 4, 19, 6, 30, 24),
    ),
    TypeCase(
        "datetime",
        "DATETIME",
        "'2026-04-19 06:30:24'",
        pl.Datetime("us"),
        dt.datetime(2026, 4, 19, 6, 30, 24),
    ),
    TypeCase(
        "money",
        "MONEY",
        "-922337203685477.5808",
        pl.Decimal(19, 4),
        Decimal("-922337203685477.5808"),
    ),
    TypeCase(
        "smallmoney",
        "SMALLMONEY",
        "-214748.3648",
        pl.Decimal(10, 4),
        Decimal("-214748.3648"),
    ),
    TypeCase("float_24", "FLOAT(24)", "1.5", pl.Float32, 1.5),
    TypeCase(
        "time_7", "TIME(7)", "'06:30:24.1234567'", pl.Time, dt.time(6, 30, 24, 123456)
    ),
    TypeCase(
        "datetime2_7",
        "DATETIME2(7)",
        "'2026-04-19 06:30:24.1234567'",
        pl.Datetime("us"),
        dt.datetime(2026, 4, 19, 6, 30, 24, 123456),
    ),
    TypeCase(
        "datetimeoffset",
        "DATETIMEOFFSET",
        "'2026-04-19 06:30:24.1234567 +02:00'",
        pl.Datetime("us", "UTC"),
        dt.datetime(2026, 4, 19, 4, 30, 24, 123456, tzinfo=dt.timezone.utc),
    ),
    TypeCase(
        "uniqueidentifier",
        "UNIQUEIDENTIFIER",
        "'a0eebc99-9c0b-4ef8-bb6d-6bb9bd380a11'",
        pl.String,
        "A0EEBC99-9C0B-4EF8-BB6D-6BB9BD380A11",
    ),
    TypeCase("xml", "XML", "'<a>1</a>'", pl.String, "<a>1</a>"),
    TypeCase("nvarchar_max", "NVARCHAR(MAX)", "N'abç'", pl.String, "abç"),
    TypeCase("varbinary_max", "VARBINARY(MAX)", "0x0102", pl.Binary, b"\x01\x02"),
]


DECIMALS = decimal_cases(shapes((1, 5, 10, 18, 19, 28, 38), 38))


def _read(domain: MssqlserverDomain, tmp_path: Path):
    def read(query: str, size: int):
        src = MSSQLSrc(queries=[query], src_cfg={"tabsdata.sql.chunk_size": size})
        [slot] = plugin.MSSQLSrcPlugin().read_in(
            PluginSrcCtx(work_dir(tmp_path)), src_conn(domain), src
        )
        return [table.frame.collect() for table in slot]

    return read


@pytest.mark.parametrize("chunk_size", CHUNK_SIZES)
@mssqlserver_domain("mssqlserver")
def test_src_columns_are_typed_from_the_description(
    domain_mssqlserver: MssqlserverDomain, tmp_path: Path, chunk_size: int, request
):
    domain = domain_mssqlserver
    read = _read(domain, tmp_path)
    check(store(domain)._engine, read, table_for(request), CASES, chunk_size)


@mssqlserver_domain("mssqlserver")
def test_src_decimals_are_typed_with_their_precision_and_scale(
    domain_mssqlserver: MssqlserverDomain, tmp_path: Path
):
    domain = domain_mssqlserver
    read = _read(domain, tmp_path)
    check(store(domain)._engine, read, "td_src_described_decimals", DECIMALS, 100)
