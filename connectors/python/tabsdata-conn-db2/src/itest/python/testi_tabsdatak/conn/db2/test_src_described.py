#
# Copyright 2026 Tabsdata Inc.
#

"""Db2 source columns typed from what ibm_db_sa describes; see
:mod:`testi_tabsdatak.conn.common.sql.described`. ibm_db_sa describes a type
group, not a type; its precision tells a SMALLINT, an INTEGER and a BOOLEAN
apart, and a REAL from a DOUBLE."""

from __future__ import annotations

import datetime as dt
from decimal import Decimal
from pathlib import Path

import polars as pl
import pytest
from tabsdatax.itest.fixture.backends.db2 import Db2Domain, db2_domain
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
from testi_tabsdatak.conn.db2.backend import src_conn, store

# noinspection PyProtectedMember
from tabsdatak.conn.db2 import Db2Src, _plugin as plugin

pytestmark = pytest.mark.db2

CASES = [
    TypeCase("smallint", "SMALLINT", "-7", pl.Int16, -7),
    TypeCase("integer", "INTEGER", "-7", pl.Int32, -7),
    TypeCase("bigint", "BIGINT", str(2**40), pl.Int64, 2**40),
    TypeCase("decimal", "DECIMAL(10, 2)", "12.34", pl.Decimal(10, 2), Decimal("12.34")),
    TypeCase("double", "DOUBLE", "1.5", pl.Float64, 1.5),
    TypeCase("real", "REAL", "1.5", pl.Float32, 1.5),
    TypeCase("boolean", "BOOLEAN", "TRUE", pl.Boolean, True),
    TypeCase("char", "CHAR(3)", "'abc'", pl.String, "abc"),
    TypeCase("varchar", "VARCHAR(20)", "'abc'", pl.String, "abc"),
    TypeCase("clob", "CLOB(1K)", "CLOB('abc')", pl.String, "abc"),
    TypeCase("blob", "BLOB(1K)", "BLOB(X'0102')", pl.Binary, b"\x01\x02"),
    TypeCase("date", "DATE", "DATE('2026-04-19')", pl.Date, dt.date(2026, 4, 19)),
    TypeCase("time", "TIME", "TIME('06:30:24')", pl.Time, dt.time(6, 30, 24)),
    TypeCase(
        "timestamp",
        "TIMESTAMP",
        "TIMESTAMP('2026-04-19 06:30:24')",
        pl.Datetime("us"),
        dt.datetime(2026, 4, 19, 6, 30, 24),
    ),
    TypeCase("float", "FLOAT", "1.5", pl.Float64, 1.5),
    TypeCase(
        "decfloat", "DECFLOAT(16)", "-1234567890.123456", pl.Float64, -1234567890.123456
    ),
    TypeCase(
        "numeric", "NUMERIC(7, 3)", "-9999.999", pl.Decimal(7, 3), Decimal("-9999.999")
    ),
    TypeCase("long_varchar", "VARCHAR(32672)", "'abc'", pl.String, "abc"),
    TypeCase("dbclob", "DBCLOB(1K)", "DBCLOB('abc')", pl.String, "abc"),
    TypeCase(
        "timestamp_12",
        "TIMESTAMP(12)",
        "TIMESTAMP('2026-04-19 06:30:24.123456789012')",
        pl.Datetime("us"),
        dt.datetime(2026, 4, 19, 6, 30, 24, 123456),
    ),
]


DECIMALS = decimal_cases(shapes((1, 5, 10, 18, 19, 31), 31))


def _read(domain: Db2Domain, tmp_path: Path):
    def read(query: str, size: int):
        src = Db2Src(queries=[query], src_cfg={"tabsdata.sql.chunk_size": size})
        [slot] = plugin.Db2SrcPlugin().read_in(
            PluginSrcCtx(work_dir(tmp_path)), src_conn(domain), src
        )
        return [table.frame.collect() for table in slot]

    return read


@pytest.mark.parametrize("chunk_size", CHUNK_SIZES)
@db2_domain("db2")
def test_src_columns_are_typed_from_the_description(
    domain_db2: Db2Domain, tmp_path: Path, chunk_size: int, request
):
    domain = domain_db2
    read = _read(domain, tmp_path)
    check(store(domain)._engine, read, table_for(request), CASES, chunk_size)


@db2_domain("db2")
def test_src_decimals_are_typed_with_their_precision_and_scale(
    domain_db2: Db2Domain, tmp_path: Path
):
    domain = domain_db2
    read = _read(domain, tmp_path)
    check(store(domain)._engine, read, "td_src_described_decimals", DECIMALS, 100)
