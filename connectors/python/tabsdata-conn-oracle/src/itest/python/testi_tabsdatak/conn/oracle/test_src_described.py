#
# Copyright 2026 Tabsdata Inc.
#

"""Oracle source columns typed from what oracledb describes; see
:mod:`testi_tabsdatak.conn.common.sql.described`."""

from __future__ import annotations

import datetime as dt
from decimal import Decimal
from pathlib import Path

import polars as pl
import pytest
from tabsdatax.itest.fixture.backends.oracle import OracleDomain, oracle_domain
from tabsdatax.itest.fixture.functions.files import PluginSrcCtx
from testi_tabsdatak.conn.common.sql.described import (
    CHUNK_SIZES,
    TypeCase,
    check,
    table_for,
)
from testi_tabsdatak.conn.common.sql.plugin_io import work_dir
from testi_tabsdatak.conn.oracle.backend import src_conn, store

# noinspection PyProtectedMember
from tabsdatak.conn.oracle import OracleSrc, _plugin as plugin

pytestmark = pytest.mark.oracle

CASES = [
    TypeCase("number_10", "NUMBER(10)", "-7", pl.Int64, -7),
    TypeCase("number_18", "NUMBER(18)", str(2**40), pl.Int64, 2**40),
    TypeCase(
        "number_30", "NUMBER(30)", str(10**25), pl.Decimal(30, 0), Decimal(10**25)
    ),
    TypeCase(
        "number_10_2", "NUMBER(10, 2)", "12.34", pl.Decimal(10, 2), Decimal("12.34")
    ),
    TypeCase("float", "FLOAT", "1.5", pl.Float64, 1.5),
    TypeCase("binary_float", "BINARY_FLOAT", "1.5", pl.Float32, 1.5),
    TypeCase("binary_double", "BINARY_DOUBLE", "1.5", pl.Float64, 1.5),
    TypeCase("char", "CHAR(3)", "'abc'", pl.String, "abc"),
    TypeCase("varchar2", "VARCHAR2(20)", "'abc'", pl.String, "abc"),
    TypeCase("nvarchar2", "NVARCHAR2(20)", "N'abc'", pl.String, "abc"),
    TypeCase("clob", "CLOB", "TO_CLOB('abc')", pl.String, "abc"),
    TypeCase("raw", "RAW(4)", "HEXTORAW('0102')", pl.Binary, b"\x01\x02"),
    TypeCase("blob", "BLOB", "TO_BLOB(HEXTORAW('0102'))", pl.Binary, b"\x01\x02"),
    TypeCase(
        "date",
        "DATE",
        "DATE '2026-04-19'",
        pl.Datetime("us"),
        dt.datetime(2026, 4, 19),
    ),
    TypeCase(
        "timestamp",
        "TIMESTAMP",
        "TIMESTAMP '2026-04-19 06:30:24'",
        pl.Datetime("us"),
        dt.datetime(2026, 4, 19, 6, 30, 24),
    ),
    TypeCase(
        "interval",
        "INTERVAL DAY TO SECOND",
        "INTERVAL '0 01:00:00' DAY TO SECOND",
        pl.Duration("us"),
        dt.timedelta(hours=1),
    ),
]


@pytest.mark.parametrize("chunk_size", CHUNK_SIZES)
@oracle_domain("oracle")
def test_src_columns_are_typed_from_the_description(
    domain_oracle: OracleDomain, tmp_path: Path, chunk_size: int, request
):
    domain = domain_oracle

    def read(query: str, size: int):
        src = OracleSrc(queries=[query], src_cfg={"tabsdata.sql.chunk_size": size})
        [slot] = plugin.OracleSrcPlugin().read_in(
            PluginSrcCtx(work_dir(tmp_path)), src_conn(domain), src
        )
        return [table.frame.collect() for table in slot]

    check(store(domain)._engine, read, table_for(request), CASES, chunk_size)
