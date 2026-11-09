#
# Copyright 2026 Tabsdata Inc.
#

"""Postgres source columns typed from what psycopg2 describes; see
:mod:`testi_tabsdatak.conn.common.sql.described`."""

from __future__ import annotations

import datetime as dt
from decimal import Decimal
from pathlib import Path

import polars as pl
import pytest
from tabsdatax.itest.fixture.backends.postgres import PostgresDomain, postgres_domain
from tabsdatax.itest.fixture.functions.files import PluginSrcCtx
from testi_tabsdatak.conn.common.sql.described import (
    CHUNK_SIZES,
    TypeCase,
    check,
    table_for,
)
from testi_tabsdatak.conn.common.sql.plugin_io import work_dir
from testi_tabsdatak.conn.postgres.backend import src_conn, store

# noinspection PyProtectedMember
from tabsdatak.conn.postgres import PostgresSrc, _plugin as plugin

pytestmark = pytest.mark.postgres

CASES = [
    TypeCase("smallint", "SMALLINT", "7", pl.Int16, 7),
    TypeCase("integer", "INTEGER", "7", pl.Int32, 7),
    TypeCase("bigint", "BIGINT", str(2**40), pl.Int64, 2**40),
    TypeCase("numeric", "NUMERIC(10, 2)", "12.34", pl.Decimal(10, 2), Decimal("12.34")),
    TypeCase("real", "REAL", "1.5", pl.Float32, 1.5),
    TypeCase("double", "DOUBLE PRECISION", "1.5", pl.Float64, 1.5),
    TypeCase("boolean", "BOOLEAN", "TRUE", pl.Boolean, True),
    TypeCase("char", "CHAR(3)", "'abc'", pl.String, "abc"),
    TypeCase("varchar", "VARCHAR(20)", "'abc'", pl.String, "abc"),
    TypeCase("text", "TEXT", "'abc'", pl.String, "abc"),
    TypeCase("bytea", "BYTEA", "'\\x0102'::bytea", pl.Binary, b"\x01\x02"),
    TypeCase("date", "DATE", "DATE '2026-04-19'", pl.Date, dt.date(2026, 4, 19)),
    TypeCase("time", "TIME", "TIME '06:30:24'", pl.Time, dt.time(6, 30, 24)),
    TypeCase(
        "timestamp",
        "TIMESTAMP",
        "TIMESTAMP '2026-04-19 06:30:24'",
        pl.Datetime("us"),
        dt.datetime(2026, 4, 19, 6, 30, 24),
    ),
    TypeCase(
        "timestamptz",
        "TIMESTAMPTZ",
        "TIMESTAMPTZ '2026-04-19 06:30:24+00'",
        pl.Datetime("us", "UTC"),
        dt.datetime(2026, 4, 19, 6, 30, 24, tzinfo=dt.timezone.utc),
    ),
    TypeCase(
        "interval",
        "INTERVAL",
        "INTERVAL '1 hour'",
        pl.Duration("us"),
        dt.timedelta(hours=1),
    ),
]


@pytest.mark.parametrize("chunk_size", CHUNK_SIZES)
@postgres_domain("postgres")
def test_src_columns_are_typed_from_the_description(
    domain_postgres: PostgresDomain, tmp_path: Path, chunk_size: int, request
):
    domain = domain_postgres

    def read(query: str, size: int):
        src = PostgresSrc(queries=[query], src_cfg={"tabsdata.sql.chunk_size": size})
        [slot] = plugin.PostgresSrcPlugin().read_in(
            PluginSrcCtx(work_dir(tmp_path)), src_conn(domain), src
        )
        return [table.frame.collect() for table in slot]

    check(store(domain)._engine, read, table_for(request), CASES, chunk_size)
