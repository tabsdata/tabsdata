#
# Copyright 2026 Tabsdata Inc.
#

"""Postgres connector integration tests: drive ``PostgresSrcPlugin.read_in`` /
``PostgresDestPlugin.write_out`` directly against a tdx-provisioned ``postgres``
domain with a fabricated plugin context -- unlike ``test__plugin_e2e.py``, which
goes through a running tabsdata instance. Round-trips write to uniquely-named
scratch tables in the domain database (dropped on teardown), so they never touch
the provisioned model tables.

The fabricated plugin contexts and the SQL helpers are shared under
:mod:`tabsdatax.itest.fixture.functions` (``files`` / ``sql``); only the postgres
``Conn`` construction is here. Requires a postgres docker container and the
``postgres`` domain provisioned (``tdx-domain create --sys postgres --domain
postgres``).
"""

from __future__ import annotations

from pathlib import Path

import polars as pl
import pytest
from tabsdatax.itest.fixture.backends.postgres import PostgresDomain, postgres_domain
from tabsdatax.itest.fixture.functions.files import PluginDestCtx, PluginSrcCtx
from tabsdatax.itest.fixture.functions.sql import (
    read_parquet_slot,
    scratch_table,
    sql_credentials,
    write_parquet,
)

# noinspection PyProtectedMember
from tabsdatak.conn.postgres import (
    PostgresDest,
    PostgresDestConn,
    PostgresSrc,
    PostgresSrcConn,
    _plugin as plugin,
)

pytestmark = pytest.mark.postgres


def _conn_uri(domain: PostgresDomain) -> str:
    return f"postgresql+psycopg2://{domain.host}:{domain.port}/{domain.db}"


def _src_conn(domain: PostgresDomain) -> PostgresSrcConn:
    return PostgresSrcConn(uri=_conn_uri(domain), credentials=sql_credentials(domain))


def _dest_conn(domain: PostgresDomain) -> PostgresDestConn:
    return PostgresDestConn(uri=_conn_uri(domain), credentials=sql_credentials(domain))


@postgres_domain("postgres")
def test_dest_then_src_round_trip(domain_postgres: PostgresDomain, sql, tmp_path: Path):
    db = sql(domain_postgres)
    table = scratch_table()
    work = tmp_path / "work"
    work.mkdir()
    parquet = write_parquet(
        tmp_path / "in.parquet", [{"id": 1, "name": "alice"}, {"id": 2, "name": "bob"}]
    )
    try:
        plugin.PostgresDestPlugin().write_out(
            PluginDestCtx(work),
            _dest_conn(domain_postgres),
            PostgresDest(tables=[table], if_table_exists="replace"),
            [parquet],
        )
        src = PostgresSrc(
            queries=[f"SELECT id, name FROM {db.quote(table)} ORDER BY id"]
        )
        out = plugin.PostgresSrcPlugin().read_in(
            PluginSrcCtx(work), _src_conn(domain_postgres), src
        )
        assert len(out) == 1
        got = pl.concat([read_parquet_slot(p) for p in out[0]])
        assert got["name"].to_list() == ["alice", "bob"]
        assert db.read_table(table).height == 2
    finally:
        db.drop_table(table)


@postgres_domain("postgres")
def test_dest_append_accumulates(domain_postgres: PostgresDomain, sql, tmp_path: Path):
    db = sql(domain_postgres)
    table = scratch_table()
    work = tmp_path / "work"
    work.mkdir()
    first = write_parquet(tmp_path / "a.parquet", [{"id": 1, "name": "alice"}])
    second = write_parquet(tmp_path / "b.parquet", [{"id": 2, "name": "bob"}])
    dest_conn = _dest_conn(domain_postgres)
    try:
        plugin.PostgresDestPlugin().write_out(
            PluginDestCtx(work),
            dest_conn,
            PostgresDest(tables=[table], if_table_exists="replace"),
            [first],
        )
        plugin.PostgresDestPlugin().write_out(
            PluginDestCtx(work),
            dest_conn,
            PostgresDest(tables=[table], if_table_exists="append"),
            [second],
        )
        assert db.read_table(table).height == 2
    finally:
        db.drop_table(table)


@postgres_domain("postgres")
def test_src_multiple_queries_with_initial_values(
    domain_postgres: PostgresDomain, sql, tmp_path: Path
):
    db = sql(domain_postgres)
    table = scratch_table()
    work = tmp_path / "work"
    work.mkdir()
    parquet = write_parquet(
        tmp_path / "in.parquet", [{"id": i, "name": f"n{i}"} for i in range(1, 6)]
    )
    try:
        plugin.PostgresDestPlugin().write_out(
            PluginDestCtx(work),
            _dest_conn(domain_postgres),
            PostgresDest(tables=[table], if_table_exists="replace"),
            [parquet],
        )
        q = db.quote(table)
        src = PostgresSrc(
            queries=[
                f"SELECT id FROM {q} WHERE id >= :cut ORDER BY id",
                f"SELECT id FROM {q} WHERE id < :cut ORDER BY id",
            ],
            initial_values={"cut": 3},
        )
        out = plugin.PostgresSrcPlugin().read_in(
            PluginSrcCtx(work), _src_conn(domain_postgres), src
        )
        assert len(out) == 2
        ge = pl.concat([read_parquet_slot(p) for p in out[0]])["id"].to_list()
        lt = pl.concat([read_parquet_slot(p) for p in out[1]])["id"].to_list()
        assert ge == [3, 4, 5]
        assert lt == [1, 2]
    finally:
        db.drop_table(table)
