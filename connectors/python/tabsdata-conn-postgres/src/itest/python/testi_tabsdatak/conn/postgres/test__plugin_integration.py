#
# Copyright 2026 Tabsdata Inc.
#

"""Postgres connector integration tests: drive ``PostgresSrcPlugin.read_in`` /
``PostgresDestPlugin.write_out`` directly against a tdx-provisioned ``postgres``
domain with a fabricated plugin context -- unlike ``test__plugin_e2e.py``, which
goes through a running tabsdata instance. Round-trips write to uniquely-named
scratch tables in the domain database (dropped on teardown), so they never touch
the provisioned model tables.

The fabricated plugin contexts (``PluginSrcCtx`` / ``PluginDestCtx``) are shared
with the file connectors; the ``sql`` helper (``conftest.py``) reads tables back
to verify writes. Requires a postgres docker container and the ``postgres``
domain provisioned (``tdx-domain create --sys postgres --domain postgres``).
"""

from __future__ import annotations

import uuid
from pathlib import Path

import polars as pl
import pyarrow.parquet as pq
import pytest
from tabsdatax.itest.fixture.backends.postgres import PostgresDomain, postgres_domain
from tabsdatax.itest.fixture.functions.files import PluginDestCtx, PluginSrcCtx

from tabsdatak.api import Secret
from tabsdatak.conn.common.types import UserPassword

# noinspection PyProtectedMember
from tabsdatak.conn.postgres import (
    PostgresDest,
    PostgresDestConn,
    PostgresSrc,
    PostgresSrcConn,
    _plugin as plugin,
)
from tabsdatak.spi import TableFileInput

pytestmark = pytest.mark.postgres


def _conn_uri(domain: PostgresDomain) -> str:
    return f"postgresql+psycopg2://{domain.host}:{domain.port}/{domain.db}"


def _credentials(domain: PostgresDomain) -> UserPassword:
    return UserPassword(user=Secret(domain.user), password=Secret(domain.password))


def _src_conn(domain: PostgresDomain) -> PostgresSrcConn:
    return PostgresSrcConn(uri=_conn_uri(domain), credentials=_credentials(domain))


def _dest_conn(domain: PostgresDomain) -> PostgresDestConn:
    return PostgresDestConn(uri=_conn_uri(domain), credentials=_credentials(domain))


def _read_parquet(tfi: TableFileInput) -> pl.DataFrame:
    return pl.from_arrow(pq.read_table(tfi.file))


def _scratch_table() -> str:
    return f"td_it_{uuid.uuid4().hex[:8]}"


def _write_parquet(path: Path, rows: list[dict]) -> Path:
    pl.DataFrame(rows).write_parquet(path, use_pyarrow=True)
    return path


@postgres_domain("postgres")
def test_dest_then_src_round_trip(domain_postgres: PostgresDomain, sql, tmp_path: Path):
    db = sql(domain_postgres)
    table = _scratch_table()
    work = tmp_path / "work"
    work.mkdir()
    rows = [{"id": 1, "name": "alice"}, {"id": 2, "name": "bob"}]
    parquet = _write_parquet(tmp_path / "in.parquet", rows)
    try:
        plugin.PostgresDestPlugin().write_out(
            PluginDestCtx(work),
            _dest_conn(domain_postgres),
            PostgresDest(tables=[table], if_table_exists="replace"),
            [parquet],
        )
        src = PostgresSrc(queries=[f'SELECT id, name FROM "{table}" ORDER BY id'])
        out = plugin.PostgresSrcPlugin().read_in(
            PluginSrcCtx(work), _src_conn(domain_postgres), src
        )
        assert len(out) == 1
        got = pl.concat([_read_parquet(p) for p in out[0]])
        assert got["name"].to_list() == ["alice", "bob"]
        assert db.read_table(table).height == 2
    finally:
        db.drop_table(table)


@postgres_domain("postgres")
def test_dest_append_accumulates(domain_postgres: PostgresDomain, sql, tmp_path: Path):
    db = sql(domain_postgres)
    table = _scratch_table()
    work = tmp_path / "work"
    work.mkdir()
    first = _write_parquet(tmp_path / "a.parquet", [{"id": 1, "name": "alice"}])
    second = _write_parquet(tmp_path / "b.parquet", [{"id": 2, "name": "bob"}])
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
    table = _scratch_table()
    work = tmp_path / "work"
    work.mkdir()
    rows = [{"id": i, "name": f"n{i}"} for i in range(1, 6)]
    parquet = _write_parquet(tmp_path / "in.parquet", rows)
    try:
        plugin.PostgresDestPlugin().write_out(
            PluginDestCtx(work),
            _dest_conn(domain_postgres),
            PostgresDest(tables=[table], if_table_exists="replace"),
            [parquet],
        )
        src = PostgresSrc(
            queries=[
                f'SELECT id FROM "{table}" WHERE id >= :cut ORDER BY id',
                f'SELECT id FROM "{table}" WHERE id < :cut ORDER BY id',
            ],
            initial_values={"cut": 3},
        )
        out = plugin.PostgresSrcPlugin().read_in(
            PluginSrcCtx(work), _src_conn(domain_postgres), src
        )
        assert len(out) == 2
        ge = pl.concat([_read_parquet(p) for p in out[0]])["id"].to_list()
        lt = pl.concat([_read_parquet(p) for p in out[1]])["id"].to_list()
        assert ge == [3, 4, 5]
        assert lt == [1, 2]
    finally:
        db.drop_table(table)
