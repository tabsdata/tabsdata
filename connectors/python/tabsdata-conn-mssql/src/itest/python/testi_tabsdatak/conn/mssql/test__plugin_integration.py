#
# Copyright 2026 Tabsdata Inc.
#

"""MSSQL connector integration tests: drive ``MSSQLSrcPlugin.read_in`` /
``MSSQLDestPlugin.write_out`` directly against a tdx-provisioned ``mssqlserver``
domain with a fabricated plugin context -- unlike ``test__plugin_e2e.py``, which
goes through a running tabsdata instance. Round-trips write to uniquely-named
scratch tables in the domain database (dropped on teardown), so they never touch
the provisioned model tables.

The fabricated plugin contexts (``PluginSrcCtx`` / ``PluginDestCtx``) are shared
with the file connectors; the ``sql`` helper (``conftest.py``) reads tables back
to verify writes. Requires a SQL Server docker container and the ``mssql`` domain
provisioned (``tdx-domain create --sys mssql --domain mssql``).
"""

from __future__ import annotations

import urllib.parse
import uuid
from pathlib import Path

import polars as pl
import pyarrow.parquet as pq
import pytest
from tabsdatax.itest.fixture.backends.mssqlserver import (
    MssqlserverDomain,
    mssqlserver_domain,
)
from tabsdatax.itest.fixture.functions.files import PluginDestCtx, PluginSrcCtx

from tabsdatak.api import Secret
from tabsdatak.conn.common.types import UserPassword

# noinspection PyProtectedMember
from tabsdatak.conn.mssql import (
    MSSQLDest,
    MSSQLDestConn,
    MSSQLSrc,
    MSSQLSrcConn,
    _plugin as plugin,
)
from tabsdatak.spi import TableFileInput

pytestmark = pytest.mark.mssql

_ODBC_DRIVER = "ODBC Driver 18 for SQL Server"


def _conn_uri(domain: MssqlserverDomain) -> str:
    driver = urllib.parse.quote_plus(_ODBC_DRIVER)
    return (
        f"mssql+pyodbc://{domain.host}:{domain.port}/{domain.db}"
        f"?driver={driver}&TrustServerCertificate=yes"
    )


def _credentials(domain: MssqlserverDomain) -> UserPassword:
    return UserPassword(user=Secret(domain.user), password=Secret(domain.password))


def _src_conn(domain: MssqlserverDomain) -> MSSQLSrcConn:
    return MSSQLSrcConn(uri=_conn_uri(domain), credentials=_credentials(domain))


def _dest_conn(domain: MssqlserverDomain) -> MSSQLDestConn:
    return MSSQLDestConn(uri=_conn_uri(domain), credentials=_credentials(domain))


def _read_parquet(tfi: TableFileInput) -> pl.DataFrame:
    return pl.from_arrow(pq.read_table(tfi.file))


def _scratch_table() -> str:
    return f"td_it_{uuid.uuid4().hex[:8]}"


def _write_parquet(path: Path, rows: list[dict]) -> Path:
    pl.DataFrame(rows).write_parquet(path, use_pyarrow=True)
    return path


@mssqlserver_domain("mssql")
def test_dest_then_src_round_trip(domain_mssql: MssqlserverDomain, sql, tmp_path: Path):
    db = sql(domain_mssql)
    table = _scratch_table()
    work = tmp_path / "work"
    work.mkdir()
    rows = [{"id": 1, "name": "alice"}, {"id": 2, "name": "bob"}]
    parquet = _write_parquet(tmp_path / "in.parquet", rows)
    try:
        plugin.MSSQLDestPlugin().write_out(
            PluginDestCtx(work),
            _dest_conn(domain_mssql),
            MSSQLDest(tables=[table], if_table_exists="replace"),
            [parquet],
        )
        src = MSSQLSrc(queries=[f"SELECT id, name FROM [{table}] ORDER BY id"])
        out = plugin.MSSQLSrcPlugin().read_in(
            PluginSrcCtx(work), _src_conn(domain_mssql), src
        )
        assert len(out) == 1
        got = pl.concat([_read_parquet(p) for p in out[0]])
        assert got["name"].to_list() == ["alice", "bob"]
        assert db.read_table(table).height == 2
    finally:
        db.drop_table(table)


@mssqlserver_domain("mssql")
def test_dest_append_accumulates(domain_mssql: MssqlserverDomain, sql, tmp_path: Path):
    db = sql(domain_mssql)
    table = _scratch_table()
    work = tmp_path / "work"
    work.mkdir()
    first = _write_parquet(tmp_path / "a.parquet", [{"id": 1, "name": "alice"}])
    second = _write_parquet(tmp_path / "b.parquet", [{"id": 2, "name": "bob"}])
    dest_conn = _dest_conn(domain_mssql)
    try:
        plugin.MSSQLDestPlugin().write_out(
            PluginDestCtx(work),
            dest_conn,
            MSSQLDest(tables=[table], if_table_exists="replace"),
            [first],
        )
        plugin.MSSQLDestPlugin().write_out(
            PluginDestCtx(work),
            dest_conn,
            MSSQLDest(tables=[table], if_table_exists="append"),
            [second],
        )
        assert db.read_table(table).height == 2
    finally:
        db.drop_table(table)


@mssqlserver_domain("mssql")
def test_src_multiple_queries_with_initial_values(
    domain_mssql: MssqlserverDomain, sql, tmp_path: Path
):
    db = sql(domain_mssql)
    table = _scratch_table()
    work = tmp_path / "work"
    work.mkdir()
    rows = [{"id": i, "name": f"n{i}"} for i in range(1, 6)]
    parquet = _write_parquet(tmp_path / "in.parquet", rows)
    try:
        plugin.MSSQLDestPlugin().write_out(
            PluginDestCtx(work),
            _dest_conn(domain_mssql),
            MSSQLDest(tables=[table], if_table_exists="replace"),
            [parquet],
        )
        src = MSSQLSrc(
            queries=[
                f"SELECT id FROM [{table}] WHERE id >= :cut ORDER BY id",
                f"SELECT id FROM [{table}] WHERE id < :cut ORDER BY id",
            ],
            initial_values={"cut": 3},
        )
        out = plugin.MSSQLSrcPlugin().read_in(
            PluginSrcCtx(work), _src_conn(domain_mssql), src
        )
        assert len(out) == 2
        ge = pl.concat([_read_parquet(p) for p in out[0]])["id"].to_list()
        lt = pl.concat([_read_parquet(p) for p in out[1]])["id"].to_list()
        assert ge == [3, 4, 5]
        assert lt == [1, 2]
    finally:
        db.drop_table(table)
