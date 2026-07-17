#
# Copyright 2026 Tabsdata Inc.
#

"""MSSQL connector integration tests: drive ``MSSQLSrcPlugin.read_in`` /
``MSSQLDestPlugin.write_out`` directly against a tdx-provisioned ``mssqlserver``
domain with a fabricated plugin context -- unlike ``test__plugin_e2e.py``, which
goes through a running tabsdata instance. Round-trips write to uniquely-named
scratch tables in the domain database (dropped on teardown), so they never touch
the provisioned model tables.

The fabricated plugin contexts and the SQL helpers are shared under
:mod:`tabsdatax.itest.fixture.functions` (``files`` / ``sql``); only the mssql
``Conn`` construction is here. Requires a SQL Server docker container and the
``mssql`` domain provisioned (``tdx-domain create --sys mssql --domain mssql``).
"""

from __future__ import annotations

import urllib.parse
from pathlib import Path

import polars as pl
import pytest
from tabsdatax.itest.fixture.backends.mssqlserver import (
    MssqlserverDomain,
    mssqlserver_domain,
)
from tabsdatax.itest.fixture.functions.files import PluginDestCtx, PluginSrcCtx
from tabsdatax.itest.fixture.functions.sql import (
    read_parquet_slot,
    scratch_table,
    sql_credentials,
    write_parquet,
)

# noinspection PyProtectedMember
from tabsdatak.conn.mssql import (
    MSSQLDest,
    MSSQLDestConn,
    MSSQLSrc,
    MSSQLSrcConn,
    _plugin as plugin,
)

pytestmark = pytest.mark.mssql

_ODBC_DRIVER = "ODBC Driver 18 for SQL Server"


def _conn_uri(domain: MssqlserverDomain) -> str:
    driver = urllib.parse.quote_plus(_ODBC_DRIVER)
    return (
        f"mssql+pyodbc://{domain.host}:{domain.port}/{domain.db}"
        f"?driver={driver}&TrustServerCertificate=yes"
    )


def _src_conn(domain: MssqlserverDomain) -> MSSQLSrcConn:
    return MSSQLSrcConn(uri=_conn_uri(domain), credentials=sql_credentials(domain))


def _dest_conn(domain: MssqlserverDomain) -> MSSQLDestConn:
    return MSSQLDestConn(uri=_conn_uri(domain), credentials=sql_credentials(domain))


@mssqlserver_domain("mssqlserver")
def test_dest_then_src_round_trip(
    domain_mssqlserver: MssqlserverDomain, sql, tmp_path: Path
):
    db = sql(domain_mssqlserver)
    table = scratch_table()
    work = tmp_path / "work"
    work.mkdir()
    parquet = write_parquet(
        tmp_path / "in.parquet", [{"id": 1, "name": "alice"}, {"id": 2, "name": "bob"}]
    )
    try:
        plugin.MSSQLDestPlugin().write_out(
            PluginDestCtx(work),
            _dest_conn(domain_mssqlserver),
            MSSQLDest(tables=[table], if_table_exists="replace"),
            [parquet],
        )
        src = MSSQLSrc(queries=[f"SELECT id, name FROM {db.quote(table)} ORDER BY id"])
        out = plugin.MSSQLSrcPlugin().read_in(
            PluginSrcCtx(work), _src_conn(domain_mssqlserver), src
        )
        assert len(out) == 1
        got = pl.concat([read_parquet_slot(p) for p in out[0]])
        assert got["name"].to_list() == ["alice", "bob"]
        assert db.read_table(table).height == 2
    finally:
        db.drop_table(table)


@mssqlserver_domain("mssqlserver")
def test_dest_append_accumulates(
    domain_mssqlserver: MssqlserverDomain, sql, tmp_path: Path
):
    db = sql(domain_mssqlserver)
    table = scratch_table()
    work = tmp_path / "work"
    work.mkdir()
    first = write_parquet(tmp_path / "a.parquet", [{"id": 1, "name": "alice"}])
    second = write_parquet(tmp_path / "b.parquet", [{"id": 2, "name": "bob"}])
    dest_conn = _dest_conn(domain_mssqlserver)
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


@mssqlserver_domain("mssqlserver")
def test_src_multiple_queries_with_initial_values(
    domain_mssqlserver: MssqlserverDomain, sql, tmp_path: Path
):
    db = sql(domain_mssqlserver)
    table = scratch_table()
    work = tmp_path / "work"
    work.mkdir()
    parquet = write_parquet(
        tmp_path / "in.parquet", [{"id": i, "name": f"n{i}"} for i in range(1, 6)]
    )
    try:
        plugin.MSSQLDestPlugin().write_out(
            PluginDestCtx(work),
            _dest_conn(domain_mssqlserver),
            MSSQLDest(tables=[table], if_table_exists="replace"),
            [parquet],
        )
        q = db.quote(table)
        src = MSSQLSrc(
            queries=[
                f"SELECT id FROM {q} WHERE id >= :cut ORDER BY id",
                f"SELECT id FROM {q} WHERE id < :cut ORDER BY id",
            ],
            initial_values={"cut": 3},
        )
        out = plugin.MSSQLSrcPlugin().read_in(
            PluginSrcCtx(work), _src_conn(domain_mssqlserver), src
        )
        assert len(out) == 2
        ge = pl.concat([read_parquet_slot(p) for p in out[0]])["id"].to_list()
        lt = pl.concat([read_parquet_slot(p) for p in out[1]])["id"].to_list()
        assert ge == [3, 4, 5]
        assert lt == [1, 2]
    finally:
        db.drop_table(table)
