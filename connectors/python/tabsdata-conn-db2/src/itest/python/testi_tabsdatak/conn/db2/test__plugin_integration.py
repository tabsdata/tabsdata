#
# Copyright 2026 Tabsdata Inc.
#

"""Db2 connector integration tests: drive ``Db2SrcPlugin.read_in`` /
``Db2DestPlugin.write_out`` directly against a tdx-provisioned ``db2`` domain with
a fabricated plugin context -- unlike ``test__plugin_e2e.py``, which goes through a
running tabsdata instance. Round-trips write to uniquely-named scratch tables in
the domain schema (dropped on teardown), so they never touch the provisioned model
tables.

The fabricated plugin contexts and the SQL helpers are shared under
:mod:`tabsdatax.itest.fixture.functions` (``files`` / ``sql``); only the db2
``Conn`` construction is here. Requires a Db2 docker container and the ``db2``
domain provisioned.
"""

from __future__ import annotations

from pathlib import Path

import polars as pl
import pytest
from tabsdatax.itest.fixture.backends.db2 import Db2Domain, db2_domain
from tabsdatax.itest.fixture.functions.files import PluginDestCtx, PluginSrcCtx
from tabsdatax.itest.fixture.functions.sql import (
    read_parquet_slot,
    scratch_table,
    sql_credentials,
    write_parquet,
)

# noinspection PyProtectedMember
from tabsdatak.conn.db2 import (
    Db2Dest,
    Db2DestConn,
    Db2Src,
    Db2SrcConn,
    _plugin as plugin,
)

pytestmark = pytest.mark.db2


def _src_conn(domain: Db2Domain) -> Db2SrcConn:
    return Db2SrcConn(uri=domain.uri, credentials=sql_credentials(domain))


def _dest_conn(domain: Db2Domain) -> Db2DestConn:
    return Db2DestConn(uri=domain.uri, credentials=sql_credentials(domain))


@db2_domain("db2")
def test_dest_then_src_round_trip(domain_db2: Db2Domain, sql, tmp_path: Path):
    db = sql(domain_db2)
    table = scratch_table()
    work = tmp_path / "work"
    work.mkdir()
    parquet = write_parquet(
        tmp_path / "in.parquet", [{"id": 1, "name": "alice"}, {"id": 2, "name": "bob"}]
    )
    try:
        plugin.Db2DestPlugin().write_out(
            PluginDestCtx(work),
            _dest_conn(domain_db2),
            Db2Dest(tables=[table], if_table_exists="replace"),
            [parquet],
        )
        src = Db2Src(queries=[f"SELECT id, name FROM {db.quote(table)} ORDER BY id"])
        out = plugin.Db2SrcPlugin().read_in(
            PluginSrcCtx(work), _src_conn(domain_db2), src
        )
        assert len(out) == 1
        got = pl.concat([read_parquet_slot(p) for p in out[0]])
        # Db2 folds unquoted identifiers to upper case, so the columns come back
        # as ID / NAME rather than the lower-case names in the query.
        assert got["NAME"].to_list() == ["alice", "bob"]
        assert db.read_table(table).height == 2
    finally:
        db.drop_table(table)


@pytest.mark.xfail(
    strict=True,
    reason=(
        "DB2 append is broken in the shared polars write engine: "
        "write_database(if_table_exists='append') cannot detect the existing table "
        "through ibm_db_sa (currentSchema/case introspection fails), so it re-issues "
        "CREATE and DB2 rejects it with SQL0601N. Connector-side bug tracked in "
        "TD-2799; drop this xfail once the write path handles DB2 existence checks."
    ),
)
@db2_domain("db2")
def test_dest_append_accumulates(domain_db2: Db2Domain, sql, tmp_path: Path):
    db = sql(domain_db2)
    table = scratch_table()
    work = tmp_path / "work"
    work.mkdir()
    first = write_parquet(tmp_path / "a.parquet", [{"id": 1, "name": "alice"}])
    second = write_parquet(tmp_path / "b.parquet", [{"id": 2, "name": "bob"}])
    dest_conn = _dest_conn(domain_db2)
    try:
        plugin.Db2DestPlugin().write_out(
            PluginDestCtx(work),
            dest_conn,
            Db2Dest(tables=[table], if_table_exists="replace"),
            [first],
        )
        plugin.Db2DestPlugin().write_out(
            PluginDestCtx(work),
            dest_conn,
            Db2Dest(tables=[table], if_table_exists="append"),
            [second],
        )
        assert db.read_table(table).height == 2
    finally:
        db.drop_table(table)


@db2_domain("db2")
def test_src_multiple_queries_with_initial_values(
    domain_db2: Db2Domain, sql, tmp_path: Path
):
    db = sql(domain_db2)
    table = scratch_table()
    work = tmp_path / "work"
    work.mkdir()
    parquet = write_parquet(
        tmp_path / "in.parquet", [{"id": i, "name": f"n{i}"} for i in range(1, 6)]
    )
    try:
        plugin.Db2DestPlugin().write_out(
            PluginDestCtx(work),
            _dest_conn(domain_db2),
            Db2Dest(tables=[table], if_table_exists="replace"),
            [parquet],
        )
        q = db.quote(table)
        src = Db2Src(
            queries=[
                f"SELECT id FROM {q} WHERE id >= :cut ORDER BY id",
                f"SELECT id FROM {q} WHERE id < :cut ORDER BY id",
            ],
            initial_values={"cut": 3},
        )
        out = plugin.Db2SrcPlugin().read_in(
            PluginSrcCtx(work), _src_conn(domain_db2), src
        )
        assert len(out) == 2
        ge = pl.concat([read_parquet_slot(p) for p in out[0]])["ID"].to_list()
        lt = pl.concat([read_parquet_slot(p) for p in out[1]])["ID"].to_list()
        assert ge == [3, 4, 5]
        assert lt == [1, 2]
    finally:
        db.drop_table(table)
