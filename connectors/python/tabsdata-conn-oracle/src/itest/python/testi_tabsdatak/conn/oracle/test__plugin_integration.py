#
# Copyright 2026 Tabsdata Inc.
#

"""Oracle connector integration tests: drive ``OracleSrcPlugin.read_in`` /
``OracleDestPlugin.write_out`` directly against a tdx-provisioned ``oracle``
domain with a fabricated plugin context -- unlike ``test__plugin_e2e.py``, which
goes through a running tabsdata instance. Round-trips write to uniquely-named
scratch tables in the domain schema (dropped on teardown), so they never touch
the provisioned model tables.

The fabricated plugin contexts and the SQL helpers are shared under
:mod:`tabsdatax.itest.fixture.functions` (``files`` / ``sql``); only the oracle
``Conn`` construction is here. Requires an Oracle docker container (+ Oracle
Instant Client) and the ``oracle`` domain provisioned.
"""

from __future__ import annotations

from pathlib import Path

import polars as pl
import pytest
from tabsdatax.itest.fixture.backends.oracle import OracleDomain, oracle_domain
from tabsdatax.itest.fixture.functions.files import PluginDestCtx, PluginSrcCtx
from tabsdatax.itest.fixture.functions.sql import (
    read_parquet_slot,
    scratch_table,
    sql_credentials,
    write_parquet,
)

# noinspection PyProtectedMember
from tabsdatak.conn.oracle import (
    OracleDest,
    OracleDestConn,
    OracleSrc,
    OracleSrcConn,
    _plugin as plugin,
)

pytestmark = pytest.mark.oracle


def _src_conn(domain: OracleDomain) -> OracleSrcConn:
    return OracleSrcConn(uri=domain.uri, credentials=sql_credentials(domain))


def _dest_conn(domain: OracleDomain) -> OracleDestConn:
    return OracleDestConn(uri=domain.uri, credentials=sql_credentials(domain))


@oracle_domain("oracle")
def test_dest_then_src_round_trip(domain_oracle: OracleDomain, sql, tmp_path: Path):
    db = sql(domain_oracle)
    table = scratch_table()
    work = tmp_path / "work"
    work.mkdir()
    parquet = write_parquet(
        tmp_path / "in.parquet", [{"id": 1, "name": "alice"}, {"id": 2, "name": "bob"}]
    )
    try:
        plugin.OracleDestPlugin().write_out(
            PluginDestCtx(work),
            _dest_conn(domain_oracle),
            OracleDest(tables=[table], if_table_exists="replace"),
            [parquet],
        )
        src = OracleSrc(queries=[f"SELECT id, name FROM {db.quote(table)} ORDER BY id"])
        out = plugin.OracleSrcPlugin().read_in(
            PluginSrcCtx(work), _src_conn(domain_oracle), src
        )
        assert len(out) == 1
        got = pl.concat([read_parquet_slot(p) for p in out[0]])
        # Oracle folds unquoted identifiers to upper case, so the columns come
        # back as ID / NAME rather than the lower-case names in the query.
        assert got["NAME"].to_list() == ["alice", "bob"]
        assert db.read_table(table).height == 2
    finally:
        db.drop_table(table)


@oracle_domain("oracle")
def test_dest_append_accumulates(domain_oracle: OracleDomain, sql, tmp_path: Path):
    db = sql(domain_oracle)
    table = scratch_table()
    work = tmp_path / "work"
    work.mkdir()
    first = write_parquet(tmp_path / "a.parquet", [{"id": 1, "name": "alice"}])
    second = write_parquet(tmp_path / "b.parquet", [{"id": 2, "name": "bob"}])
    dest_conn = _dest_conn(domain_oracle)
    try:
        plugin.OracleDestPlugin().write_out(
            PluginDestCtx(work),
            dest_conn,
            OracleDest(tables=[table], if_table_exists="replace"),
            [first],
        )
        plugin.OracleDestPlugin().write_out(
            PluginDestCtx(work),
            dest_conn,
            OracleDest(tables=[table], if_table_exists="append"),
            [second],
        )
        assert db.read_table(table).height == 2
    finally:
        db.drop_table(table)


@oracle_domain("oracle")
def test_src_multiple_queries_with_initial_values(
    domain_oracle: OracleDomain, sql, tmp_path: Path
):
    db = sql(domain_oracle)
    table = scratch_table()
    work = tmp_path / "work"
    work.mkdir()
    parquet = write_parquet(
        tmp_path / "in.parquet", [{"id": i, "name": f"n{i}"} for i in range(1, 6)]
    )
    try:
        plugin.OracleDestPlugin().write_out(
            PluginDestCtx(work),
            _dest_conn(domain_oracle),
            OracleDest(tables=[table], if_table_exists="replace"),
            [parquet],
        )
        q = db.quote(table)
        src = OracleSrc(
            queries=[
                f"SELECT id FROM {q} WHERE id >= :cut ORDER BY id",
                f"SELECT id FROM {q} WHERE id < :cut ORDER BY id",
            ],
            initial_values={"cut": 3},
        )
        out = plugin.OracleSrcPlugin().read_in(
            PluginSrcCtx(work), _src_conn(domain_oracle), src
        )
        assert len(out) == 2
        ge = pl.concat([read_parquet_slot(p) for p in out[0]])["ID"].to_list()
        lt = pl.concat([read_parquet_slot(p) for p in out[1]])["ID"].to_list()
        assert ge == [3, 4, 5]
        assert lt == [1, 2]
    finally:
        db.drop_table(table)
