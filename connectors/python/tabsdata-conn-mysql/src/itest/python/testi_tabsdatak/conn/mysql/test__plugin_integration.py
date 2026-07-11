#
# Copyright 2026 Tabsdata Inc.
#

"""MySQL connector integration tests: drive ``MySQLSrcPlugin.read_in`` /
``MySQLDestPlugin.write_out`` directly against a tdx-provisioned ``mysql`` domain
with a fabricated plugin context -- unlike ``test__plugin_e2e.py``, which goes
through a running tabsdata instance. Round-trips write to uniquely-named scratch
tables in the domain database (dropped on teardown), so they never touch the
provisioned model tables.

The fabricated plugin contexts and the SQL helpers are shared under
:mod:`tabsdatax.itest.fixture.functions` (``files`` / ``sql``); only the mysql
``Conn`` construction is here. Requires a mysql docker container and the
``mysql`` domain provisioned (``tdx-domain create --sys mysql --domain mysql``).
"""

from __future__ import annotations

from pathlib import Path

import polars as pl
import pytest
from tabsdatax.itest.fixture.backends.mysql import MysqlDomain, mysql_domain
from tabsdatax.itest.fixture.functions.files import PluginDestCtx, PluginSrcCtx
from tabsdatax.itest.fixture.functions.sql import (
    read_parquet_slot,
    scratch_table,
    sql_credentials,
    write_parquet,
)

# noinspection PyProtectedMember
from tabsdatak.conn.mysql import (
    MySQLDest,
    MySQLDestConn,
    MySQLSrc,
    MySQLSrcConn,
    _plugin as plugin,
)

pytestmark = pytest.mark.mysql


def _conn_uri(domain: MysqlDomain) -> str:
    return f"mysql+pymysql://{domain.host}:{domain.port}/{domain.db}"


def _src_conn(domain: MysqlDomain) -> MySQLSrcConn:
    return MySQLSrcConn(uri=_conn_uri(domain), credentials=sql_credentials(domain))


def _dest_conn(domain: MysqlDomain) -> MySQLDestConn:
    return MySQLDestConn(uri=_conn_uri(domain), credentials=sql_credentials(domain))


@mysql_domain("mysql")
def test_dest_then_src_round_trip(domain_mysql: MysqlDomain, sql, tmp_path: Path):
    db = sql(domain_mysql)
    table = scratch_table()
    work = tmp_path / "work"
    work.mkdir()
    parquet = write_parquet(
        tmp_path / "in.parquet", [{"id": 1, "name": "alice"}, {"id": 2, "name": "bob"}]
    )
    try:
        plugin.MySQLDestPlugin().write_out(
            PluginDestCtx(work),
            _dest_conn(domain_mysql),
            MySQLDest(tables=[table], if_table_exists="replace"),
            [parquet],
        )
        src = MySQLSrc(queries=[f"SELECT id, name FROM {db.quote(table)} ORDER BY id"])
        out = plugin.MySQLSrcPlugin().read_in(
            PluginSrcCtx(work), _src_conn(domain_mysql), src
        )
        assert len(out) == 1
        got = pl.concat([read_parquet_slot(p) for p in out[0]])
        assert got["name"].to_list() == ["alice", "bob"]
        assert db.read_table(table).height == 2
    finally:
        db.drop_table(table)


@mysql_domain("mysql")
def test_dest_append_accumulates(domain_mysql: MysqlDomain, sql, tmp_path: Path):
    db = sql(domain_mysql)
    table = scratch_table()
    work = tmp_path / "work"
    work.mkdir()
    first = write_parquet(tmp_path / "a.parquet", [{"id": 1, "name": "alice"}])
    second = write_parquet(tmp_path / "b.parquet", [{"id": 2, "name": "bob"}])
    dest_conn = _dest_conn(domain_mysql)
    try:
        plugin.MySQLDestPlugin().write_out(
            PluginDestCtx(work),
            dest_conn,
            MySQLDest(tables=[table], if_table_exists="replace"),
            [first],
        )
        plugin.MySQLDestPlugin().write_out(
            PluginDestCtx(work),
            dest_conn,
            MySQLDest(tables=[table], if_table_exists="append"),
            [second],
        )
        assert db.read_table(table).height == 2
    finally:
        db.drop_table(table)


@mysql_domain("mysql")
def test_src_multiple_queries_with_initial_values(
    domain_mysql: MysqlDomain, sql, tmp_path: Path
):
    db = sql(domain_mysql)
    table = scratch_table()
    work = tmp_path / "work"
    work.mkdir()
    parquet = write_parquet(
        tmp_path / "in.parquet", [{"id": i, "name": f"n{i}"} for i in range(1, 6)]
    )
    try:
        plugin.MySQLDestPlugin().write_out(
            PluginDestCtx(work),
            _dest_conn(domain_mysql),
            MySQLDest(tables=[table], if_table_exists="replace"),
            [parquet],
        )
        q = db.quote(table)
        src = MySQLSrc(
            queries=[
                f"SELECT id FROM {q} WHERE id >= :cut ORDER BY id",
                f"SELECT id FROM {q} WHERE id < :cut ORDER BY id",
            ],
            initial_values={"cut": 3},
        )
        out = plugin.MySQLSrcPlugin().read_in(
            PluginSrcCtx(work), _src_conn(domain_mysql), src
        )
        assert len(out) == 2
        ge = pl.concat([read_parquet_slot(p) for p in out[0]])["id"].to_list()
        lt = pl.concat([read_parquet_slot(p) for p in out[1]])["id"].to_list()
        assert ge == [3, 4, 5]
        assert lt == [1, 2]
    finally:
        db.drop_table(table)
