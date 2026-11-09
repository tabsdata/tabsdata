#
# Copyright 2026 Tabsdata Inc.
#

"""MySQL destination-plugin integration tests using fake context"""

from __future__ import annotations

from pathlib import Path

import polars as pl
import pytest
from tabsdatax.itest.fixture.backends.fs import FsDomain, fs_domain
from tabsdatax.itest.fixture.backends.mysql import MysqlDomain, mysql_domain
from tabsdatax.itest.fixture.functions.files import PluginDestCtx
from testi_tabsdatak.conn.common.sql.employees_model import DEPARTMENTS, DEPT_MANAGER
from testi_tabsdatak.conn.common.sql.plugin_io import provisioned_parquet, work_dir
from testi_tabsdatak.conn.mysql.backend import dest_conn, store

# noinspection PyProtectedMember
from tabsdatak.conn.mysql import MySQLDest, _plugin as plugin

pytestmark = pytest.mark.mysql


@fs_domain("fs_employees_xs")
@mysql_domain("mysql")
def test_dest_plugin_write_out_appends_rows(
    domain_fs_employees_xs: FsDomain,
    domain_mysql: MysqlDomain,
    tmp_path: Path,
):
    db = store(domain_mysql)
    table = "td_dest_append"
    work = work_dir(tmp_path)
    parquet = provisioned_parquet(domain_fs_employees_xs, DEPARTMENTS)
    rows = pl.read_parquet(parquet).height
    conn = dest_conn(domain_mysql)
    try:
        plugin.MySQLDestPlugin().write_out(
            PluginDestCtx(work),
            conn,
            MySQLDest(tables=[table], if_table_exists="replace"),
            [pl.scan_parquet(parquet)],
        )
        assert db.read_table(table).height == rows

        plugin.MySQLDestPlugin().write_out(
            PluginDestCtx(work),
            conn,
            MySQLDest(tables=[table], if_table_exists="append"),
            [pl.scan_parquet(parquet)],
        )
        assert db.read_table(table).height == 2 * rows
    finally:
        db.drop_table(table)


@fs_domain("fs_employees_xs")
@mysql_domain("mysql")
def test_dest_plugin_write_out_replace_keeps_the_table(
    domain_fs_employees_xs: FsDomain,
    domain_mysql: MysqlDomain,
    tmp_path: Path,
):
    # A replace keeps the table and only ever grows its schema: a different
    # entity is refused under strict, leaving the first load in place, and
    # under update it adds its columns and leaves only its own rows.
    db = store(domain_mysql)
    table = "td_dest_replace"
    work = work_dir(tmp_path)
    first = provisioned_parquet(domain_fs_employees_xs, DEPARTMENTS)
    second = provisioned_parquet(domain_fs_employees_xs, DEPT_MANAGER)
    expected = pl.read_parquet(second)
    conn = dest_conn(domain_mysql)
    try:
        plugin.MySQLDestPlugin().write_out(
            PluginDestCtx(work),
            conn,
            MySQLDest(tables=[table], if_table_exists="replace"),
            [pl.scan_parquet(first)],
        )
        assert db.read_table(table).height == pl.read_parquet(first).height

        with pytest.raises(Exception, match="SQL_COMMON_10"):
            plugin.MySQLDestPlugin().write_out(
                PluginDestCtx(work),
                conn,
                MySQLDest(tables=[table], if_table_exists="replace"),
                [pl.scan_parquet(second)],
            )
        kept = db.read_table(table)
        assert kept.height == pl.read_parquet(first).height
        assert set(kept.columns) == set(pl.read_parquet(first).columns)
        plugin.MySQLDestPlugin().write_out(
            PluginDestCtx(work),
            conn,
            MySQLDest(
                tables=[table], if_table_exists="replace", schema_evolution="update"
            ),
            [pl.scan_parquet(second)],
        )
        got = db.read_table(table)
        assert got.height == expected.height
        assert set(got.columns) == set(expected.columns) | set(
            pl.read_parquet(first).columns
        )
    finally:
        db.drop_table(table)
