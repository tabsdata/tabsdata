#
# Copyright 2026 Tabsdata Inc.
#

"""MSSQL destination-plugin integration tests using fake context"""

from __future__ import annotations

from pathlib import Path

import polars as pl
import pytest
from tabsdatax.itest.fixture.backends.fs import FsDomain, fs_domain
from tabsdatax.itest.fixture.backends.mssqlserver import (
    MssqlserverDomain,
    mssqlserver_domain,
)
from tabsdatax.itest.fixture.functions.files import PluginDestCtx
from testi_tabsdatak.conn.common.sql.employees_model import DEPARTMENTS, DEPT_MANAGER
from testi_tabsdatak.conn.common.sql.plugin_io import provisioned_parquet, work_dir
from testi_tabsdatak.conn.mssql.backend import dest_conn, store

# noinspection PyProtectedMember
from tabsdatak.conn.mssql import MSSQLDest, _plugin as plugin

pytestmark = pytest.mark.mssql


@fs_domain("fs_employees_xs")
@mssqlserver_domain("mssqlserver")
def test_dest_plugin_write_out_appends_rows(
    domain_fs_employees_xs: FsDomain,
    domain_mssqlserver: MssqlserverDomain,
    tmp_path: Path,
):
    db = store(domain_mssqlserver)
    table = "td_dest_append"
    work = work_dir(tmp_path)
    parquet = provisioned_parquet(domain_fs_employees_xs, DEPARTMENTS)
    rows = pl.read_parquet(parquet).height
    conn = dest_conn(domain_mssqlserver)
    try:
        plugin.MSSQLDestPlugin().write_out(
            PluginDestCtx(work),
            conn,
            MSSQLDest(tables=[table], if_table_exists="replace"),
            [parquet],
        )
        assert db.read_table(table).height == rows

        plugin.MSSQLDestPlugin().write_out(
            PluginDestCtx(work),
            conn,
            MSSQLDest(tables=[table], if_table_exists="append"),
            [parquet],
        )
        assert db.read_table(table).height == 2 * rows
    finally:
        db.drop_table(table)


@fs_domain("fs_employees_xs")
@mssqlserver_domain("mssqlserver")
def test_dest_plugin_write_out_replace_clears_existing(
    domain_fs_employees_xs: FsDomain,
    domain_mssqlserver: MssqlserverDomain,
    tmp_path: Path,
):
    # Replacing with a different entity: the second write must leave only its own
    # rows and columns behind, so neither the first load's rows nor its schema
    # survive.
    db = store(domain_mssqlserver)
    table = "td_dest_replace"
    work = work_dir(tmp_path)
    first = provisioned_parquet(domain_fs_employees_xs, DEPARTMENTS)
    second = provisioned_parquet(domain_fs_employees_xs, DEPT_MANAGER)
    expected = pl.read_parquet(second)
    conn = dest_conn(domain_mssqlserver)
    try:
        plugin.MSSQLDestPlugin().write_out(
            PluginDestCtx(work),
            conn,
            MSSQLDest(tables=[table], if_table_exists="replace"),
            [first],
        )
        assert db.read_table(table).height == pl.read_parquet(first).height

        plugin.MSSQLDestPlugin().write_out(
            PluginDestCtx(work),
            conn,
            MSSQLDest(tables=[table], if_table_exists="replace"),
            [second],
        )
        got = db.read_table(table)
        assert got.height == expected.height
        assert set(got.columns) == set(expected.columns)
    finally:
        db.drop_table(table)
