#
# Copyright 2026 Tabsdata Inc.
#

"""DB2 destination-plugin integration tests using fake context"""

from __future__ import annotations

from pathlib import Path

import polars as pl
import pytest
from tabsdatax.itest.fixture.backends.db2 import Db2Domain, db2_domain
from tabsdatax.itest.fixture.backends.fs import FsDomain, fs_domain
from tabsdatax.itest.fixture.functions.files import PluginDestCtx
from testi_tabsdatak.conn.common.sql.employees_model import DEPARTMENTS, DEPT_MANAGER
from testi_tabsdatak.conn.common.sql.plugin_io import provisioned_parquet, work_dir
from testi_tabsdatak.conn.db2.backend import dest_conn, store

# noinspection PyProtectedMember
from tabsdatak.conn.db2 import Db2Dest, _plugin as plugin

pytestmark = pytest.mark.db2


@fs_domain("fs_employees_xs")
@db2_domain("db2")
def test_dest_plugin_write_out_appends_rows(
    domain_fs_employees_xs: FsDomain,
    domain_db2: Db2Domain,
    tmp_path: Path,
):
    db = store(domain_db2)
    table = "td_dest_append"
    work = work_dir(tmp_path)
    parquet = provisioned_parquet(domain_fs_employees_xs, DEPARTMENTS)
    rows = pl.read_parquet(parquet).height
    conn = dest_conn(domain_db2)
    try:
        plugin.Db2DestPlugin().write_out(
            PluginDestCtx(work),
            conn,
            Db2Dest(tables=[table], if_table_exists="replace"),
            [pl.scan_parquet(parquet)],
        )
        assert db.read_table(table).height == rows

        plugin.Db2DestPlugin().write_out(
            PluginDestCtx(work),
            conn,
            Db2Dest(tables=[table], if_table_exists="append"),
            [pl.scan_parquet(parquet)],
        )
        assert db.read_table(table).height == 2 * rows
    finally:
        db.drop_table(table)


@fs_domain("fs_employees_xs")
@db2_domain("db2")
def test_dest_plugin_write_out_replace_clears_existing(
    domain_fs_employees_xs: FsDomain,
    domain_db2: Db2Domain,
    tmp_path: Path,
):
    # Replacing with a different entity: the second write must leave only its own
    # rows and columns behind, so neither the first load's rows nor its schema
    # survive.
    db = store(domain_db2)
    table = "td_dest_replace"
    work = work_dir(tmp_path)
    first = provisioned_parquet(domain_fs_employees_xs, DEPARTMENTS)
    second = provisioned_parquet(domain_fs_employees_xs, DEPT_MANAGER)
    expected = pl.read_parquet(second)
    conn = dest_conn(domain_db2)
    try:
        plugin.Db2DestPlugin().write_out(
            PluginDestCtx(work),
            conn,
            Db2Dest(tables=[table], if_table_exists="replace"),
            [pl.scan_parquet(first)],
        )
        assert db.read_table(table).height == pl.read_parquet(first).height

        plugin.Db2DestPlugin().write_out(
            PluginDestCtx(work),
            conn,
            Db2Dest(tables=[table], if_table_exists="replace"),
            [pl.scan_parquet(second)],
        )
        got = db.read_table(table)
        assert got.height == expected.height
        assert set(got.columns) == set(expected.columns)
    finally:
        db.drop_table(table)


@db2_domain("db2")
def test_dest_plugin_write_out_keeps_every_decimal_digit(
    domain_db2: Db2Domain,
    tmp_path: Path,
):
    # ibm_db_sa has SQLAlchemy turn a Decimal into a float on the way in; the
    # value has more digits than a float holds, so only an exact bind keeps it.
    from decimal import Decimal

    db = store(domain_db2)
    table = "td_dest_exact_decimal"
    value = Decimal("123456789012345678.25")
    frame = pl.LazyFrame({"d": [value]}, schema={"d": pl.Decimal(20, 2)})
    try:
        plugin.Db2DestPlugin().write_out(
            PluginDestCtx(work_dir(tmp_path)),
            dest_conn(domain_db2),
            Db2Dest(tables=[table], if_table_exists="replace"),
            [frame],
        )
        stored = db.read_table(table)
        assert Decimal(str(stored[stored.columns[0]][0])) == value
    finally:
        db.drop_table(table)
