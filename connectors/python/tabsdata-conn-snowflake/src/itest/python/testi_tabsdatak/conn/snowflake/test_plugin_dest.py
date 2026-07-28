#
# Copyright 2026 Tabsdata Inc.
#

"""Snowflake destination-plugin integration tests using fake context"""

from __future__ import annotations

from pathlib import Path

import polars as pl
import pytest
from tabsdatax.itest.fixture.backends.fs import FsDomain, fs_domain
from tabsdatax.itest.fixture.backends.snowflake import SnowflakeDomain, snowflake_domain
from tabsdatax.itest.fixture.functions.files import PluginDestCtx
from testi_tabsdatak.conn.common.sql.employees_model import DEPARTMENTS
from testi_tabsdatak.conn.common.sql.plugin_io import provisioned_parquet, work_dir
from testi_tabsdatak.conn.snowflake.backend import dest_conn, store

# noinspection PyProtectedMember
from tabsdatak.conn.snowflake import SnowflakeDest, _plugin as plugin

pytestmark = pytest.mark.snowflake

# The connector double-quotes every part of a target name, so a name only lands
# on the objects tdx provisions when it is already spelled the way Snowflake
# stores an unquoted identifier: upper case. Hence the upper-case table names
# here and the upper-cased database / schema in the multi-part spellings.
_SPELLINGS = {
    "bare-table": "{table}",
    "schema.table": "{schema}.{table}",
    "database.schema.table": "{database}.{schema}.{table}",
    "quoted-parts": '"{database}"."{schema}"."{table}"',
}


def _spelling_params():
    for spelling, template in _SPELLINGS.items():
        table = "TD_DEST_FQN_" + spelling.replace(".", "_").replace("-", "_").upper()
        yield pytest.param(template, table, id=spelling)


@fs_domain("fs_employees_xs")
@snowflake_domain("snowflake")
def test_dest_plugin_write_out_appends_rows(
    domain_fs_employees_xs: FsDomain,
    domain_snowflake: SnowflakeDomain,
    tmp_path: Path,
):
    db = store(domain_snowflake)
    table = "TD_DEST_APPEND"
    work = work_dir(tmp_path)
    parquet = provisioned_parquet(domain_fs_employees_xs, DEPARTMENTS)
    rows = pl.read_parquet(parquet).height
    conn = dest_conn(domain_snowflake)
    try:
        plugin.SnowflakeDestPlugin().write_out(
            PluginDestCtx(work),
            conn,
            SnowflakeDest(tables=[table], if_table_exists="replace"),
            [parquet],
        )
        assert db.count(table) == rows

        plugin.SnowflakeDestPlugin().write_out(
            PluginDestCtx(work),
            conn,
            SnowflakeDest(tables=[table], if_table_exists="append"),
            [parquet],
        )
        assert db.count(table) == 2 * rows
    finally:
        db.drop_table(table)


@fs_domain("fs_employees_xs")
@snowflake_domain("snowflake")
def test_dest_plugin_write_out_replace_clears_existing(
    domain_fs_employees_xs: FsDomain,
    domain_snowflake: SnowflakeDomain,
    tmp_path: Path,
):
    # Snowflake's replace is a TRUNCATE, so it clears the rows and keeps the
    # column set: the reload is the same entity and the assertion is on the row
    # count returning to a single load's worth.
    db = store(domain_snowflake)
    table = "TD_DEST_REPLACE"
    work = work_dir(tmp_path)
    parquet = provisioned_parquet(domain_fs_employees_xs, DEPARTMENTS)
    expected = pl.read_parquet(parquet)
    conn = dest_conn(domain_snowflake)
    try:
        for mode in ("replace", "append"):
            plugin.SnowflakeDestPlugin().write_out(
                PluginDestCtx(work),
                conn,
                SnowflakeDest(tables=[table], if_table_exists=mode),
                [parquet],
            )
        assert db.count(table) == 2 * expected.height

        plugin.SnowflakeDestPlugin().write_out(
            PluginDestCtx(work),
            conn,
            SnowflakeDest(tables=[table], if_table_exists="replace"),
            [parquet],
        )
        got = db.read_table(table)
        assert got.height == expected.height
        assert set(got.columns) == set(expected.columns)
    finally:
        db.drop_table(table)


@pytest.mark.parametrize("template, table", list(_spelling_params()))
@fs_domain("fs_employees_xs")
@snowflake_domain("snowflake")
def test_dest_plugin_write_out_resolves_table_name_spelling(
    domain_fs_employees_xs: FsDomain,
    domain_snowflake: SnowflakeDomain,
    template: str,
    table: str,
    tmp_path: Path,
):
    # The table is created under its bare name and then appended to through the
    # spelling under test, which passes only if both resolve to the same
    # physical table.
    db = store(domain_snowflake)
    work = work_dir(tmp_path)
    parquet = provisioned_parquet(domain_fs_employees_xs, DEPARTMENTS)
    rows = pl.read_parquet(parquet).height
    conn = dest_conn(domain_snowflake)
    spelled = template.format(
        database=domain_snowflake.database.upper(),
        schema=domain_snowflake.schema.upper(),
        table=table,
    )
    try:
        plugin.SnowflakeDestPlugin().write_out(
            PluginDestCtx(work),
            conn,
            SnowflakeDest(tables=[table], if_table_exists="replace"),
            [parquet],
        )
        plugin.SnowflakeDestPlugin().write_out(
            PluginDestCtx(work),
            conn,
            SnowflakeDest(tables=[spelled], if_table_exists="append"),
            [parquet],
        )
        assert db.count(table) == 2 * rows
    finally:
        db.drop_table(table)
