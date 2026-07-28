#
# Copyright 2026 Tabsdata Inc.
#

"""BigQuery destination-plugin integration tests using fake context"""

from __future__ import annotations

from pathlib import Path

import polars as pl
import pytest
from tabsdatax.itest.fixture.backends.bigquery import BigqueryDomain, bigquery_domain
from tabsdatax.itest.fixture.backends.fs import FsDomain, fs_domain
from tabsdatax.itest.fixture.functions.files import PluginDestCtx
from testi_tabsdatak.conn.bigquery.backend import dest_conn, store, table_fqn
from testi_tabsdatak.conn.common.sql.employees_model import DEPARTMENTS, DEPT_MANAGER
from testi_tabsdatak.conn.common.sql.plugin_io import provisioned_parquet, work_dir

# noinspection PyProtectedMember
from tabsdatak.conn.bigquery import BigQueryDest, _plugin as plugin

pytestmark = pytest.mark.bigquery

# Every accepted spelling of a target table name, each with its own table so a
# run leaves nothing unpredictable behind. The missing leading parts are
# completed from the connection's project / dataset.
_SPELLINGS = {
    "bare-table": "{table}",
    "dataset.table": "{dataset}.{table}",
    "project.dataset.table": "{project}.{dataset}.{table}",
}


def _spelling_params():
    for spelling, template in _SPELLINGS.items():
        table = "td_dest_fqn_" + spelling.replace(".", "_").replace("-", "_")
        yield pytest.param(template, table, id=spelling)


@fs_domain("fs_employees_xs")
@bigquery_domain("bigquery")
def test_dest_plugin_write_out_appends_rows(
    domain_fs_employees_xs: FsDomain,
    domain_bigquery: BigqueryDomain,
    tmp_path: Path,
):
    db = store(domain_bigquery)
    table = "td_dest_append"
    work = work_dir(tmp_path)
    parquet = provisioned_parquet(domain_fs_employees_xs, DEPARTMENTS)
    rows = pl.read_parquet(parquet).height
    conn = dest_conn(domain_bigquery)
    try:
        plugin._BigQueryDestPlugin().write_out(
            PluginDestCtx(work),
            conn,
            BigQueryDest(tables=[table_fqn(table)], if_table_exists="replace"),
            [parquet],
        )
        assert db.count(table) == rows

        plugin._BigQueryDestPlugin().write_out(
            PluginDestCtx(work),
            conn,
            BigQueryDest(tables=[table_fqn(table)], if_table_exists="append"),
            [parquet],
        )
        assert db.count(table) == 2 * rows
    finally:
        db.drop_table(table)


@fs_domain("fs_employees_xs")
@bigquery_domain("bigquery")
def test_dest_plugin_write_out_replace_clears_existing(
    domain_fs_employees_xs: FsDomain,
    domain_bigquery: BigqueryDomain,
    tmp_path: Path,
):
    # Replacing with a different entity: the load job truncates the table and
    # replaces its schema wholesale, so neither the first load's rows nor its
    # columns survive.
    db = store(domain_bigquery)
    table = "td_dest_replace"
    work = work_dir(tmp_path)
    first = provisioned_parquet(domain_fs_employees_xs, DEPARTMENTS)
    second = provisioned_parquet(domain_fs_employees_xs, DEPT_MANAGER)
    expected = pl.read_parquet(second)
    conn = dest_conn(domain_bigquery)
    try:
        plugin._BigQueryDestPlugin().write_out(
            PluginDestCtx(work),
            conn,
            BigQueryDest(tables=[table_fqn(table)], if_table_exists="replace"),
            [first],
        )
        assert db.count(table) == pl.read_parquet(first).height

        plugin._BigQueryDestPlugin().write_out(
            PluginDestCtx(work),
            conn,
            BigQueryDest(tables=[table_fqn(table)], if_table_exists="replace"),
            [second],
        )
        got = db.read_table(table)
        assert got.height == expected.height
        assert set(got.columns) == set(expected.columns)
    finally:
        db.drop_table(table)


@pytest.mark.parametrize("template, table", list(_spelling_params()))
@fs_domain("fs_employees_xs")
@bigquery_domain("bigquery")
def test_dest_plugin_write_out_resolves_table_name_spelling(
    domain_fs_employees_xs: FsDomain,
    domain_bigquery: BigqueryDomain,
    template: str,
    table: str,
    tmp_path: Path,
):
    # The table is created under its bare name and then appended to through the
    # spelling under test, which passes only if both resolve to the same
    # physical table.
    db = store(domain_bigquery)
    work = work_dir(tmp_path)
    parquet = provisioned_parquet(domain_fs_employees_xs, DEPARTMENTS)
    rows = pl.read_parquet(parquet).height
    conn = dest_conn(domain_bigquery)
    spelled = template.format(
        project=domain_bigquery.project,
        dataset=domain_bigquery.dataset,
        table=table,
    )
    try:
        plugin._BigQueryDestPlugin().write_out(
            PluginDestCtx(work),
            conn,
            BigQueryDest(tables=[table_fqn(table)], if_table_exists="replace"),
            [parquet],
        )
        plugin._BigQueryDestPlugin().write_out(
            PluginDestCtx(work),
            conn,
            BigQueryDest(tables=[table_fqn(spelled)], if_table_exists="append"),
            [parquet],
        )
        assert db.count(table) == 2 * rows
    finally:
        db.drop_table(table)


@fs_domain("fs_employees_xs")
@bigquery_domain("bigquery")
def test_dest_plugin_write_out_loads_one_table_per_slot(
    domain_fs_employees_xs: FsDomain,
    domain_bigquery: BigqueryDomain,
    tmp_path: Path,
):
    db = store(domain_bigquery)
    tables = ["td_dest_multi_a", "td_dest_multi_b"]
    work = work_dir(tmp_path)
    parquets = [
        provisioned_parquet(domain_fs_employees_xs, DEPARTMENTS),
        provisioned_parquet(domain_fs_employees_xs, DEPT_MANAGER),
    ]
    conn = dest_conn(domain_bigquery)
    try:
        plugin._BigQueryDestPlugin().write_out(
            PluginDestCtx(work),
            conn,
            BigQueryDest(
                tables=[table_fqn(t) for t in tables], if_table_exists="replace"
            ),
            parquets,
        )
        for table, parquet in zip(tables, parquets):
            expected = pl.read_parquet(parquet)
            got = db.read_table(table)
            assert got.height == expected.height
            assert set(got.columns) == set(expected.columns)
    finally:
        for table in tables:
            db.drop_table(table)
