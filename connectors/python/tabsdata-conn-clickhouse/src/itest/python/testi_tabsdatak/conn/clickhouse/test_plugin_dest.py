#
# Copyright 2026 Tabsdata Inc.
#

"""ClickHouse destination-plugin integration tests using fake context"""

from __future__ import annotations

from pathlib import Path

import polars as pl
import pytest
from tabsdatax.itest.fixture.backends.azureblob import AzureblobDomain, azureblob_domain
from tabsdatax.itest.fixture.backends.clickhouse import (
    ClickhouseDomain,
    clickhouse_domain,
)
from tabsdatax.itest.fixture.backends.fs import FsDomain, fs_domain
from tabsdatax.itest.fixture.backends.gcs import GcsDomain, gcs_domain
from tabsdatax.itest.fixture.backends.s3 import S3Domain, s3_domain
from tabsdatax.itest.fixture.functions.files import PluginDestCtx
from testi_tabsdatak.conn.clickhouse.backend import (
    STAGING_PROVIDERS,
    dest_conn,
    staging_domain,
    store,
)
from testi_tabsdatak.conn.common.sql.employees_model import DEPARTMENTS, EMPLOYEES
from testi_tabsdatak.conn.common.sql.plugin_io import provisioned_parquet, work_dir

# noinspection PyProtectedMember
from tabsdatak.conn.clickhouse import ClickHouseDest, _plugin as plugin

pytestmark = pytest.mark.clickhouse

# Every accepted spelling of a target table name, each with its own table so a
# run leaves nothing unpredictable behind.
_SPELLINGS = {
    "bare-table": "{table}",
    "db.table": "{db}.{table}",
    "quoted-db.quoted-table": "`{db}`.`{table}`",
    "quoted-db.table": "`{db}`.{table}",
    "db.quoted-table": "{db}.`{table}`",
}


def _spelling_params():
    for spelling, template in _SPELLINGS.items():
        table = "td_dest_fqn_" + spelling.replace(".", "_").replace("-", "_")
        yield pytest.param(template, table, id=spelling)


@pytest.mark.parametrize("provider", STAGING_PROVIDERS)
@fs_domain("fs_employees_xs")
@clickhouse_domain("clickhouse")
@s3_domain("s3")
@azureblob_domain("azureblob")
@gcs_domain("gcs")
def test_dest_plugin_write_out_appends_rows(
    provider: str,
    domain_fs_employees_xs: FsDomain,
    domain_clickhouse: ClickhouseDomain,
    domain_s3: S3Domain,
    domain_azureblob: AzureblobDomain,
    domain_gcs: GcsDomain,
    tmp_path: Path,
):
    db = store(domain_clickhouse)
    table = f"td_dest_append_{provider}"
    work = work_dir(tmp_path)
    parquet = provisioned_parquet(domain_fs_employees_xs, DEPARTMENTS)
    rows = pl.read_parquet(parquet).height
    staging = staging_domain(provider, domain_s3, domain_azureblob, domain_gcs)
    conn = dest_conn(domain_clickhouse, staging)
    try:
        plugin._ClickHouseDestPlugin().write_out(
            PluginDestCtx(work),
            conn,
            ClickHouseDest(tables=[table], if_table_exists="replace"),
            [parquet],
        )
        assert db.count(table) == rows

        plugin._ClickHouseDestPlugin().write_out(
            PluginDestCtx(work),
            conn,
            ClickHouseDest(tables=[table], if_table_exists="append"),
            [parquet],
        )
        assert db.count(table) == 2 * rows
    finally:
        db.drop_table(table)


@pytest.mark.parametrize("provider", STAGING_PROVIDERS)
@fs_domain("fs_employees_xs")
@clickhouse_domain("clickhouse")
@s3_domain("s3")
@azureblob_domain("azureblob")
@gcs_domain("gcs")
def test_dest_plugin_write_out_replace_clears_existing(
    provider: str,
    domain_fs_employees_xs: FsDomain,
    domain_clickhouse: ClickhouseDomain,
    domain_s3: S3Domain,
    domain_azureblob: AzureblobDomain,
    domain_gcs: GcsDomain,
    tmp_path: Path,
):
    # Replacing with a different entity: the second write must leave only its own
    # rows and columns behind, so neither the first load's rows nor its schema
    # survive. `employees` is the second entity because the date-bearing entities
    # carry the model's 9999-01-01 sentinel, which overflows ClickHouse's Date32.
    db = store(domain_clickhouse)
    table = f"td_dest_replace_{provider}"
    work = work_dir(tmp_path)
    first = provisioned_parquet(domain_fs_employees_xs, DEPARTMENTS)
    second = provisioned_parquet(domain_fs_employees_xs, EMPLOYEES)
    expected = pl.read_parquet(second)
    staging = staging_domain(provider, domain_s3, domain_azureblob, domain_gcs)
    conn = dest_conn(domain_clickhouse, staging)
    try:
        plugin._ClickHouseDestPlugin().write_out(
            PluginDestCtx(work),
            conn,
            ClickHouseDest(tables=[table], if_table_exists="replace"),
            [first],
        )
        assert db.count(table) == pl.read_parquet(first).height

        plugin._ClickHouseDestPlugin().write_out(
            PluginDestCtx(work),
            conn,
            ClickHouseDest(tables=[table], if_table_exists="replace"),
            [second],
        )
        got = db.read_table(table)
        assert got.height == expected.height
        assert set(got.columns) == set(expected.columns)
    finally:
        db.drop_table(table)


@pytest.mark.parametrize("template, table", list(_spelling_params()))
@fs_domain("fs_employees_xs")
@clickhouse_domain("clickhouse")
@s3_domain("s3")
def test_dest_plugin_write_out_resolves_table_name_spelling(
    domain_fs_employees_xs: FsDomain,
    domain_clickhouse: ClickhouseDomain,
    domain_s3: S3Domain,
    template: str,
    table: str,
    tmp_path: Path,
):
    # The dialect parses the target name before any cloud call, so one provider
    # covers every spelling. The table is created under its bare name and then
    # appended to through the spelling under test, which passes only if both
    # resolve to the same physical table.
    db = store(domain_clickhouse)
    work = work_dir(tmp_path)
    parquet = provisioned_parquet(domain_fs_employees_xs, DEPARTMENTS)
    rows = pl.read_parquet(parquet).height
    conn = dest_conn(domain_clickhouse, domain_s3)
    try:
        plugin._ClickHouseDestPlugin().write_out(
            PluginDestCtx(work),
            conn,
            ClickHouseDest(tables=[table], if_table_exists="replace"),
            [parquet],
        )
        plugin._ClickHouseDestPlugin().write_out(
            PluginDestCtx(work),
            conn,
            ClickHouseDest(
                tables=[template.format(db=domain_clickhouse.database, table=table)],
                if_table_exists="append",
            ),
            [parquet],
        )
        assert db.count(table) == 2 * rows
    finally:
        db.drop_table(table)
