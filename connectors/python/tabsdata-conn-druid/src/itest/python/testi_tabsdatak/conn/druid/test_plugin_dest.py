#
# Copyright 2026 Tabsdata Inc.
#

"""Druid destination-plugin integration tests using fake context"""

from __future__ import annotations

from pathlib import Path

import polars as pl
import pytest
from tabsdatax.itest.fixture.backends.azureblob import AzureblobDomain, azureblob_domain
from tabsdatax.itest.fixture.backends.druid import DruidDomain, druid_domain
from tabsdatax.itest.fixture.backends.fs import FsDomain, fs_domain
from tabsdatax.itest.fixture.backends.s3 import S3Domain, s3_domain
from tabsdatax.itest.fixture.functions.files import PluginDestCtx
from testi_tabsdatak.conn.common.sql.employees_model import (
    DEPARTMENTS,
    DEPT_MANAGER,
    EMPLOYEES,
)
from testi_tabsdatak.conn.common.sql.plugin_io import provisioned_parquet, work_dir
from testi_tabsdatak.conn.druid.backend import (
    STAGING_PROVIDERS,
    dest_conn,
    staging_domain,
    store,
)

# noinspection PyProtectedMember
from tabsdatak.conn.druid import DruidDest, _plugin as plugin

# Grouped with the rest of the package: the shared cluster ingests a couple of
# tasks at a time, so these cannot run beside each other. See test_explorer.py.
pytestmark = [pytest.mark.druid, pytest.mark.xdist_group("druid_cluster_seq")]


@fs_domain("fs_employees_xs")
@druid_domain("druid")
@pytest.mark.parametrize("provider", STAGING_PROVIDERS)
@s3_domain("s3")
@azureblob_domain("azureblob")
def test_dest_plugin_write_out_appends_rows(
    domain_fs_employees_xs: FsDomain,
    domain_druid: DruidDomain,
    domain_s3: S3Domain,
    domain_azureblob: AzureblobDomain,
    provider: str,
    tmp_path: Path,
):
    db = store(domain_druid)
    datasource = "td_dest_append"
    work = work_dir(tmp_path)
    parquet = provisioned_parquet(domain_fs_employees_xs, DEPARTMENTS)
    rows = pl.read_parquet(parquet).height
    conn = dest_conn(
        domain_druid, staging_domain(provider, domain_s3, domain_azureblob)
    )
    try:
        plugin.DruidDestPlugin().write_out(
            PluginDestCtx(work),
            conn,
            DruidDest(tables=[datasource], if_table_exists="replace"),
            [parquet],
        )
        assert db.wait_count(datasource, rows) == rows

        plugin.DruidDestPlugin().write_out(
            PluginDestCtx(work),
            conn,
            DruidDest(tables=[datasource], if_table_exists="append"),
            [parquet],
        )
        assert db.wait_count(datasource, 2 * rows) == 2 * rows
    finally:
        db.drop_table(datasource)


@fs_domain("fs_employees_xs")
@druid_domain("druid")
@pytest.mark.parametrize("provider", STAGING_PROVIDERS)
@s3_domain("s3")
@azureblob_domain("azureblob")
def test_dest_plugin_write_out_replace_clears_existing(
    domain_fs_employees_xs: FsDomain,
    domain_druid: DruidDomain,
    domain_s3: S3Domain,
    domain_azureblob: AzureblobDomain,
    provider: str,
    tmp_path: Path,
):
    # Replacing with a different entity: the second write must leave only its own
    # rows and columns behind, so neither the first load's rows nor its schema
    # survive.
    db = store(domain_druid)
    datasource = "td_dest_replace"
    work = work_dir(tmp_path)
    first = provisioned_parquet(domain_fs_employees_xs, DEPARTMENTS)
    second = provisioned_parquet(domain_fs_employees_xs, DEPT_MANAGER)
    replaced = pl.read_parquet(first).height
    expected = pl.read_parquet(second)
    conn = dest_conn(
        domain_druid, staging_domain(provider, domain_s3, domain_azureblob)
    )
    try:
        plugin.DruidDestPlugin().write_out(
            PluginDestCtx(work),
            conn,
            DruidDest(tables=[datasource], if_table_exists="replace"),
            [first],
        )
        assert db.wait_count(datasource, replaced) == replaced

        plugin.DruidDestPlugin().write_out(
            PluginDestCtx(work),
            conn,
            DruidDest(tables=[datasource], if_table_exists="replace"),
            [second],
        )
        assert db.wait_count(datasource, expected.height) == expected.height
        assert db.dimensions(datasource) == set(expected.columns)
    finally:
        db.drop_table(datasource)


@fs_domain("fs_employees_xs")
@druid_domain("druid")
@pytest.mark.parametrize("provider", STAGING_PROVIDERS)
@s3_domain("s3")
@azureblob_domain("azureblob")
def test_dest_plugin_write_out_writes_one_datasource_per_slot(
    domain_fs_employees_xs: FsDomain,
    domain_druid: DruidDomain,
    domain_s3: S3Domain,
    domain_azureblob: AzureblobDomain,
    provider: str,
    tmp_path: Path,
):
    # One ingestion task per slot, each fed only its own staged parquet -- so the
    # two datasources must end up with their own entity's rows, not the union.
    db = store(domain_druid)
    first_datasource, second_datasource = "td_dest_slot_a", "td_dest_slot_b"
    work = work_dir(tmp_path)
    first = provisioned_parquet(domain_fs_employees_xs, DEPARTMENTS)
    second = provisioned_parquet(domain_fs_employees_xs, DEPT_MANAGER)
    first_rows = pl.read_parquet(first).height
    second_rows = pl.read_parquet(second).height
    try:
        plugin.DruidDestPlugin().write_out(
            PluginDestCtx(work),
            dest_conn(
                domain_druid,
                staging_domain(provider, domain_s3, domain_azureblob),
            ),
            DruidDest(
                tables=[first_datasource, second_datasource],
                if_table_exists="replace",
            ),
            [first, second],
        )
        assert db.wait_count(first_datasource, first_rows) == first_rows
        assert db.wait_count(second_datasource, second_rows) == second_rows
    finally:
        db.drop_table(first_datasource)
        db.drop_table(second_datasource)


@fs_domain("fs_employees_xs")
@druid_domain("druid")
@pytest.mark.parametrize("provider", STAGING_PROVIDERS)
@s3_domain("s3")
@azureblob_domain("azureblob")
def test_dest_plugin_write_out_maps_time_column(
    domain_fs_employees_xs: FsDomain,
    domain_druid: DruidDomain,
    domain_s3: S3Domain,
    domain_azureblob: AzureblobDomain,
    provider: str,
    tmp_path: Path,
):
    # Druid needs a primary timestamp on every datasource, and a write without a
    # time_column gets a synthetic one holding a constant. Naming a column instead
    # promotes it to `__time`, so it leaves the ingested columns.
    db = store(domain_druid)
    datasource = "td_dest_time_column"
    work = work_dir(tmp_path)
    parquet = provisioned_parquet(domain_fs_employees_xs, EMPLOYEES)
    expected = pl.read_parquet(parquet)
    try:
        plugin.DruidDestPlugin().write_out(
            PluginDestCtx(work),
            dest_conn(
                domain_druid,
                staging_domain(provider, domain_s3, domain_azureblob),
            ),
            DruidDest(
                tables=[datasource],
                if_table_exists="replace",
                time_column="hire_date",
                segment_granularity="YEAR",
            ),
            [parquet],
        )
        assert db.wait_count(datasource, expected.height) == expected.height
        assert db.dimensions(datasource) == set(expected.columns) - {"hire_date"}
    finally:
        db.drop_table(datasource)
