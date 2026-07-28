#
# Copyright 2026 Tabsdata Inc.
#

"""StarRocks destination-plugin integration tests using fake context"""

from __future__ import annotations

from pathlib import Path

import polars as pl
import pytest
from tabsdatax.itest.fixture.backends.azureblob import AzureblobDomain, azureblob_domain
from tabsdatax.itest.fixture.backends.fs import FsDomain, fs_domain
from tabsdatax.itest.fixture.backends.gcs import GCSDomain, gcs_domain
from tabsdatax.itest.fixture.backends.s3 import S3Domain, s3_domain
from tabsdatax.itest.fixture.backends.starrocks import StarrocksDomain, starrocks_domain
from tabsdatax.itest.fixture.functions.files import PluginDestCtx
from tabsdatax.itest.fixture.functions.harness import assert_frames_match
from testi_tabsdatak.conn.common.sql.employees_model import DEPARTMENTS, DEPT_MANAGER
from testi_tabsdatak.conn.common.sql.plugin_io import provisioned_parquet, work_dir
from testi_tabsdatak.conn.starrocks.backend import (
    STAGING_PROVIDERS,
    column_spec,
    dest_conn,
    staging_domain,
    store,
)

# noinspection PyProtectedMember
from tabsdatak.conn.starrocks import StarRocksDest, _plugin as plugin

pytestmark = pytest.mark.starrocks

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
@starrocks_domain("starrocks")
@s3_domain("s3")
@azureblob_domain("azureblob")
@gcs_domain("gcs")
def test_dest_plugin_write_out_appends_rows(
    provider: str,
    domain_fs_employees_xs: FsDomain,
    domain_starrocks: StarrocksDomain,
    domain_s3: S3Domain,
    domain_azureblob: AzureblobDomain,
    domain_gcs: GCSDomain,
    tmp_path: Path,
):
    db = store(domain_starrocks)
    table = f"td_dest_append_{provider}"
    work = work_dir(tmp_path)
    parquet = provisioned_parquet(domain_fs_employees_xs, DEPARTMENTS)
    expected = pl.read_parquet(parquet)
    staging = staging_domain(provider, domain_s3, domain_azureblob, domain_gcs)
    conn = dest_conn(domain_starrocks, staging)
    db.create_table(table, column_spec(expected.schema))
    try:
        plugin.StarRocksDestPlugin().write_out(
            PluginDestCtx(work),
            conn,
            StarRocksDest(tables=[table], if_table_exists="replace"),
            [parquet],
        )
        assert_frames_match(db.read_table(table), expected)

        plugin.StarRocksDestPlugin().write_out(
            PluginDestCtx(work),
            conn,
            StarRocksDest(tables=[table], if_table_exists="append"),
            [parquet],
        )
        assert db.read_table(table).height == 2 * expected.height
    finally:
        db.drop_table(table)


@pytest.mark.parametrize("provider", STAGING_PROVIDERS)
@fs_domain("fs_employees_xs")
@starrocks_domain("starrocks")
@s3_domain("s3")
@azureblob_domain("azureblob")
@gcs_domain("gcs")
def test_dest_plugin_write_out_replace_clears_existing(
    provider: str,
    domain_fs_employees_xs: FsDomain,
    domain_starrocks: StarrocksDomain,
    domain_s3: S3Domain,
    domain_azureblob: AzureblobDomain,
    domain_gcs: GCSDomain,
    tmp_path: Path,
):
    # `replace` truncates the existing table rather than recreating it -- the
    # connector issues no DDL at all -- so the second load must leave only its own
    # rows behind, against the schema the test created.
    db = store(domain_starrocks)
    table = f"td_dest_replace_{provider}"
    work = work_dir(tmp_path)
    parquet = provisioned_parquet(domain_fs_employees_xs, DEPARTMENTS)
    expected = pl.read_parquet(parquet)
    staging = staging_domain(provider, domain_s3, domain_azureblob, domain_gcs)
    conn = dest_conn(domain_starrocks, staging)
    db.create_table(table, column_spec(expected.schema))
    try:
        for _ in range(2):
            plugin.StarRocksDestPlugin().write_out(
                PluginDestCtx(work),
                conn,
                StarRocksDest(tables=[table], if_table_exists="replace"),
                [parquet],
            )
        assert_frames_match(db.read_table(table), expected)
    finally:
        db.drop_table(table)


@pytest.mark.parametrize("template, table", list(_spelling_params()))
@fs_domain("fs_employees_xs")
@starrocks_domain("starrocks")
@s3_domain("s3")
def test_dest_plugin_write_out_resolves_table_name_spelling(
    domain_fs_employees_xs: FsDomain,
    domain_starrocks: StarrocksDomain,
    domain_s3: S3Domain,
    template: str,
    table: str,
    tmp_path: Path,
):
    # The dialect parses the target name before any cloud call, so one provider
    # covers every spelling. The table is created under its bare name and then
    # appended to through the spelling under test, which passes only if both
    # resolve to the same physical table.
    db = store(domain_starrocks)
    work = work_dir(tmp_path)
    parquet = provisioned_parquet(domain_fs_employees_xs, DEPARTMENTS)
    expected = pl.read_parquet(parquet)
    conn = dest_conn(domain_starrocks, domain_s3)
    db.create_table(table, column_spec(expected.schema))
    try:
        plugin.StarRocksDestPlugin().write_out(
            PluginDestCtx(work),
            conn,
            StarRocksDest(tables=[table], if_table_exists="replace"),
            [parquet],
        )
        plugin.StarRocksDestPlugin().write_out(
            PluginDestCtx(work),
            conn,
            StarRocksDest(
                tables=[template.format(db=domain_starrocks.database, table=table)],
                if_table_exists="append",
            ),
            [parquet],
        )
        assert db.read_table(table).height == 2 * expected.height
    finally:
        db.drop_table(table)


@fs_domain("fs_employees_xs")
@starrocks_domain("starrocks")
@s3_domain("s3")
def test_dest_plugin_write_out_loads_every_table(
    domain_fs_employees_xs: FsDomain,
    domain_starrocks: StarrocksDomain,
    domain_s3: S3Domain,
    tmp_path: Path,
):
    # Tables and slots pair up positionally, each loaded on its own: the tables
    # differ in shape, so a crossed pair would not even load.
    db = store(domain_starrocks)
    work = work_dir(tmp_path)
    parquets = [
        provisioned_parquet(domain_fs_employees_xs, entity)
        for entity in (DEPARTMENTS, DEPT_MANAGER)
    ]
    expected = [pl.read_parquet(parquet) for parquet in parquets]
    tables = ["td_dest_multi_departments", "td_dest_multi_dept_manager"]
    conn = dest_conn(domain_starrocks, domain_s3)
    for table, frame in zip(tables, expected):
        db.create_table(table, column_spec(frame.schema))
    try:
        plugin.StarRocksDestPlugin().write_out(
            PluginDestCtx(work),
            conn,
            StarRocksDest(tables=tables, if_table_exists="replace"),
            parquets,
        )
        for table, frame in zip(tables, expected):
            assert_frames_match(db.read_table(table), frame)
    finally:
        for table in tables:
            db.drop_table(table)
