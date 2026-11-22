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
from tabsdatax.itest.fixture.backends.gcs import GCSDomain, gcs_domain
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
    domain_gcs: GCSDomain,
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
            [pl.scan_parquet(parquet)],
        )
        assert db.count(table) == rows

        plugin._ClickHouseDestPlugin().write_out(
            PluginDestCtx(work),
            conn,
            ClickHouseDest(tables=[table], if_table_exists="append"),
            [pl.scan_parquet(parquet)],
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
def test_dest_plugin_write_out_replace_keeps_the_table(
    provider: str,
    domain_fs_employees_xs: FsDomain,
    domain_clickhouse: ClickhouseDomain,
    domain_s3: S3Domain,
    domain_azureblob: AzureblobDomain,
    domain_gcs: GCSDomain,
    tmp_path: Path,
):
    # A replace keeps the table and only ever grows its schema: a different
    # entity is refused under strict, leaving the first load in place, and
    # under update it adds its columns and leaves only its own rows.
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
            [pl.scan_parquet(first)],
        )
        assert db.count(table) == pl.read_parquet(first).height

        with pytest.raises(Exception, match="SQL_COMMON_10"):
            plugin._ClickHouseDestPlugin().write_out(
                PluginDestCtx(work),
                conn,
                ClickHouseDest(tables=[table], if_table_exists="replace"),
                [pl.scan_parquet(second)],
            )
        kept = db.read_table(table)
        assert kept.height == pl.read_parquet(first).height
        assert set(kept.columns) == set(pl.read_parquet(first).columns)
        plugin._ClickHouseDestPlugin().write_out(
            PluginDestCtx(work),
            conn,
            ClickHouseDest(
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
            [pl.scan_parquet(parquet)],
        )
        plugin._ClickHouseDestPlugin().write_out(
            PluginDestCtx(work),
            conn,
            ClickHouseDest(
                tables=[template.format(db=domain_clickhouse.database, table=table)],
                if_table_exists="append",
            ),
            [pl.scan_parquet(parquet)],
        )
        assert db.count(table) == 2 * rows
    finally:
        db.drop_table(table)


class _FunctionCtx(PluginDestCtx):
    def __init__(self, work: Path, function_id: str) -> None:
        super().__init__(work)
        self._function_id = function_id

    @property
    def function_id(self) -> str:
        return self._function_id


def _staged(storage) -> list[str]:
    """Every staged key, relative to the staging ``base_path``."""
    import obstore

    prefix = f"{storage.base_path.strip('/')}/".lstrip("/")
    return sorted(
        meta["path"][len(prefix) :]
        for batch in obstore.list(storage.store().store(), prefix=prefix)
        for meta in batch
    )


@pytest.mark.parametrize("provider", STAGING_PROVIDERS)
@clickhouse_domain("clickhouse")
@s3_domain("s3")
@azureblob_domain("azureblob")
@gcs_domain("gcs")
def test_dest_plugin_write_out_clears_only_its_own_staging_folder(
    provider: str,
    domain_clickhouse: ClickhouseDomain,
    domain_s3: S3Domain,
    domain_azureblob: AzureblobDomain,
    domain_gcs: GCSDomain,
    tmp_path: Path,
    caplog,
):
    from tabsdatak.conn.clickhouse._impl import build_cloud_storage

    db = store(domain_clickhouse)
    table = f"td_dest_sweep_{provider}"
    staging = staging_domain(provider, domain_s3, domain_azureblob, domain_gcs)
    conn = dest_conn(domain_clickhouse, staging)
    storage = build_cloud_storage(conn.staging)
    leftover = pl.LazyFrame({"id": [9]})
    # What a crashed run of this function left, and another function's load.
    storage.upload("fn-mine", [("crashed_0.parquet", leftover)])
    storage.upload("fn-other", [("running_0.parquet", leftover)])
    try:
        plugin._ClickHouseDestPlugin().write_out(
            _FunctionCtx(work_dir(tmp_path), "fn-mine"),
            conn,
            ClickHouseDest(tables=[table], if_table_exists="replace"),
            [pl.LazyFrame({"id": [1, 2]})],
        )
        assert db.count(table) == 2
        assert not [r for r in caplog.records if "Cleanup failed" in r.getMessage()]
        assert _staged(storage) == ["fn-other/running_0.parquet"]
    finally:
        storage.delete("fn-other")
        db.drop_table(table)


@clickhouse_domain("clickhouse")
@s3_domain("s3")
def test_two_subscribers_on_one_staging_location_keep_each_others_files(
    domain_clickhouse: ClickhouseDomain,
    domain_s3: S3Domain,
    tmp_path: Path,
    monkeypatch,
):
    # noinspection PyProtectedMember
    from tabsdatak.conn.clickhouse._bulk_loader import CloudStagedBulkLoader
    from tabsdatak.conn.clickhouse._impl import build_cloud_storage

    db = store(domain_clickhouse)
    first, second = "td_dest_shared_first", "td_dest_shared_second"
    conn = dest_conn(domain_clickhouse, domain_s3)
    storage = build_cloud_storage(conn.staging)
    staged_meanwhile: list[str] = []

    def write(function_id: str, table: str, ids: list[int]) -> None:
        (tmp_path / function_id).mkdir()
        plugin._ClickHouseDestPlugin().write_out(
            _FunctionCtx(work_dir(tmp_path / function_id), function_id),
            conn,
            ClickHouseDest(tables=[table], if_table_exists="replace"),
            [pl.LazyFrame({"id": ids})],
        )

    load = CloudStagedBulkLoader.phase_bulk_load

    # The second subscriber starts and finishes while the first has staged its
    # files but not loaded them yet.
    def interleaved(self, *args, **kwargs):
        if self.staging_folder == "fn-first":
            write("fn-second", second, [3])
            staged_meanwhile.extend(_staged(storage))
        return load(self, *args, **kwargs)

    monkeypatch.setattr(CloudStagedBulkLoader, "phase_bulk_load", interleaved)
    try:
        write("fn-first", first, [1, 2])
        assert db.count(first) == 2
        assert db.count(second) == 1
        transaction = PluginDestCtx(tmp_path).transaction_id
        assert staged_meanwhile == [f"fn-first/{transaction}_0.parquet"]
        assert _staged(storage) == []
    finally:
        db.drop_table(first)
        db.drop_table(second)
