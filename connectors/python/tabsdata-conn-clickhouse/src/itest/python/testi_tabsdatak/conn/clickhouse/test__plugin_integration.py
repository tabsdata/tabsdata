#
# Copyright 2026 Tabsdata Inc.
#

"""ClickHouse connector integration tests: drive ``_ClickHouseDestPlugin.write_out``
directly against a tdx-provisioned ``clickhouse`` domain with a fabricated plugin
context -- unlike ``test__plugin_e2e.py``, which goes through a running tabsdata
instance. ClickHouse is destination-only (no ``ClickHouseSrc``) and bulk-loads
through cloud staging, so each test also declares the object-store backends whose
credentials build the staging ``CloudLocation``. Every test is parametrized over
all three supported staging providers -- ``s3`` / ``azblob`` / ``gcs`` (ClickHouse
reaches GCS via HMAC keys, unlike StarRocks). Writes land in uniquely-named scratch
tables (the connector creates them; dropped on teardown) and are verified with a
``clickhouse_connect`` client, never through the connector.

Requires a ClickHouse docker container, the ``clickhouse`` domain provisioned, and
the ``s3`` / ``azureblob`` / ``gcs`` backends whose buckets are reachable from the
container.
"""

from __future__ import annotations

import uuid
from pathlib import Path

import polars as pl
import pytest
from tabsdatax.itest.fixture.backends.azureblob import AzureblobDomain, azureblob_domain
from tabsdatax.itest.fixture.backends.clickhouse import (
    ClickhouseDomain,
    clickhouse_domain,
)
from tabsdatax.itest.fixture.backends.gcs import GcsDomain, gcs_domain
from tabsdatax.itest.fixture.backends.s3 import S3Domain, s3_domain
from tabsdatax.itest.fixture.functions.files import PluginDestCtx
from tabsdatax.itest.fixture.functions.harness import assert_frames_match
from tabsdatax.itest.fixture.functions.sql import (
    scratch_table,
    sql_credentials,
    write_parquet,
)

# noinspection PyProtectedMember
from tabsdatak.conn.clickhouse import (
    ClickHouseDest,
    ClickHouseDestConn,
    _plugin as plugin,
)

pytestmark = pytest.mark.clickhouse

_STAGING_PROVIDERS = ["s3", "azblob", "gcs"]


def _staging_path(backend) -> str:
    base = (
        backend.prefix
        if isinstance(backend, AzureblobDomain)
        else str(backend.base_path)
    )
    return f"/{base.strip('/')}/clickhouse-staging/{uuid.uuid4().hex}"


def _dest_conn(
    domain: ClickhouseDomain, staging_backend, staging
) -> ClickHouseDestConn:
    return ClickHouseDestConn(
        host=domain.host,
        port=str(domain.port),
        credentials=sql_credentials(domain),
        staging=staging(staging_backend, _staging_path(staging_backend)),
        database=domain.database,
        secure="false",
    )


def _staging_backend(provider, domain_s3, domain_azureblob, domain_gcs):
    return {"s3": domain_s3, "azblob": domain_azureblob, "gcs": domain_gcs}[provider]


@pytest.mark.parametrize("provider", _STAGING_PROVIDERS)
@clickhouse_domain("clickhouse")
@s3_domain("s3")
@azureblob_domain("azureblob")
@gcs_domain("gcs")
def test_dest_then_read_round_trip(
    provider: str,
    domain_clickhouse: ClickhouseDomain,
    domain_s3: S3Domain,
    domain_azureblob: AzureblobDomain,
    domain_gcs: GcsDomain,
    clickhouse,
    staging,
    tmp_path: Path,
):
    db = clickhouse(domain_clickhouse)
    backend = _staging_backend(provider, domain_s3, domain_azureblob, domain_gcs)
    table = scratch_table()
    work = tmp_path / "work"
    work.mkdir()
    parquet = write_parquet(
        tmp_path / "in.parquet",
        [{"id": 1, "name": "alice"}, {"id": 2, "name": "bob"}],
    )
    try:
        plugin._ClickHouseDestPlugin().write_out(
            PluginDestCtx(work),
            _dest_conn(domain_clickhouse, backend, staging),
            ClickHouseDest(tables=[table], if_table_exists="replace"),
            [parquet],
        )
        assert_frames_match(
            db.read_table(table),
            pl.DataFrame([{"id": 1, "name": "alice"}, {"id": 2, "name": "bob"}]),
        )
    finally:
        db.drop_table(table)


@pytest.mark.parametrize("provider", _STAGING_PROVIDERS)
@clickhouse_domain("clickhouse")
@s3_domain("s3")
@azureblob_domain("azureblob")
@gcs_domain("gcs")
def test_dest_append_accumulates(
    provider: str,
    domain_clickhouse: ClickhouseDomain,
    domain_s3: S3Domain,
    domain_azureblob: AzureblobDomain,
    domain_gcs: GcsDomain,
    clickhouse,
    staging,
    tmp_path: Path,
):
    db = clickhouse(domain_clickhouse)
    backend = _staging_backend(provider, domain_s3, domain_azureblob, domain_gcs)
    table = scratch_table()
    work = tmp_path / "work"
    work.mkdir()
    first = write_parquet(tmp_path / "a.parquet", [{"id": 1, "name": "alice"}])
    second = write_parquet(tmp_path / "b.parquet", [{"id": 2, "name": "bob"}])
    dest_conn = _dest_conn(domain_clickhouse, backend, staging)
    try:
        plugin._ClickHouseDestPlugin().write_out(
            PluginDestCtx(work),
            dest_conn,
            ClickHouseDest(tables=[table], if_table_exists="replace"),
            [first],
        )
        plugin._ClickHouseDestPlugin().write_out(
            PluginDestCtx(work),
            dest_conn,
            ClickHouseDest(tables=[table], if_table_exists="append"),
            [second],
        )
        assert db.count(table) == 2
    finally:
        db.drop_table(table)


@pytest.mark.parametrize("provider", _STAGING_PROVIDERS)
@clickhouse_domain("clickhouse")
@s3_domain("s3")
@azureblob_domain("azureblob")
@gcs_domain("gcs")
def test_dest_replace_drops_existing(
    provider: str,
    domain_clickhouse: ClickhouseDomain,
    domain_s3: S3Domain,
    domain_azureblob: AzureblobDomain,
    domain_gcs: GcsDomain,
    clickhouse,
    staging,
    tmp_path: Path,
):
    db = clickhouse(domain_clickhouse)
    backend = _staging_backend(provider, domain_s3, domain_azureblob, domain_gcs)
    table = scratch_table()
    work = tmp_path / "work"
    work.mkdir()
    first = write_parquet(
        tmp_path / "a.parquet", [{"id": 1, "name": "alice"}, {"id": 2, "name": "bob"}]
    )
    second = write_parquet(tmp_path / "b.parquet", [{"id": 3, "name": "carol"}])
    dest_conn = _dest_conn(domain_clickhouse, backend, staging)
    try:
        plugin._ClickHouseDestPlugin().write_out(
            PluginDestCtx(work),
            dest_conn,
            ClickHouseDest(tables=[table], if_table_exists="replace"),
            [first],
        )
        plugin._ClickHouseDestPlugin().write_out(
            PluginDestCtx(work),
            dest_conn,
            ClickHouseDest(tables=[table], if_table_exists="replace"),
            [second],
        )
        got = db.read_table(table)
        assert got["name"].to_list() == ["carol"]
    finally:
        db.drop_table(table)
