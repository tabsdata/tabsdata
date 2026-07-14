#
# Copyright 2026 Tabsdata Inc.
#

"""StarRocks connector integration tests: drive ``StarRocksDestPlugin.write_out``
directly against a tdx-provisioned ``starrocks`` domain with a fabricated plugin
context -- unlike ``test__plugin_e2e.py``, which goes through a running tabsdata
instance. StarRocks is destination-only (no ``StarRocksSrc``) and bulk-loads
through cloud staging, so each test also declares the object-store backends whose
credentials build the staging ``CloudLocation``. Every test is parametrized over
the supported staging providers -- ``s3`` (``S3Location``) and ``azblob``
(``AzBlobLocation``); GCS is skipped because the connector raises STARROCKS_7 for
it (missing HMAC credential type, TD-2803). Writes land in uniquely-named scratch
tables (dropped on teardown) and are verified over StarRocks' MySQL wire protocol
(the ``sql`` fixture), never through the connector.

Requires a StarRocks docker container, the ``starrocks`` domain provisioned, and
the ``s3`` / ``azureblob`` backends whose buckets are reachable from the
container.
"""

from __future__ import annotations

import uuid
from pathlib import Path

import polars as pl
import pytest
from tabsdatax.itest.fixture.backends.azureblob import AzureblobDomain, azureblob_domain
from tabsdatax.itest.fixture.backends.s3 import S3Domain, s3_domain
from tabsdatax.itest.fixture.backends.starrocks import StarrocksDomain, starrocks_domain
from tabsdatax.itest.fixture.functions.files import PluginDestCtx
from tabsdatax.itest.fixture.functions.harness import assert_frames_match
from tabsdatax.itest.fixture.functions.sql import (
    scratch_table,
    sql_credentials,
    write_parquet,
)

# noinspection PyProtectedMember
from tabsdatak.conn.starrocks import (
    StarRocksDest,
    StarRocksDestConn,
    _plugin as plugin,
)

pytestmark = pytest.mark.starrocks

# Once TD-2803 lands (GCS staging gains an HMAC credential type instead of always
# raising STARROCKS_7), add "gcs" here to parametrize these tests over it too.
_STAGING_PROVIDERS = ["s3", "azblob"]


def _staging_path(backend) -> str:
    # S3Domain isolates under its base_path; AzureblobDomain under its prefix.
    base = str(backend.base_path) if isinstance(backend, S3Domain) else backend.prefix
    return f"/{base.strip('/')}/starrocks-staging/{uuid.uuid4().hex}"


def _dest_conn(domain: StarrocksDomain, staging_backend, staging) -> StarRocksDestConn:
    return StarRocksDestConn(
        host=domain.host,
        port=int(domain.port),
        credentials=sql_credentials(domain),
        staging=staging(staging_backend, _staging_path(staging_backend)),
        database=domain.database,
    )


def _staging_backend(provider: str, domain_s3, domain_azureblob):
    return domain_s3 if provider == "s3" else domain_azureblob


@pytest.mark.parametrize("provider", _STAGING_PROVIDERS)
@starrocks_domain("starrocks")
@s3_domain("s3")
@azureblob_domain("azureblob")
def test_dest_then_read_round_trip(
    provider: str,
    domain_starrocks: StarrocksDomain,
    domain_s3: S3Domain,
    domain_azureblob: AzureblobDomain,
    sql,
    staging,
    create_table,
    tmp_path: Path,
):
    db = sql(domain_starrocks)
    backend = _staging_backend(provider, domain_s3, domain_azureblob)
    table = scratch_table()
    work = tmp_path / "work"
    work.mkdir()
    create_table(domain_starrocks, table)
    parquet = write_parquet(
        tmp_path / "in.parquet",
        [{"id": 1, "name": "alice"}, {"id": 2, "name": "bob"}],
    )
    try:
        plugin.StarRocksDestPlugin().write_out(
            PluginDestCtx(work),
            _dest_conn(domain_starrocks, backend, staging),
            StarRocksDest(tables=[table], if_table_exists="replace"),
            [parquet],
        )
        assert_frames_match(
            db.read_table(table),
            pl.DataFrame([{"id": 1, "name": "alice"}, {"id": 2, "name": "bob"}]),
        )
    finally:
        db.drop_table(table)


@pytest.mark.parametrize("provider", _STAGING_PROVIDERS)
@starrocks_domain("starrocks")
@s3_domain("s3")
@azureblob_domain("azureblob")
def test_dest_append_accumulates(
    provider: str,
    domain_starrocks: StarrocksDomain,
    domain_s3: S3Domain,
    domain_azureblob: AzureblobDomain,
    sql,
    staging,
    create_table,
    tmp_path: Path,
):
    db = sql(domain_starrocks)
    backend = _staging_backend(provider, domain_s3, domain_azureblob)
    table = scratch_table()
    work = tmp_path / "work"
    work.mkdir()
    create_table(domain_starrocks, table)
    first = write_parquet(tmp_path / "a.parquet", [{"id": 1, "name": "alice"}])
    second = write_parquet(tmp_path / "b.parquet", [{"id": 2, "name": "bob"}])
    dest_conn = _dest_conn(domain_starrocks, backend, staging)
    try:
        plugin.StarRocksDestPlugin().write_out(
            PluginDestCtx(work),
            dest_conn,
            StarRocksDest(tables=[table], if_table_exists="replace"),
            [first],
        )
        plugin.StarRocksDestPlugin().write_out(
            PluginDestCtx(work),
            dest_conn,
            StarRocksDest(tables=[table], if_table_exists="append"),
            [second],
        )
        assert db.read_table(table).height == 2
    finally:
        db.drop_table(table)


@pytest.mark.parametrize("provider", _STAGING_PROVIDERS)
@starrocks_domain("starrocks")
@s3_domain("s3")
@azureblob_domain("azureblob")
def test_dest_replace_drops_existing(
    provider: str,
    domain_starrocks: StarrocksDomain,
    domain_s3: S3Domain,
    domain_azureblob: AzureblobDomain,
    sql,
    staging,
    create_table,
    tmp_path: Path,
):
    db = sql(domain_starrocks)
    backend = _staging_backend(provider, domain_s3, domain_azureblob)
    table = scratch_table()
    work = tmp_path / "work"
    work.mkdir()
    create_table(domain_starrocks, table)
    first = write_parquet(
        tmp_path / "a.parquet", [{"id": 1, "name": "alice"}, {"id": 2, "name": "bob"}]
    )
    second = write_parquet(tmp_path / "b.parquet", [{"id": 3, "name": "carol"}])
    dest_conn = _dest_conn(domain_starrocks, backend, staging)
    try:
        plugin.StarRocksDestPlugin().write_out(
            PluginDestCtx(work),
            dest_conn,
            StarRocksDest(tables=[table], if_table_exists="replace"),
            [first],
        )
        plugin.StarRocksDestPlugin().write_out(
            PluginDestCtx(work),
            dest_conn,
            StarRocksDest(tables=[table], if_table_exists="replace"),
            [second],
        )
        got = db.read_table(table)
        assert got["name"].to_list() == ["carol"]
    finally:
        db.drop_table(table)
