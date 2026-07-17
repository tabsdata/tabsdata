#
# Copyright 2026 Tabsdata Inc.
#

"""Druid connector integration tests: drive ``DruidDestPlugin.write_out``
directly against a tdx-provisioned ``druid`` domain (a real Druid cluster) with a
fabricated plugin context -- unlike ``test__plugin_e2e.py``, which goes through a
running tabsdata instance. Druid is destination-only (no ``DruidSrc``); it stages
parquet to S3 and POSTs an ingestion task that creates the datasource from the
parquet schema, so tests need no pre-created table (staging bucket + credentials
come from the ``s3`` backend). Writes land in uniquely-named scratch datasources
(dropped on teardown) and are verified with Druid SQL, never through the
connector.

Ingestion is asynchronous and Druid is eventually consistent, so verification
polls (see ``_DruidStore``). Requires the ``druid`` domain (Overlord/Router +
auth) and an ``s3`` staging backend."""

from __future__ import annotations

import uuid
from pathlib import Path

import polars as pl
import pytest
from tabsdatax.itest.fixture.backends.druid import DruidDomain, druid_domain
from tabsdatax.itest.fixture.backends.s3 import S3Domain, s3_domain
from tabsdatax.itest.fixture.functions.files import PluginDestCtx
from tabsdatax.itest.fixture.functions.harness import assert_frames_match
from tabsdatax.itest.fixture.functions.sql import scratch_table, write_parquet

from tabsdatak.api import Secret
from tabsdatak.conn.druid import (
    DruidDest,
    DruidDestConn,
    UserPassword,
    _plugin as plugin,
)

pytestmark = pytest.mark.druid


def _dest_conn(
    domain_druid: DruidDomain, domain_s3: S3Domain, staging
) -> DruidDestConn:
    base_path = f"/druid-staging/{uuid.uuid4().hex}"
    return DruidDestConn(
        overlord_url=domain_druid.overlord_url,
        staging=staging(domain_s3, base_path),
        credentials=UserPassword(
            user=Secret(domain_druid.user),
            password=Secret(domain_druid.password),
        ),
    )


@druid_domain("druid")
@s3_domain("s3")
def test_dest_then_read_round_trip(
    domain_druid: DruidDomain,
    domain_s3: S3Domain,
    druid,
    staging,
    tmp_path: Path,
):
    store = druid(domain_druid)
    datasource = scratch_table()
    work = tmp_path / "work"
    work.mkdir()
    parquet = write_parquet(
        tmp_path / "in.parquet",
        [{"id": 1, "name": "alice"}, {"id": 2, "name": "bob"}],
    )
    try:
        plugin.DruidDestPlugin().write_out(
            PluginDestCtx(work),
            _dest_conn(domain_druid, domain_s3, staging),
            DruidDest(tables=[datasource], if_table_exists="replace"),
            [parquet],
        )
        assert_frames_match(
            store.read_table(datasource, expected_rows=2),
            pl.DataFrame([{"id": 1, "name": "alice"}, {"id": 2, "name": "bob"}]),
        )
    finally:
        store.drop_datasource(datasource)


@druid_domain("druid")
@s3_domain("s3")
def test_dest_append_accumulates(
    domain_druid: DruidDomain,
    domain_s3: S3Domain,
    druid,
    staging,
    tmp_path: Path,
):
    store = druid(domain_druid)
    datasource = scratch_table()
    work = tmp_path / "work"
    work.mkdir()
    first = write_parquet(tmp_path / "a.parquet", [{"id": 1, "name": "alice"}])
    second = write_parquet(tmp_path / "b.parquet", [{"id": 2, "name": "bob"}])
    try:
        plugin.DruidDestPlugin().write_out(
            PluginDestCtx(work),
            _dest_conn(domain_druid, domain_s3, staging),
            DruidDest(tables=[datasource], if_table_exists="replace"),
            [first],
        )
        plugin.DruidDestPlugin().write_out(
            PluginDestCtx(work),
            _dest_conn(domain_druid, domain_s3, staging),
            DruidDest(tables=[datasource], if_table_exists="append"),
            [second],
        )
        assert store.wait_count(datasource, 2) == 2
    finally:
        store.drop_datasource(datasource)


@druid_domain("druid")
@s3_domain("s3")
def test_dest_replace_drops_existing(
    domain_druid: DruidDomain,
    domain_s3: S3Domain,
    druid,
    staging,
    tmp_path: Path,
):
    store = druid(domain_druid)
    datasource = scratch_table()
    work = tmp_path / "work"
    work.mkdir()
    first = write_parquet(
        tmp_path / "a.parquet", [{"id": 1, "name": "alice"}, {"id": 2, "name": "bob"}]
    )
    second = write_parquet(tmp_path / "b.parquet", [{"id": 3, "name": "carol"}])
    try:
        plugin.DruidDestPlugin().write_out(
            PluginDestCtx(work),
            _dest_conn(domain_druid, domain_s3, staging),
            DruidDest(tables=[datasource], if_table_exists="replace"),
            [first],
        )
        plugin.DruidDestPlugin().write_out(
            PluginDestCtx(work),
            _dest_conn(domain_druid, domain_s3, staging),
            DruidDest(tables=[datasource], if_table_exists="replace"),
            [second],
        )
        got = store.read_table(datasource, expected_rows=1)
        assert got["name"].to_list() == ["carol"]
    finally:
        store.drop_datasource(datasource)
