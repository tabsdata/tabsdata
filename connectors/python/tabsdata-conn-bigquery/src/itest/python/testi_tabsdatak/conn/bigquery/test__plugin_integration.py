#
# Copyright 2026 Tabsdata Inc.
#

"""BigQuery connector integration tests: drive ``_BigQueryDestPlugin.write_out``
directly against a tdx-provisioned ``bigquery`` domain (a real BigQuery dataset)
with a fabricated plugin context -- unlike ``test__plugin_e2e.py``, which goes
through a running tabsdata instance. BigQuery is destination-only (no
``BigQuerySrc``); it stages parquet to GCS and runs a load job that creates the
table from the parquet schema, so tests need no pre-created table (the staging
bucket + credentials come from the domain). Writes land in uniquely-named scratch
tables (dropped on teardown) and are verified with a ``google-cloud-bigquery``
client, never through the connector.

BigQuery preserves identifier case, so no case normalization is needed. Requires
the ``bigquery`` domain provisioned with project/dataset/bucket/SA creds.
"""

from __future__ import annotations

from pathlib import Path

import polars as pl
import pytest
from tabsdatax.itest.fixture.backends.bigquery import BigqueryDomain, bigquery_domain
from tabsdatax.itest.fixture.functions.files import PluginDestCtx
from tabsdatax.itest.fixture.functions.harness import assert_frames_match
from tabsdatax.itest.fixture.functions.sql import scratch_table, write_parquet

from tabsdatak.api import Secret

# noinspection PyProtectedMember
from tabsdatak.conn.bigquery import (
    BigQueryDest,
    BigQueryDestConn,
    GCPJson,
    _plugin as plugin,
)

pytestmark = pytest.mark.bigquery


def _dest_conn(domain: BigqueryDomain, staging) -> BigQueryDestConn:
    return BigQueryDestConn(
        staging=staging(domain),
        credentials=GCPJson(json=Secret(domain.service_account_key)),
        project=domain.project,
        dataset=domain.dataset,
    )


@bigquery_domain("bigquery")
def test_dest_then_read_round_trip(
    domain_bigquery: BigqueryDomain, bigquery, staging, tmp_path: Path
):
    db = bigquery(domain_bigquery)
    table = scratch_table()
    work = tmp_path / "work"
    work.mkdir()
    parquet = write_parquet(
        tmp_path / "in.parquet",
        [{"id": 1, "name": "alice"}, {"id": 2, "name": "bob"}],
    )
    try:
        plugin._BigQueryDestPlugin().write_out(
            PluginDestCtx(work),
            _dest_conn(domain_bigquery, staging),
            BigQueryDest(tables=[f"`{table}`"], if_table_exists="replace"),
            [parquet],
        )
        assert_frames_match(
            db.read_table(table),
            pl.DataFrame([{"id": 1, "name": "alice"}, {"id": 2, "name": "bob"}]),
        )
    finally:
        db.drop_table(table)


@bigquery_domain("bigquery")
def test_dest_append_accumulates(
    domain_bigquery: BigqueryDomain, bigquery, staging, tmp_path: Path
):
    db = bigquery(domain_bigquery)
    table = scratch_table()
    work = tmp_path / "work"
    work.mkdir()
    first = write_parquet(tmp_path / "a.parquet", [{"id": 1, "name": "alice"}])
    second = write_parquet(tmp_path / "b.parquet", [{"id": 2, "name": "bob"}])
    dest_conn = _dest_conn(domain_bigquery, staging)
    try:
        plugin._BigQueryDestPlugin().write_out(
            PluginDestCtx(work),
            dest_conn,
            BigQueryDest(tables=[f"`{table}`"], if_table_exists="replace"),
            [first],
        )
        plugin._BigQueryDestPlugin().write_out(
            PluginDestCtx(work),
            dest_conn,
            BigQueryDest(tables=[f"`{table}`"], if_table_exists="append"),
            [second],
        )
        assert db.count(table) == 2
    finally:
        db.drop_table(table)


@bigquery_domain("bigquery")
def test_dest_replace_drops_existing(
    domain_bigquery: BigqueryDomain, bigquery, staging, tmp_path: Path
):
    db = bigquery(domain_bigquery)
    table = scratch_table()
    work = tmp_path / "work"
    work.mkdir()
    first = write_parquet(
        tmp_path / "a.parquet", [{"id": 1, "name": "alice"}, {"id": 2, "name": "bob"}]
    )
    second = write_parquet(tmp_path / "b.parquet", [{"id": 3, "name": "carol"}])
    dest_conn = _dest_conn(domain_bigquery, staging)
    try:
        plugin._BigQueryDestPlugin().write_out(
            PluginDestCtx(work),
            dest_conn,
            BigQueryDest(tables=[f"`{table}`"], if_table_exists="replace"),
            [first],
        )
        plugin._BigQueryDestPlugin().write_out(
            PluginDestCtx(work),
            dest_conn,
            BigQueryDest(tables=[f"`{table}`"], if_table_exists="replace"),
            [second],
        )
        got = db.read_table(table)
        assert got["name"].to_list() == ["carol"]
    finally:
        db.drop_table(table)
