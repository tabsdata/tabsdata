#
#  Copyright 2026. Tabs Data Inc.
#

"""End-to-end ``BigQueryConnectionExplorer`` tests against a real BigQuery
project.

BigQuery has no local sandbox: these are live tests driven by the
``fresh_bigquery`` fixture (see ``conftest.py``), which reads the
``GCP0__*`` environment variables (service account key, project,
dataset, GCS staging URI) and skips the whole suite when any is missing.
"""

from __future__ import annotations

import uuid

import pytest

from tabsdatak.conn.bigquery._explorer import BigQueryConnectionExplorer
from tabsdatak.conn.common.sql._explorer import SQLEntity

# noinspection PyProtectedMember
from test_tabsdatak.conn.bigquery._integration_helpers import _bq_client, _conn_from

pytestmark = [pytest.mark.bigquery]


def test_check_connect_succeeds(fresh_bigquery):
    conn = _conn_from(fresh_bigquery)
    assert BigQueryConnectionExplorer().check_connect(conn) is None


def test_metadata_returns_sql_entities(fresh_bigquery):
    conn = _conn_from(fresh_bigquery)
    entities = BigQueryConnectionExplorer().metadata(conn)
    assert isinstance(entities, list)
    assert all(isinstance(e, SQLEntity) for e in entities)
    # The project is reported as `database` for every entity.
    assert all(e.database == fresh_bigquery.project for e in entities)


def test_data_reads_sample_rows(fresh_bigquery):
    # Self-contained: seed a table this test owns, so the read is independent of
    # any other backend test's tables in the shared dataset.
    cfg = fresh_bigquery
    table = f"td_bigquery_explorer_{uuid.uuid4().hex[:16]}"
    full = f"{cfg.project}.{cfg.dataset}.{table}"

    client = _bq_client(cfg)
    try:
        client.query(
            f"CREATE TABLE `{full}` (id INT64, name STRING) AS "
            "SELECT 1 AS id, 'alice' AS name "
            "UNION ALL SELECT 2, 'bob'"
        ).result()

        conn = _conn_from(cfg)
        explorer = BigQueryConnectionExplorer()
        entity = next(e for e in explorer.metadata(conn) if e.table == table)

        rows = explorer.data(conn, entity, limit=5)
        assert isinstance(rows, list)
        assert len(rows) == 2
        expected = {c.name for c in entity.columns}
        assert all(set(r) == expected for r in rows)
    finally:
        try:
            client.delete_table(full, not_found_ok=True)
        finally:
            client.close()
