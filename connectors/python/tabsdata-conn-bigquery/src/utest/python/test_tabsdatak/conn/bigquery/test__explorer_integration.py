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

import pytest

from tabsdatak.conn.bigquery._explorer import BigQueryConnectionExplorer
from tabsdatak.conn.common.sql._explorer import SQLEntity

# noinspection PyProtectedMember
from test_tabsdatak.conn.bigquery._integration_helpers import _conn_from

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
    conn = _conn_from(fresh_bigquery)
    explorer = BigQueryConnectionExplorer()
    entities = explorer.metadata(conn)
    if not entities:
        pytest.skip("no tables present in the configured BigQuery dataset")

    rows = explorer.data(conn, entities[0], limit=5)
    assert isinstance(rows, list)
    assert len(rows) <= 5
    expected = {c.name for c in entities[0].columns}
    assert all(set(r) == expected for r in rows)
