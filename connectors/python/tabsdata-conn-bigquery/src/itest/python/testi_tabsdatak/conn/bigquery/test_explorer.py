#
#  Copyright 2026 Tabsdata Inc.
#

"""BigQuery metadata-explorer integration tests"""

from __future__ import annotations

import pytest
from tabsdatax.itest.fixture.backends.bigquery import BigqueryDomain, bigquery_domain
from testi_tabsdatak.conn.bigquery.backend import dest_conn, store

from tabsdatak.conn.bigquery._explorer import BigQueryConnectionExplorer
from tabsdatak.conn.common.sql._explorer import SQLEntity

pytestmark = pytest.mark.bigquery

# The `bigquery` domain carries no model, so each test seeds the table it
# introspects and drops it again.
SEEDED_ROWS = [
    {"id": 1, "name": "alice"},
    {"id": 2, "name": "bob"},
    {"id": 3, "name": "carol"},
]


def _entity_by_table(entities: list[SQLEntity], table: str) -> SQLEntity:
    matches = [e for e in entities if e.table == table]
    assert matches, f"no entity for {table!r} in {[e.table for e in entities]}"
    return matches[0]


# ---------------------------------------------------------------------------
# check_connect
# ---------------------------------------------------------------------------


@bigquery_domain("bigquery")
def test_check_connect_succeeds(domain_bigquery: BigqueryDomain):
    conn = dest_conn(domain_bigquery)
    assert BigQueryConnectionExplorer().check_connect(conn) is None


# ---------------------------------------------------------------------------
# metadata
# ---------------------------------------------------------------------------


@bigquery_domain("bigquery")
def test_metadata_lists_seeded_table(domain_bigquery: BigqueryDomain):
    db = store(domain_bigquery)
    table = "td_explorer_metadata"
    db.create_int_table(table, SEEDED_ROWS)
    try:
        entities = BigQueryConnectionExplorer().metadata(dest_conn(domain_bigquery))

        assert entities and all(isinstance(e, SQLEntity) for e in entities)

        # The scan is pinned to the connection's dataset: everything it lists
        # lives there. The listing is not an exact set -- the dataset is shared
        # with the destination tests, whose tables show up alongside this one.
        assert {(e.database, e.schema) for e in entities} == {
            (domain_bigquery.project, domain_bigquery.dataset)
        }

        # BigQuery has no schema level below the dataset: the project fills
        # `database` and the dataset fills `schema`.
        entity = _entity_by_table(entities, table)
        assert entity.database == domain_bigquery.project
        assert entity.schema == domain_bigquery.dataset
    finally:
        db.drop_table(table)


@bigquery_domain("bigquery")
def test_metadata_reflects_columns(domain_bigquery: BigqueryDomain):
    db = store(domain_bigquery)
    table = "td_explorer_columns"
    db.create_int_table(table, SEEDED_ROWS)
    try:
        entity = _entity_by_table(
            BigQueryConnectionExplorer().metadata(dest_conn(domain_bigquery)),
            table,
        )

        by_name = {c.name: c for c in entity.columns}
        assert set(by_name) == {"id", "name"}

        # The sqlalchemy-bigquery dialect reflects INT64 / STRING onto the
        # generic SQLAlchemy types, so they report their spelling, not BigQuery's.
        assert by_name["id"].type == "INTEGER"
        assert by_name["name"].type == "VARCHAR"
        assert all(column.nullable for column in by_name.values())

        # BigQuery has no primary keys to reflect.
        assert not any(column.primary_key for column in by_name.values())
    finally:
        db.drop_table(table)


# ---------------------------------------------------------------------------
# data
# ---------------------------------------------------------------------------


@bigquery_domain("bigquery")
def test_data_returns_all_rows(domain_bigquery: BigqueryDomain):
    db = store(domain_bigquery)
    table = "td_explorer_data"
    db.create_int_table(table, SEEDED_ROWS)
    try:
        conn = dest_conn(domain_bigquery)
        explorer = BigQueryConnectionExplorer()
        entity = _entity_by_table(explorer.metadata(conn), table)

        rows = explorer.data(conn, entity, limit=len(SEEDED_ROWS))

        assert isinstance(rows, list)
        # `data` applies no ordering.
        assert sorted(rows, key=str) == sorted(SEEDED_ROWS, key=str)
    finally:
        db.drop_table(table)


@bigquery_domain("bigquery")
def test_data_respects_limit(domain_bigquery: BigqueryDomain):
    db = store(domain_bigquery)
    table = "td_explorer_limit"
    db.create_int_table(table, SEEDED_ROWS)
    try:
        conn = dest_conn(domain_bigquery)
        explorer = BigQueryConnectionExplorer()
        entity = _entity_by_table(explorer.metadata(conn), table)

        assert len(explorer.data(conn, entity, limit=2)) == 2
    finally:
        db.drop_table(table)
