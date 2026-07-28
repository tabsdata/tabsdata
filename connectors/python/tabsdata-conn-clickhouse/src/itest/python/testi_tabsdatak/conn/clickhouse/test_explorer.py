#
#  Copyright 2026 Tabsdata Inc.
#

"""ClickHouse metadata-explorer integration tests"""

from __future__ import annotations

import pytest
from tabsdatax.itest.fixture.backends.clickhouse import (
    ClickhouseDomain,
    clickhouse_domain,
)
from testi_tabsdatak.conn.clickhouse.backend import explorer_conn, store

from tabsdatak.conn.clickhouse._explorer import ClickHouseConnectionExplorer
from tabsdatak.conn.common.sql._explorer import SQLEntity

pytestmark = pytest.mark.clickhouse

# The `clickhouse` domain carries no model, so each test seeds the table it
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


@clickhouse_domain("clickhouse")
def test_check_connect_succeeds(domain_clickhouse: ClickhouseDomain):
    conn = explorer_conn(domain_clickhouse)
    assert ClickHouseConnectionExplorer().check_connect(conn) is None


# ---------------------------------------------------------------------------
# metadata
# ---------------------------------------------------------------------------


@clickhouse_domain("clickhouse")
def test_metadata_lists_seeded_table(domain_clickhouse: ClickhouseDomain):
    db = store(domain_clickhouse)
    table = "td_explorer_metadata"
    db.create_int_table(table, SEEDED_ROWS)
    try:
        entities = ClickHouseConnectionExplorer().metadata(
            explorer_conn(domain_clickhouse)
        )

        assert entities and all(isinstance(e, SQLEntity) for e in entities)

        # The scan is pinned to the connection's database: everything it lists
        # lives there, so `system` and the other server-owned databases never
        # show up. The listing is not an exact set -- the database is shared with
        # the destination tests, whose tables appear alongside this one.
        assert {(e.database, e.schema) for e in entities} == {
            (domain_clickhouse.database, "")
        }

        # ClickHouse has no schema level: the database fills `database` and
        # `schema` is left blank.
        entity = _entity_by_table(entities, table)
        assert entity.database == domain_clickhouse.database
        assert entity.schema == ""
    finally:
        db.drop_table(table)


@clickhouse_domain("clickhouse")
def test_metadata_reflects_columns(domain_clickhouse: ClickhouseDomain):
    db = store(domain_clickhouse)
    table = "td_explorer_columns"
    db.create_int_table(table, SEEDED_ROWS)
    try:
        entity = _entity_by_table(
            ClickHouseConnectionExplorer().metadata(explorer_conn(domain_clickhouse)),
            table,
        )

        by_name = {c.name: c for c in entity.columns}
        assert set(by_name) == {"id", "name"}

        # Types keep ClickHouse's native spelling, with any Nullable(...) marker
        # unwrapped into the flag and the parameter list dropped.
        assert by_name["id"].type == "Int32"
        assert by_name["name"].type == "String"
        assert not any(column.nullable for column in by_name.values())

        # The MergeTree sorting key is what ClickHouse reports as the primary key.
        assert by_name["id"].primary_key is True
        assert by_name["name"].primary_key is False
    finally:
        db.drop_table(table)


# ---------------------------------------------------------------------------
# data
# ---------------------------------------------------------------------------


@clickhouse_domain("clickhouse")
def test_data_returns_all_rows(domain_clickhouse: ClickhouseDomain):
    db = store(domain_clickhouse)
    table = "td_explorer_data"
    db.create_int_table(table, SEEDED_ROWS)
    try:
        conn = explorer_conn(domain_clickhouse)
        explorer = ClickHouseConnectionExplorer()
        entity = _entity_by_table(explorer.metadata(conn), table)

        rows = explorer.data(conn, entity, limit=len(SEEDED_ROWS))

        assert isinstance(rows, list)
        assert rows == SEEDED_ROWS
    finally:
        db.drop_table(table)


@clickhouse_domain("clickhouse")
def test_data_respects_limit(domain_clickhouse: ClickhouseDomain):
    db = store(domain_clickhouse)
    table = "td_explorer_limit"
    db.create_int_table(table, SEEDED_ROWS)
    try:
        conn = explorer_conn(domain_clickhouse)
        explorer = ClickHouseConnectionExplorer()
        entity = _entity_by_table(explorer.metadata(conn), table)

        assert len(explorer.data(conn, entity, limit=2)) == 2
    finally:
        db.drop_table(table)
