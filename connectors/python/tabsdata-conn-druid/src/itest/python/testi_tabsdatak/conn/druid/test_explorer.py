#
#  Copyright 2026 Tabsdata Inc.
#

"""Druid metadata-explorer integration tests"""

from __future__ import annotations

import pytest
from tabsdatax.itest.fixture.backends.druid import DruidDomain, druid_domain
from testi_tabsdatak.conn.druid.backend import explorer_conn, store

from tabsdatak.conn.common.sql._explorer import SQLEntity
from tabsdatak.conn.druid._explorer import DruidConnectionExplorer

# Every test in this package drives ingestion on the one shared Druid cluster,
# whose middle manager runs only a couple of tasks at a time. Left to spread
# across xdist workers they queue behind each other and blow their poll
# deadlines, so the whole package shares a group and runs on a single worker.
pytestmark = [pytest.mark.druid, pytest.mark.xdist_group("druid_cluster_seq")]

# A Druid datasource only exists once something has been ingested into it, and the
# `druid` domain carries no model, so each test seeds the datasource it
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


@druid_domain("druid")
def test_check_connect_succeeds(domain_druid: DruidDomain):
    conn = explorer_conn(domain_druid)
    assert DruidConnectionExplorer().check_connect(conn) is None


# ---------------------------------------------------------------------------
# metadata
# ---------------------------------------------------------------------------


@druid_domain("druid")
def test_metadata_lists_seeded_datasource(domain_druid: DruidDomain):
    db = store(domain_druid)
    datasource = "td_explorer_metadata"
    db.create_int_table(datasource, SEEDED_ROWS)
    try:
        entities = DruidConnectionExplorer().metadata(explorer_conn(domain_druid))

        assert entities and all(isinstance(e, SQLEntity) for e in entities)

        # Datasources are cluster-wide rather than scoped to the domain, so the
        # seeded one is asserted to be among them and not to be the only one.
        # They all live in the `druid` schema, and Druid has no database level.
        assert all(e.schema == "druid" for e in entities)
        entity = _entity_by_table(entities, datasource)
        assert entity.database == ""
    finally:
        db.drop_table(datasource)


@druid_domain("druid")
def test_metadata_reflects_columns(domain_druid: DruidDomain):
    db = store(domain_druid)
    datasource = "td_explorer_columns"
    db.create_int_table(datasource, SEEDED_ROWS)
    try:
        entity = _entity_by_table(
            DruidConnectionExplorer().metadata(explorer_conn(domain_druid)),
            datasource,
        )

        # Ingestion adds the synthetic `__time` column, so the seeded columns are
        # a subset of what the scan reports.
        by_name = {c.name for c in entity.columns}
        assert {"id", "name"} <= by_name
    finally:
        db.drop_table(datasource)


# ---------------------------------------------------------------------------
# data
# ---------------------------------------------------------------------------


@druid_domain("druid")
def test_data_returns_all_rows(domain_druid: DruidDomain):
    db = store(domain_druid)
    datasource = "td_explorer_data"
    db.create_int_table(datasource, SEEDED_ROWS)
    try:
        conn = explorer_conn(domain_druid)
        explorer = DruidConnectionExplorer()
        entity = _entity_by_table(explorer.metadata(conn), datasource)

        rows = explorer.data(conn, entity, limit=len(SEEDED_ROWS))

        assert isinstance(rows, list)
        assert all(set(row) <= {c.name for c in entity.columns} for row in rows)
        seeded = [{key: row[key] for key in ("id", "name")} for row in rows]
        assert sorted(seeded, key=lambda row: row["id"]) == SEEDED_ROWS
    finally:
        db.drop_table(datasource)


@druid_domain("druid")
def test_data_respects_limit(domain_druid: DruidDomain):
    db = store(domain_druid)
    datasource = "td_explorer_limit"
    db.create_int_table(datasource, SEEDED_ROWS)
    try:
        conn = explorer_conn(domain_druid)
        explorer = DruidConnectionExplorer()
        entity = _entity_by_table(explorer.metadata(conn), datasource)

        assert len(explorer.data(conn, entity, limit=2)) == 2
    finally:
        db.drop_table(datasource)
