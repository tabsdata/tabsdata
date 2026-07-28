#
#  Copyright 2026 Tabsdata Inc.
#

"""StarRocks metadata-explorer integration tests"""

from __future__ import annotations

import pytest
from sqlalchemy.exc import DBAPIError
from tabsdatax.itest.fixture.backends.starrocks import StarrocksDomain, starrocks_domain
from testi_tabsdatak.conn.starrocks.backend import (
    UNUSED_STAGING,
    explorer_conn,
    store,
)

from tabsdatak.api import Secret
from tabsdatak.conn.common.sql._explorer import SQLEntity
from tabsdatak.conn.common.types import UserPassword
from tabsdatak.conn.starrocks import StarRocksDestConn
from tabsdatak.conn.starrocks._explorer import StarRocksConnectionExplorer

pytestmark = pytest.mark.starrocks

# The `starrocks` domain carries no model, so each test seeds the table it
# introspects and drops it again. The columns cover the type spellings the
# explorer canonicalizes.
SEEDED_COLUMNS = {
    "id": "BIGINT",
    "name": "VARCHAR(255)",
    "country_code": "CHAR(3)",
    "population": "INT",
}
SEEDED_ROWS = [
    {"id": 1, "name": "alice", "country_code": "USA", "population": 10},
    {"id": 2, "name": "bob", "country_code": "ESP", "population": 20},
    {"id": 3, "name": "carol", "country_code": "FRA", "population": 30},
]


def _seed(db, table: str) -> None:
    db.create_table(table, SEEDED_COLUMNS)
    db.insert_rows(table, SEEDED_ROWS)


def _entity_by_table(entities: list[SQLEntity], table: str) -> SQLEntity:
    matches = [e for e in entities if e.table == table]
    assert matches, f"no entity for {table!r} in {[e.table for e in entities]}"
    return matches[0]


# ---------------------------------------------------------------------------
# check_connect
# ---------------------------------------------------------------------------


@starrocks_domain("starrocks")
def test_check_connect_succeeds(domain_starrocks: StarrocksDomain):
    conn = explorer_conn(domain_starrocks)
    assert StarRocksConnectionExplorer().check_connect(conn) is None


@starrocks_domain("starrocks")
def test_check_connect_bad_credentials_raises(domain_starrocks: StarrocksDomain):
    bad = StarRocksDestConn(
        host=domain_starrocks.host,
        port=str(domain_starrocks.port),
        credentials=UserPassword(
            user=Secret(domain_starrocks.user),
            password=Secret("definitely-wrong-password"),
        ),
        staging=UNUSED_STAGING,
        database=domain_starrocks.database,
    )
    with pytest.raises(DBAPIError):
        StarRocksConnectionExplorer().check_connect(bad)


# ---------------------------------------------------------------------------
# metadata
# ---------------------------------------------------------------------------


@starrocks_domain("starrocks")
def test_metadata_lists_seeded_table(domain_starrocks: StarrocksDomain):
    db = store(domain_starrocks)
    table = "td_explorer_metadata"
    _seed(db, table)
    try:
        entities = StarRocksConnectionExplorer().metadata(
            explorer_conn(domain_starrocks)
        )

        assert entities and all(isinstance(e, SQLEntity) for e in entities)

        # The scan is pinned to the connection's database: everything it lists
        # lives there, so `information_schema` and the other server-owned
        # databases never show up. The listing is not an exact set -- the
        # database is shared with the destination tests, whose tables appear
        # alongside this one.
        assert {(e.database, e.schema) for e in entities} == {
            (domain_starrocks.database, "")
        }

        # StarRocks has no schema level: the database fills `database` and `schema`
        # is left blank.
        entity = _entity_by_table(entities, table)
        assert entity.database == domain_starrocks.database
        assert entity.schema == ""
    finally:
        db.drop_table(table)


@starrocks_domain("starrocks")
def test_metadata_reflects_columns(domain_starrocks: StarrocksDomain):
    db = store(domain_starrocks)
    table = "td_explorer_columns"
    _seed(db, table)
    try:
        entity = _entity_by_table(
            StarRocksConnectionExplorer().metadata(explorer_conn(domain_starrocks)),
            table,
        )

        by_name = {c.name: c for c in entity.columns}
        assert set(by_name) == set(SEEDED_COLUMNS)

        # Types are reflected and simplified to a short canonical name.
        assert by_name["id"].type == "BIGINT"
        assert by_name["name"].type == "VARCHAR"
        assert by_name["country_code"].type == "CHAR"
        assert by_name["population"].type.startswith("INT")
    finally:
        db.drop_table(table)


# ---------------------------------------------------------------------------
# data
# ---------------------------------------------------------------------------


@starrocks_domain("starrocks")
def test_data_returns_all_rows(domain_starrocks: StarrocksDomain):
    db = store(domain_starrocks)
    table = "td_explorer_data"
    _seed(db, table)
    try:
        conn = explorer_conn(domain_starrocks)
        explorer = StarRocksConnectionExplorer()
        entity = _entity_by_table(explorer.metadata(conn), table)

        rows = explorer.data(conn, entity, limit=len(SEEDED_ROWS))

        assert isinstance(rows, list)
        assert sorted(rows, key=lambda row: row["id"]) == SEEDED_ROWS
    finally:
        db.drop_table(table)


@starrocks_domain("starrocks")
def test_data_respects_limit(domain_starrocks: StarrocksDomain):
    db = store(domain_starrocks)
    table = "td_explorer_limit"
    _seed(db, table)
    try:
        conn = explorer_conn(domain_starrocks)
        explorer = StarRocksConnectionExplorer()
        entity = _entity_by_table(explorer.metadata(conn), table)

        assert len(explorer.data(conn, entity, limit=2)) == 2
    finally:
        db.drop_table(table)
