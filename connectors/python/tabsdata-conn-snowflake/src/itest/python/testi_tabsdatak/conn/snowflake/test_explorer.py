#
#  Copyright 2026 Tabsdata Inc.
#

"""Snowflake metadata-explorer integration tests"""

from __future__ import annotations

import pytest
from sqlalchemy.exc import DatabaseError
from tabsdatax.itest.fixture.backends.snowflake import SnowflakeDomain, snowflake_domain
from testi_tabsdatak.conn.snowflake.backend import dest_conn, store

from tabsdatak.api import Secret
from tabsdatak.conn.common.sql._explorer import SQLEntity
from tabsdatak.conn.common.types import UserPassword
from tabsdatak.conn.snowflake import SnowflakeDestConn
from tabsdatak.conn.snowflake._explorer import SnowflakeConnectionExplorer

pytestmark = pytest.mark.snowflake

# The `snowflake` domain carries no model, so each test seeds the table it
# introspects and drops it again. Tables are seeded unquoted, so Snowflake
# stores them upper-folded and the snowflake-sqlalchemy dialect normalizes
# every reflected name back to lower case.
SEEDED_ROWS = [
    {"id": 1, "name": "alice"},
    {"id": 2, "name": "bob"},
    {"id": 3, "name": "carol"},
]


def _entity_by_table(entities: list[SQLEntity], table: str) -> SQLEntity:
    reflected = table.lower()
    matches = [e for e in entities if e.table == reflected]
    assert matches, f"no entity for {reflected!r} in {[e.table for e in entities]}"
    return matches[0]


# ---------------------------------------------------------------------------
# check_connect
# ---------------------------------------------------------------------------


@snowflake_domain("snowflake")
def test_check_connect_succeeds(domain_snowflake: SnowflakeDomain):
    conn = dest_conn(domain_snowflake)
    assert SnowflakeConnectionExplorer().check_connect(conn) is None


@snowflake_domain("snowflake")
def test_check_connect_bad_credentials_raises(domain_snowflake: SnowflakeDomain):
    bad = SnowflakeDestConn(
        account=domain_snowflake.account,
        credentials=UserPassword(
            user=Secret(domain_snowflake.username),
            password=Secret("definitely-wrong-password"),
        ),
        database=domain_snowflake.database,
        schema=domain_snowflake.schema,
        warehouse=domain_snowflake.warehouse,
    )
    with pytest.raises(DatabaseError):
        SnowflakeConnectionExplorer().check_connect(bad)


# ---------------------------------------------------------------------------
# metadata
# ---------------------------------------------------------------------------


@snowflake_domain("snowflake")
def test_metadata_lists_seeded_table(domain_snowflake: SnowflakeDomain):
    db = store(domain_snowflake)
    table = "TD_EXPLORER_METADATA"
    db.create_int_table(table, SEEDED_ROWS)
    try:
        conn = dest_conn(domain_snowflake)
        entities = SnowflakeConnectionExplorer().metadata(conn)

        assert entities and all(isinstance(e, SQLEntity) for e in entities)

        # The scan is pinned to the connection's schema: everything it lists
        # lives there. The listing is not an exact set -- the schema is shared
        # with the destination tests, whose tables show up alongside this one.
        assert {(e.database, e.schema) for e in entities} == {
            (domain_snowflake.database, domain_snowflake.schema)
        }

        entity = _entity_by_table(entities, table)
        assert entity.database == domain_snowflake.database
        assert entity.schema == domain_snowflake.schema
    finally:
        db.drop_table(table)


@snowflake_domain("snowflake")
def test_metadata_reflects_columns(domain_snowflake: SnowflakeDomain):
    db = store(domain_snowflake)
    table = "TD_EXPLORER_COLUMNS"
    db.create_int_table(table, SEEDED_ROWS)
    try:
        entity = _entity_by_table(
            SnowflakeConnectionExplorer().metadata(dest_conn(domain_snowflake)),
            table,
        )

        by_name = {c.name: c for c in entity.columns}
        assert set(by_name) == {"id", "name"}

        # INTEGER is an alias of NUMBER(38, 0) and VARCHAR keeps its own name;
        # the parameter list is dropped from the reported type.
        assert by_name["id"].type == "DECIMAL"
        assert by_name["name"].type == "VARCHAR"
        assert all(column.nullable for column in by_name.values())

        # Snowflake PK/FK are informational and the seed DDL declares none, so no
        # column reflects as a primary key.
        assert not any(column.primary_key for column in by_name.values())
    finally:
        db.drop_table(table)


# ---------------------------------------------------------------------------
# data
# ---------------------------------------------------------------------------


@snowflake_domain("snowflake")
def test_data_returns_all_rows(domain_snowflake: SnowflakeDomain):
    db = store(domain_snowflake)
    table = "TD_EXPLORER_DATA"
    db.create_int_table(table, SEEDED_ROWS)
    try:
        conn = dest_conn(domain_snowflake)
        explorer = SnowflakeConnectionExplorer()
        entity = _entity_by_table(explorer.metadata(conn), table)

        rows = explorer.data(conn, entity, limit=len(SEEDED_ROWS))

        assert isinstance(rows, list)
        # `data` applies no ordering, and `id` is a NUMBER column so it comes
        # back as a Decimal.
        got = sorted(({"id": int(r["id"]), "name": r["name"]} for r in rows), key=str)
        assert got == sorted(SEEDED_ROWS, key=str)
    finally:
        db.drop_table(table)


@snowflake_domain("snowflake")
def test_data_respects_limit(domain_snowflake: SnowflakeDomain):
    db = store(domain_snowflake)
    table = "TD_EXPLORER_LIMIT"
    db.create_int_table(table, SEEDED_ROWS)
    try:
        conn = dest_conn(domain_snowflake)
        explorer = SnowflakeConnectionExplorer()
        entity = _entity_by_table(explorer.metadata(conn), table)

        assert len(explorer.data(conn, entity, limit=2)) == 2
    finally:
        db.drop_table(table)
