#
#  Copyright 2026. Tabs Data Inc.
#

"""End-to-end ``PostgresConnectionExplorer`` tests against a real
PostgreSQL instance.

The ``fresh_postgres`` fixture (defined in ``conftest.py``) provisions a
clean PostgreSQL backend per test and seeds a ``testing`` database with a
``users`` table (``id`` PK, ``name``, ``age``) and an ``orders`` table
(``id`` PK, ``user_id``, ``qty``) in the ``public`` schema. These tests
exercise the catalog introspection surface of
``PostgresConnectionExplorer`` -- ``check_connect`` / ``metadata`` /
``data`` -- against the live server. The three methods are inherited
from ``SqlAlchemyConnectionExplorer``; driving them through
``PostgresConnectionExplorer`` covers the Postgres-specific
``system_schemas`` / ``make_url`` overrides at the same time.
"""

from __future__ import annotations

import pytest
from sqlalchemy.exc import OperationalError

from tabsdatak.api import Secret
from tabsdatak.conn.common.types import UserPassword
from tabsdatak.conn.postgres import PostgresSrcConn
from tabsdatak.conn.postgres._explorer import PostgresConnectionExplorer
from tabsdatak.conn.common.sql._explorer import SQLEntity

# Share the serialization domain of the other ``fresh_postgres`` consumers:
# the fixture re-seeds the shared session container per test, so every
# test that touches it must run on one worker to avoid a concurrent
# re-seed corrupting a read in flight.
pytestmark = [
    pytest.mark.postgres,
    pytest.mark.xdist_group("postgres-plugin_integration_seq"),
]


def _src_conn_from(cfg) -> PostgresSrcConn:
    return PostgresSrcConn(uri=cfg.uri, credentials=cfg.credentials)


def _entity_by_table(entities: list[SQLEntity], table: str) -> SQLEntity:
    matches = [e for e in entities if e.table == table]
    assert matches, f"no entity for {table!r} in {[e.table for e in entities]}"
    return matches[0]


# ---------------------------------------------------------------------------
# check_connect
# ---------------------------------------------------------------------------


def test_check_connect_succeeds(fresh_postgres):
    conn = _src_conn_from(fresh_postgres)
    # Contract: returns None on success and must not raise.
    assert PostgresConnectionExplorer().check_connect(conn) is None


def test_check_connect_bad_credentials_raises(fresh_postgres):
    bad = PostgresSrcConn(
        uri=fresh_postgres.uri,
        credentials=UserPassword(
            user=Secret(fresh_postgres.user),
            password=Secret("definitely-wrong-password"),
        ),
    )
    with pytest.raises(OperationalError):
        PostgresConnectionExplorer().check_connect(bad)


# ---------------------------------------------------------------------------
# metadata
# ---------------------------------------------------------------------------


def test_metadata_lists_seeded_tables(fresh_postgres):
    conn = _src_conn_from(fresh_postgres)
    entities = PostgresConnectionExplorer().metadata(conn)

    assert entities and all(isinstance(e, SQLEntity) for e in entities)

    # Only ``public`` holds user tables; pg_catalog / information_schema /
    # pg_toast are skipped, so exactly the two seeded tables show up.
    assert {e.table for e in entities} == {"users", "orders"}

    # No `schema` override, so the base maps the iterated schema name
    # (``public``) into both `database` and `schema`.
    users = _entity_by_table(entities, "users")

    assert users.database == "testing"
    assert users.schema == "public"


def test_metadata_reflects_columns_and_primary_key(fresh_postgres):
    conn = _src_conn_from(fresh_postgres)
    users = _entity_by_table(PostgresConnectionExplorer().metadata(conn), "users")

    by_name = {c.name: c for c in users.columns}
    assert set(by_name) == {"id", "name", "age"}

    # `id` is the sole primary key; the others are not.
    assert by_name["id"].primary_key is True
    assert by_name["name"].primary_key is False
    assert by_name["age"].primary_key is False

    # Types are reflected and simplified to a short canonical name.
    assert by_name["id"].type.startswith("INT")
    assert by_name["name"].type == "VARCHAR"


# ---------------------------------------------------------------------------
# data
# ---------------------------------------------------------------------------


def test_data_returns_all_rows(fresh_postgres):
    conn = _src_conn_from(fresh_postgres)
    explorer = PostgresConnectionExplorer()
    users = _entity_by_table(explorer.metadata(conn), "users")

    rows = explorer.data(conn, users)

    assert isinstance(rows, list)
    assert len(rows) == 3
    assert all(set(r) == {"id", "name", "age"} for r in rows)
    assert {r["name"] for r in rows} == {"alice", "bob", "carol"}

    by_id = {r["id"]: r for r in rows}
    assert by_id[1]["name"] == "alice"
    assert by_id[1]["age"] == 30


def test_data_respects_limit(fresh_postgres):
    conn = _src_conn_from(fresh_postgres)
    explorer = PostgresConnectionExplorer()
    users = _entity_by_table(explorer.metadata(conn), "users")

    rows = explorer.data(conn, users, limit=2)
    assert len(rows) == 2
