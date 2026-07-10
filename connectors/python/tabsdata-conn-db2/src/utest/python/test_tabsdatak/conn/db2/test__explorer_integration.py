"""End-to-end ``Db2ConnectionExplorer`` tests against a real Db2 instance.

The ``fresh_db2`` fixture (defined in ``conftest.py``) provisions a clean
Db2 backend per test and seeds the connecting user's schema with a
``users`` table (``id`` PK, ``name``, ``age``) and an ``orders`` table
(``id`` PK, ``user_id``, ``qty``). Db2 stores unquoted identifiers
upper-cased, but the ibm_db_sa dialect normalizes them back to lower-case
on reflection, so the catalog reports tables ``users`` / ``orders`` with
columns ``id`` / ``name`` / ``age``. The owner is passed through
``schema_filter`` as the uppercased username (``DB2INST1``). These tests
exercise the catalog
introspection surface of ``Db2ConnectionExplorer`` -- ``check_connect`` /
``metadata`` / ``data`` -- against the live server; the three methods are
inherited from ``SqlAlchemyConnectionExplorer`` and driven through the
Db2-specific ``schema_filter`` / ``system_schemas`` / ``make_url``
overrides.
"""

from __future__ import annotations

import pytest
from sqlalchemy.exc import DBAPIError

from tabsdatak.api import Secret
from tabsdatak.conn.common.sql._explorer import SQLEntity
from tabsdatak.conn.common.types import UserPassword
from tabsdatak.conn.db2 import Db2SrcConn
from tabsdatak.conn.db2._explorer import Db2ConnectionExplorer

# Share the serialization domain of the other ``fresh_db2`` consumers: the
# fixture re-seeds the shared session container per test, so every test
# that touches it must run on one worker to avoid a concurrent re-seed
# corrupting a read in flight.
pytestmark = [
    pytest.mark.db2,
    pytest.mark.xdist_group("db2_seq"),
]


def _src_conn_from(cfg) -> Db2SrcConn:
    return Db2SrcConn(uri=cfg.uri, credentials=cfg.credentials)


def _owner(cfg) -> str:
    # Db2's schema/owner is the uppercased username.
    return cfg.user.upper()


def _entity_by_table(entities: list[SQLEntity], table: str) -> SQLEntity:
    matches = [e for e in entities if e.table == table]
    assert matches, f"no entity for {table!r} in {[e.table for e in entities]}"
    return matches[0]


# ---------------------------------------------------------------------------
# check_connect
# ---------------------------------------------------------------------------


def test_check_connect_succeeds(fresh_db2):
    conn = _src_conn_from(fresh_db2)
    assert Db2ConnectionExplorer().check_connect(conn) is None


def test_check_connect_bad_credentials_raises(fresh_db2):
    bad = Db2SrcConn(
        uri=fresh_db2.uri,
        credentials=UserPassword(
            user=Secret(fresh_db2.user),
            password=Secret("definitely-wrong-password"),
        ),
    )
    with pytest.raises(DBAPIError):
        Db2ConnectionExplorer().check_connect(bad)


# ---------------------------------------------------------------------------
# metadata
# ---------------------------------------------------------------------------


def test_metadata_lists_seeded_tables(fresh_db2):
    conn = _src_conn_from(fresh_db2)
    entities = Db2ConnectionExplorer().metadata(conn)

    assert entities and all(isinstance(e, SQLEntity) for e in entities)

    # The scan is restricted to the connecting user's own schema, so only
    # the two seeded tables show up (names normalized to lower-case).
    assert {e.table for e in entities} == {"users", "orders"}

    # No `map_entity_fields` override, so the base maps the iterated
    # schema name (the owner) into both `database` and `schema`.
    users = _entity_by_table(entities, "users")
    assert users.database == _owner(fresh_db2)
    assert users.schema == _owner(fresh_db2)


def test_metadata_reflects_columns_and_primary_key(fresh_db2):
    conn = _src_conn_from(fresh_db2)
    users = _entity_by_table(Db2ConnectionExplorer().metadata(conn), "users")

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


def test_data_returns_all_rows(fresh_db2):
    conn = _src_conn_from(fresh_db2)
    explorer = Db2ConnectionExplorer()
    users = _entity_by_table(explorer.metadata(conn), "users")

    rows = explorer.data(conn, users)

    assert isinstance(rows, list)
    assert len(rows) == 3
    assert all(set(r) == {"id", "name", "age"} for r in rows)
    assert {r["name"] for r in rows} == {"alice", "bob", "carol"}

    by_id = {r["id"]: r for r in rows}
    assert by_id[1]["name"] == "alice"
    assert by_id[1]["age"] == 30


def test_data_respects_limit(fresh_db2):
    conn = _src_conn_from(fresh_db2)
    explorer = Db2ConnectionExplorer()
    users = _entity_by_table(explorer.metadata(conn), "users")

    rows = explorer.data(conn, users, limit=2)
    assert len(rows) == 2
