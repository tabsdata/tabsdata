#
#  Copyright 2026. Tabs Data Inc.
#

"""End-to-end ``OracleConnectionExplorer`` tests against a real Oracle
instance.

The ``fresh_oracle`` fixture (defined in ``conftest.py``) provisions a
clean Oracle backend per test and seeds the connecting user's schema
with a ``users`` table (``id`` PK, ``name``, ``age``) and an ``orders``
table (``id`` PK, ``user_id``, ``qty``). Oracle stores unquoted
identifiers upper-cased, but the SQLAlchemy oracledb dialect normalizes
those case-insensitive names back to lower-case on reflection -- so the
catalog reports tables ``users`` / ``orders`` with columns ``id`` /
``name`` / ``age``. The owner is passed through ``schema_filter`` as the
uppercased username (``ETL``). These
tests exercise the catalog introspection surface of
``OracleConnectionExplorer`` -- ``check_connect`` / ``metadata`` /
``data`` -- against the live server. The three methods are inherited
from ``SqlAlchemyConnectionExplorer``; driving them through
``OracleConnectionExplorer`` covers the Oracle-specific ``schema_filter``
/ ``system_schemas`` / ``make_url`` overrides at the same time.
"""

from __future__ import annotations

import pytest
from sqlalchemy.exc import DatabaseError

from tabsdatak.api import Secret
from tabsdatak.conn.common.types import UserPassword
from tabsdatak.conn.oracle import OracleSrcConn
from tabsdatak.conn.oracle._explorer import OracleConnectionExplorer
from tabsdatak.conn.common.sql._explorer import SQLEntity

# Share the serialization domain of the other ``fresh_oracle`` consumers:
# the fixture re-seeds the shared session container per test, so every
# test that touches it must run on one worker to avoid a concurrent
# re-seed corrupting a read in flight.
pytestmark = [
    pytest.mark.oracle,
    pytest.mark.xdist_group("oracle-plugin_integration_seq"),
]


def _src_conn_from(cfg) -> OracleSrcConn:
    return OracleSrcConn(uri=cfg.uri, credentials=cfg.credentials)


def _owner(cfg) -> str:
    # Oracle's schema/owner is the uppercased username.
    return cfg.user


def _entity_by_table(entities: list[SQLEntity], table: str) -> SQLEntity:
    matches = [e for e in entities if e.table == table]
    assert matches, f"no entity for {table!r} in {[e.table for e in entities]}"
    return matches[0]


# ---------------------------------------------------------------------------
# check_connect
# ---------------------------------------------------------------------------


def test_check_connect_succeeds(fresh_oracle):
    conn = _src_conn_from(fresh_oracle)
    # Contract: returns None on success and must not raise.
    assert OracleConnectionExplorer().check_connect(conn) is None


def test_check_connect_bad_credentials_raises(fresh_oracle):
    bad = OracleSrcConn(
        uri=fresh_oracle.uri,
        credentials=UserPassword(
            user=Secret(fresh_oracle.user),
            password=Secret("definitely-wrong-password"),
        ),
    )
    # Oracle surfaces a bad logon as ORA-01017 wrapped in DatabaseError.
    with pytest.raises(DatabaseError):
        OracleConnectionExplorer().check_connect(bad)


# ---------------------------------------------------------------------------
# metadata
# ---------------------------------------------------------------------------


def test_metadata_lists_seeded_tables(fresh_oracle):
    conn = _src_conn_from(fresh_oracle)
    entities = OracleConnectionExplorer().metadata(conn)

    assert entities and all(isinstance(e, SQLEntity) for e in entities)

    # The scan is restricted to the connecting user's own schema, so only
    # the two seeded tables show up (names normalized to lower-case).
    assert {e.table for e in entities} == {"users", "orders"}

    # No `map_entity_fields` override, so the base maps the iterated
    # schema name (the owner) into both `database` and `schema`.
    users = _entity_by_table(entities, "users")

    assert users.schema == _owner(fresh_oracle)


def test_metadata_reflects_columns_and_primary_key(fresh_oracle):
    conn = _src_conn_from(fresh_oracle)
    users = _entity_by_table(OracleConnectionExplorer().metadata(conn), "users")

    by_name = {c.name: c for c in users.columns}
    assert set(by_name) == {"id", "name", "age"}

    # `id` is the sole primary key; the others are not.
    assert by_name["id"].primary_key is True
    assert by_name["name"].primary_key is False
    assert by_name["age"].primary_key is False

    # Types are reflected and simplified to a short canonical name.
    # Oracle maps NUMBER(10) -> NUMBER and VARCHAR2(64) -> VARCHAR2.
    assert by_name["id"].type == "NUMBER"
    assert by_name["name"].type.startswith("VARCHAR")


# ---------------------------------------------------------------------------
# data
# ---------------------------------------------------------------------------


def test_data_returns_all_rows(fresh_oracle):
    conn = _src_conn_from(fresh_oracle)
    explorer = OracleConnectionExplorer()
    users = _entity_by_table(explorer.metadata(conn), "users")

    rows = explorer.data(conn, users)

    assert isinstance(rows, list)
    assert len(rows) == 3
    assert all(set(r) == {"id", "name", "age"} for r in rows)
    assert {r["name"] for r in rows} == {"alice", "bob", "carol"}

    by_id = {r["id"]: r for r in rows}
    assert by_id[1]["name"] == "alice"
    assert by_id[1]["age"] == 30


def test_data_respects_limit(fresh_oracle):
    conn = _src_conn_from(fresh_oracle)
    explorer = OracleConnectionExplorer()
    users = _entity_by_table(explorer.metadata(conn), "users")

    rows = explorer.data(conn, users, limit=2)
    assert len(rows) == 2
