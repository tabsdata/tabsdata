"""End-to-end ``MySQLConnectionExplorer`` tests against a real MySQL
instance.

The ``fresh_mysql`` fixture (defined in ``conftest.py``) provisions a
clean MySQL backend per test and seeds a ``testing`` database with a
``users`` table (``id`` PK, ``name``, ``age``) and an ``orders`` table
(``id`` PK, ``user_id``, ``qty``). These tests exercise the catalog
introspection surface of ``MySQLConnectionExplorer`` -- ``check_connect``
/ ``metadata`` / ``data`` -- against the live server. The three methods
are inherited from ``SqlAlchemyConnectionExplorer``; driving them through
``MySQLConnectionExplorer`` covers the MySQL-specific ``schema_filter`` /
``map_entity_fields`` / ``make_url`` overrides at the same time.
"""

from __future__ import annotations

import pytest
from sqlalchemy.exc import OperationalError

from tabsdatak.api import Secret
from tabsdatak.conn.common.types import UserPassword
from tabsdatak.conn.mysql import MySQLSrcConn
from tabsdatak.conn.mysql._explorer import MySQLConnectionExplorer
from tabsdatak.conn.common.sql._explorer import SQLEntity

# Share the serialization domain of the other ``fresh_mysql`` consumers:
# the fixture re-seeds the shared session container per test, so every
# test that touches it must run on one worker to avoid a concurrent
# re-seed corrupting a read in flight.
pytestmark = [
    pytest.mark.mysql,
    pytest.mark.xdist_group("mysql-plugin_integration_seq"),
]


def _src_conn_from(cfg) -> MySQLSrcConn:
    return MySQLSrcConn(uri=cfg.uri, credentials=cfg.credentials)


def _entity_by_table(entities: list[SQLEntity], table: str) -> SQLEntity:
    matches = [e for e in entities if e.table == table]
    assert matches, f"no entity for {table!r} in {[e.table for e in entities]}"
    return matches[0]


# ---------------------------------------------------------------------------
# check_connect
# ---------------------------------------------------------------------------


def test_check_connect_succeeds(fresh_mysql):
    conn = _src_conn_from(fresh_mysql)
    # Contract: returns None on success and must not raise.
    assert MySQLConnectionExplorer().check_connect(conn) is None


def test_check_connect_bad_credentials_raises(fresh_mysql):
    bad = MySQLSrcConn(
        uri=fresh_mysql.uri,
        credentials=UserPassword(
            user=Secret(fresh_mysql.user),
            password=Secret("definitely-wrong-password"),
        ),
    )
    with pytest.raises(OperationalError):
        MySQLConnectionExplorer().check_connect(bad)


# ---------------------------------------------------------------------------
# metadata
# ---------------------------------------------------------------------------


def test_metadata_lists_seeded_tables(fresh_mysql):
    conn = _src_conn_from(fresh_mysql)
    entities = MySQLConnectionExplorer().metadata(conn)

    assert entities and all(isinstance(e, SQLEntity) for e in entities)

    # The scan is restricted to the URI-path database, so only the two
    # seeded tables show up -- nothing from mysql/sys/information_schema.
    assert {e.table for e in entities} == {"users", "orders"}

    # MySQL maps the URI-path database into `database`; `schema` is blank.
    users = _entity_by_table(entities, "users")
    assert users.database == fresh_mysql.database
    assert users.schema == ""


def test_metadata_reflects_columns_and_primary_key(fresh_mysql):
    conn = _src_conn_from(fresh_mysql)
    users = _entity_by_table(MySQLConnectionExplorer().metadata(conn), "users")

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


def test_data_returns_all_rows(fresh_mysql):
    conn = _src_conn_from(fresh_mysql)
    explorer = MySQLConnectionExplorer()
    users = _entity_by_table(explorer.metadata(conn), "users")

    rows = explorer.data(conn, users)

    assert isinstance(rows, list)
    assert len(rows) == 3
    assert all(set(r) == {"id", "name", "age"} for r in rows)
    assert {r["name"] for r in rows} == {"alice", "bob", "carol"}

    by_id = {r["id"]: r for r in rows}
    assert by_id[1]["name"] == "alice"
    assert by_id[1]["age"] == 30


def test_data_respects_limit(fresh_mysql):
    conn = _src_conn_from(fresh_mysql)
    explorer = MySQLConnectionExplorer()
    users = _entity_by_table(explorer.metadata(conn), "users")

    rows = explorer.data(conn, users, limit=2)
    assert len(rows) == 2
