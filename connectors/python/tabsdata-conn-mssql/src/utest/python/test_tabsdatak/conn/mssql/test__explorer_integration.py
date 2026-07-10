#
#  Copyright 2026. Tabs Data Inc.
#

"""End-to-end ``MSSQLServerConnectionExplorer`` tests against a real SQL
Server instance.

The ``fresh_mssql`` fixture (defined in ``conftest.py``) provisions a
clean SQL Server backend per test and seeds a ``testing`` database with a
``users`` table (``id`` PK, ``name``, ``age``) and an ``orders`` table
(``id`` PK, ``user_id``, ``qty``) in the default ``dbo`` schema. These
tests exercise the catalog introspection surface of
``MSSQLServerConnectionExplorer`` -- ``check_connect`` / ``metadata`` /
``data`` -- against the live server. The three methods are inherited
from ``SqlAlchemyConnectionExplorer``; driving them through
``MSSQLServerConnectionExplorer`` covers the SQL-Server-specific
``system_schemas`` / ``make_url`` overrides at the same time.
"""

from __future__ import annotations

import pytest
from sqlalchemy.exc import DBAPIError

from tabsdatak.api import Secret
from tabsdatak.conn.common.types import UserPassword
from tabsdatak.conn.mssql import MSSQLSrcConn
from tabsdatak.conn.mssql._explorer import MSSQLServerConnectionExplorer
from tabsdatak.conn.common.sql._explorer import SQLEntity

# Share the serialization domain of the other ``fresh_mssql`` consumers:
# the fixture re-seeds the shared session container per test, so every
# test that touches it must run on one worker to avoid a concurrent
# re-seed corrupting a read in flight.
pytestmark = [
    pytest.mark.mssql,
    pytest.mark.xdist_group("mssql-plugin_integration_seq"),
]


def _src_conn_from(cfg) -> MSSQLSrcConn:
    return MSSQLSrcConn(uri=cfg.uri, credentials=cfg.credentials)


def _odbc_src_conn_from(cfg) -> MSSQLSrcConn:
    # Same backend, but the ``uri`` is a raw ODBC connection string (no
    # ``://`` scheme) rather than a ``mssql://`` URI -- exercising the
    # explorer's ``odbc_connect`` / connect_args path.
    return MSSQLSrcConn(uri=cfg.odbc_connection_string, credentials=cfg.credentials)


def _entity_by_table(entities: list[SQLEntity], table: str) -> SQLEntity:
    matches = [e for e in entities if e.table == table]
    assert matches, f"no entity for {table!r} in {[e.table for e in entities]}"
    return matches[0]


# ---------------------------------------------------------------------------
# check_connect
# ---------------------------------------------------------------------------


def test_check_connect_succeeds(fresh_mssql):
    conn = _src_conn_from(fresh_mssql)
    # Contract: returns None on success and must not raise.
    assert MSSQLServerConnectionExplorer().check_connect(conn) is None


def test_check_connect_bad_credentials_raises(fresh_mssql):
    bad = MSSQLSrcConn(
        uri=fresh_mssql.uri,
        credentials=UserPassword(
            user=Secret(fresh_mssql.user),
            password=Secret("definitely-wrong-password"),
        ),
    )
    # A failed SQL Server logon surfaces as a pyodbc error wrapped by
    # SQLAlchemy (DBAPIError is the common base of those).
    with pytest.raises(DBAPIError):
        MSSQLServerConnectionExplorer().check_connect(bad)


# ---------------------------------------------------------------------------
# metadata
# ---------------------------------------------------------------------------


def test_metadata_lists_seeded_tables(fresh_mssql):
    conn = _src_conn_from(fresh_mssql)
    entities = MSSQLServerConnectionExplorer().metadata(conn)

    assert entities and all(isinstance(e, SQLEntity) for e in entities)

    # Only ``dbo`` holds user tables; sys / INFORMATION_SCHEMA and the
    # fixed database roles are skipped, so exactly the two seeded tables
    # show up.
    assert {e.table for e in entities} == {"users", "orders"}

    # No `map_entity_fields` override, so the base maps the iterated
    # schema name (``dbo``) into both `database` and `schema`.
    users = _entity_by_table(entities, "users")
    assert users.database == "dbo"
    assert users.schema == "dbo"


def test_metadata_reflects_columns_and_primary_key(fresh_mssql):
    conn = _src_conn_from(fresh_mssql)
    users = _entity_by_table(MSSQLServerConnectionExplorer().metadata(conn), "users")

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


def test_data_returns_all_rows(fresh_mssql):
    conn = _src_conn_from(fresh_mssql)
    explorer = MSSQLServerConnectionExplorer()
    users = _entity_by_table(explorer.metadata(conn), "users")

    rows = explorer.data(conn, users)

    assert isinstance(rows, list)
    assert len(rows) == 3
    assert all(set(r) == {"id", "name", "age"} for r in rows)
    assert {r["name"] for r in rows} == {"alice", "bob", "carol"}

    by_id = {r["id"]: r for r in rows}
    assert by_id[1]["name"] == "alice"
    assert by_id[1]["age"] == 30


def test_data_respects_limit(fresh_mssql):
    conn = _src_conn_from(fresh_mssql)
    explorer = MSSQLServerConnectionExplorer()
    users = _entity_by_table(explorer.metadata(conn), "users")

    rows = explorer.data(conn, users, limit=2)
    assert len(rows) == 2


# ---------------------------------------------------------------------------
# ODBC connection string `uri` form
#
# The conn's `uri` may be a raw ODBC connection string instead of a
# `mssql://` URI; the explorer wraps it as ``odbc_connect`` and passes
# UID / PWD via connect_args. These confirm that path connects and
# introspects end-to-end against the live server.
# ---------------------------------------------------------------------------


def test_check_connect_via_odbc_connection_string(fresh_mssql):
    conn = _odbc_src_conn_from(fresh_mssql)
    assert MSSQLServerConnectionExplorer().check_connect(conn) is None


def test_metadata_and_data_via_odbc_connection_string(fresh_mssql):
    conn = _odbc_src_conn_from(fresh_mssql)
    explorer = MSSQLServerConnectionExplorer()

    entities = explorer.metadata(conn)
    assert {e.table for e in entities} == {"users", "orders"}

    users = _entity_by_table(entities, "users")
    assert {c.name for c in users.columns} == {"id", "name", "age"}

    rows = explorer.data(conn, users)
    assert len(rows) == 3
    assert {r["name"] for r in rows} == {"alice", "bob", "carol"}
