#
#  Copyright 2026. Tabs Data Inc.
#

"""End-to-end ``StarRocksConnectionExplorer`` tests against a real
StarRocks instance.

The ``fresh_starrocks`` fixture (defined in ``conftest.py``) provisions a
clean StarRocks backend per test and seeds the ``testing`` database with a
``users`` table (``id``, ``name``, ``age``) and an ``orders`` table
(``id``, ``user_id``, ``qty``). Constructing a ``StarRocksDestConn`` also
needs a cloud staging area, so the tests pull in the shared ``s3_staging``
fixture (they skip when S3 staging isn't configured). These tests exercise
the catalog introspection surface of ``StarRocksConnectionExplorer`` --
``check_connect`` / ``metadata`` / ``data`` -- against the live server.

StarRocks speaks the MySQL wire protocol and has a single namespace level
(a *database*), so identifiers come back lower-case and the explorer maps
the database into ``database`` with an empty ``schema`` (like MySQL).
StarRocks' ``DUPLICATE KEY`` is a storage sort key, not a SQL primary key,
so the primary-key flag is intentionally not asserted here.
"""

from __future__ import annotations

import pytest
from sqlalchemy.exc import DBAPIError

from tabsdatak.api import Secret
from tabsdatak.conn.common.sql._explorer import SQLEntity
from tabsdatak.conn.common.types import UserPassword
from tabsdatak.conn.starrocks import StarRocksDestConn
from tabsdatak.conn.starrocks._explorer import StarRocksConnectionExplorer

# Share the serialization domain of the other ``fresh_starrocks`` consumers.
pytestmark = [
    pytest.mark.starrocks,
    pytest.mark.xdist_group("starrocks_seq"),
]


def _conn_from(cfg, staging) -> StarRocksDestConn:
    return StarRocksDestConn(
        host=cfg.host,
        port=cfg.port,
        database=cfg.database,
        staging=staging,
        credentials=cfg.credentials,
    )


def _entity_by_table(entities: list[SQLEntity], table: str) -> SQLEntity:
    matches = [e for e in entities if e.table == table]
    assert matches, f"no entity for {table!r} in {[e.table for e in entities]}"
    return matches[0]


# ---------------------------------------------------------------------------
# check_connect
# ---------------------------------------------------------------------------


def test_check_connect_succeeds(fresh_starrocks, s3_staging):
    conn = _conn_from(fresh_starrocks, s3_staging)
    assert StarRocksConnectionExplorer().check_connect(conn) is None


def test_check_connect_bad_credentials_raises(fresh_starrocks, s3_staging):
    bad = StarRocksDestConn(
        host=fresh_starrocks.host,
        port=fresh_starrocks.port,
        database=fresh_starrocks.database,
        staging=s3_staging,
        credentials=UserPassword(
            user=Secret(fresh_starrocks.user),
            password=Secret("definitely-wrong-password"),
        ),
    )
    with pytest.raises(DBAPIError):
        StarRocksConnectionExplorer().check_connect(bad)


# ---------------------------------------------------------------------------
# metadata
# ---------------------------------------------------------------------------


def test_metadata_lists_seeded_tables(fresh_starrocks, s3_staging):
    conn = _conn_from(fresh_starrocks, s3_staging)
    entities = StarRocksConnectionExplorer().metadata(conn)

    assert entities and all(isinstance(e, SQLEntity) for e in entities)

    # The scan is pinned to the configured database, so only the two
    # seeded tables show up (identifiers are lower-case).
    assert {e.table for e in entities} == {"users", "orders"}

    # StarRocks has no schema level: the database fills `database` and
    # `schema` is left blank.
    users = _entity_by_table(entities, "users")
    assert users.database == fresh_starrocks.database
    assert users.schema == ""


def test_metadata_reflects_columns(fresh_starrocks, s3_staging):
    conn = _conn_from(fresh_starrocks, s3_staging)
    users = _entity_by_table(StarRocksConnectionExplorer().metadata(conn), "users")

    by_name = {c.name: c for c in users.columns}
    assert set(by_name) == {"id", "name", "age"}
    assert by_name["id"].type.startswith("INT")


# ---------------------------------------------------------------------------
# data
# ---------------------------------------------------------------------------


def test_data_returns_all_rows(fresh_starrocks, s3_staging):
    conn = _conn_from(fresh_starrocks, s3_staging)
    explorer = StarRocksConnectionExplorer()
    users = _entity_by_table(explorer.metadata(conn), "users")

    rows = explorer.data(conn, users)

    assert isinstance(rows, list)
    assert len(rows) == 3
    assert all(set(r) == {"id", "name", "age"} for r in rows)
    assert {r["name"] for r in rows} == {"alice", "bob", "carol"}


def test_data_respects_limit(fresh_starrocks, s3_staging):
    conn = _conn_from(fresh_starrocks, s3_staging)
    explorer = StarRocksConnectionExplorer()
    users = _entity_by_table(explorer.metadata(conn), "users")

    rows = explorer.data(conn, users, limit=2)
    assert len(rows) == 2
