#
#  Copyright 2026 Tabsdata Inc.
#

"""End-to-end ``ClickHouseConnectionExplorer`` tests against a real
ClickHouse instance.

The ``clickhouse_world_xs`` domain (provisioned by tdx) seeds the database
with a ``country`` table, a ``city`` table (``id``, ``name``,
``country_code``, ``district``, ``population``) and a
``country_language`` table. These tests exercise the catalog
introspection surface of ``ClickHouseConnectionExplorer`` --
``check_connect`` / ``metadata`` / ``data`` -- against the live server.

"""

from __future__ import annotations

import pytest

from tabsdatak.api import Secret
from tabsdatak.conn.clickhouse import ClickHouseDestConn, S3Location
from tabsdatak.conn.clickhouse._explorer import ClickHouseConnectionExplorer
from tabsdatak.conn.common.sql._explorer import SQLEntity
from tabsdatak.conn.common.types import AwsAccessSecretKey, UserPassword

from tabsdatax.itest.fixture.backends.clickhouse import (
    ClickhouseDomain,
    ClickhouseSystem,
    clickhouse_domain,
    clickhouse_system,
)

pytestmark = pytest.mark.clickhouse

# The explorer only reads the catalog over clickhouse-connect; the staging
# area is never used, so a placeholder is enough to construct the conn.
_STAGING = S3Location(
    bucket="unused",
    region="us-east-1",
    credentials=AwsAccessSecretKey(
        access_key_id=Secret("unused"), secret_access_key=Secret("unused")
    ),
)


def _conn_from(cfg: ClickhouseDomain) -> ClickHouseDestConn:
    return ClickHouseDestConn(
        host=cfg.host,
        port=cfg.port,
        database=cfg.database,
        staging=_STAGING,
        secure="false",
        credentials=UserPassword(user=Secret(cfg.user), password=Secret(cfg.password)),
    )


def _entity_by_table(entities: list[SQLEntity], table: str) -> SQLEntity:
    matches = [e for e in entities if e.table == table]
    assert matches, f"no entity for {table!r} in {[e.table for e in entities]}"
    return matches[0]


# ---------------------------------------------------------------------------
# check_connect
# ---------------------------------------------------------------------------


@clickhouse_system("clickhouse")
@clickhouse_domain("clickhouse_world_xs")
def test_check_connect_succeeds(
    system_clickhouse: ClickhouseSystem, domain_clickhouse_world_xs: ClickhouseDomain
):
    conn = _conn_from(domain_clickhouse_world_xs)
    assert ClickHouseConnectionExplorer().check_connect(conn) is None


# ---------------------------------------------------------------------------
# metadata
# ---------------------------------------------------------------------------


@clickhouse_system("clickhouse")
@clickhouse_domain("clickhouse_world_xs")
def test_metadata_lists_seeded_tables(
    system_clickhouse: ClickhouseSystem, domain_clickhouse_world_xs: ClickhouseDomain
):
    conn = _conn_from(domain_clickhouse_world_xs)
    entities = ClickHouseConnectionExplorer().metadata(conn)

    assert entities and all(isinstance(e, SQLEntity) for e in entities)

    # The scan is pinned to the configured database, so only the three seeded
    # tables show up.
    assert {e.table for e in entities} == {
        "country",
        "city",
        "country_language",
    }

    # ClickHouse has no schema level: the database fills `database` and
    # `schema` is left blank.
    users = _entity_by_table(entities, "country")
    assert users.database == domain_clickhouse_world_xs.database
    assert users.schema == ""


@clickhouse_system("clickhouse")
@clickhouse_domain("clickhouse_world_xs")
def test_metadata_reflects_columns(
    system_clickhouse: ClickhouseSystem, domain_clickhouse_world_xs: ClickhouseDomain
):
    conn = _conn_from(domain_clickhouse_world_xs)
    users = _entity_by_table(ClickHouseConnectionExplorer().metadata(conn), "city")

    by_name = {c.name: c for c in users.columns}
    assert set(by_name) == {"id", "name", "country_code", "district", "population"}

    # Types keep ClickHouse's native spelling (Nullable(...) unwrapped and any
    # parameters dropped by the explorer).
    assert by_name["id"].type == "Int32"
    assert by_name["name"].type == "String"
    assert by_name["country_code"].type == "String"
    assert by_name["district"].type == "String"
    assert by_name["population"].type == "Int32"


# ---------------------------------------------------------------------------
# data
# ---------------------------------------------------------------------------


@clickhouse_system("clickhouse")
@clickhouse_domain("clickhouse_world_xs")
def test_data_returns_rows(
    system_clickhouse: ClickhouseSystem, domain_clickhouse_world_xs: ClickhouseDomain
):
    conn = _conn_from(domain_clickhouse_world_xs)
    explorer = ClickHouseConnectionExplorer()
    users = _entity_by_table(explorer.metadata(conn), "city")

    rows = explorer.data(conn, users, limit=3)

    assert isinstance(rows, list)
    assert len(rows) == 3
    assert all(
        set(r) == {"id", "name", "country_code", "district", "population"} for r in rows
    )


@clickhouse_system("clickhouse")
@clickhouse_domain("clickhouse_world_xs")
def test_data_respects_limit(
    system_clickhouse: ClickhouseSystem, domain_clickhouse_world_xs: ClickhouseDomain
):
    conn = _conn_from(domain_clickhouse_world_xs)
    explorer = ClickHouseConnectionExplorer()
    users = _entity_by_table(explorer.metadata(conn), "country_language")

    rows = explorer.data(conn, users, limit=2)
    assert len(rows) == 2
