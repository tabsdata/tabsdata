#
# Copyright 2026 Tabsdata Inc.
#

"""End-to-end ``DatabricksConnectionExplorer`` tests against a real
Databricks workspace.

The ``databricks_world_xs`` domain (provisioned by tdx) seeds the configured
Unity Catalog schema with a ``wld_country`` table (``code``), a ``wld_city``
table (``id``, ``name``, ``country_code``, ``district``, ``population``) and a
``wld_country_language`` table (``country_code`` + ``language``). Databricks
treats PK/FK as informational and the seed DDL declares none, so no column
reflects as a primary key. These tests exercise the catalog introspection
surface of ``DatabricksConnectionExplorer`` -- ``check_connect`` /
``metadata`` / ``data`` -- against the live workspace: ``metadata`` maps
Unity Catalog's catalog -> schema -> table onto the shared SQL entity model's
database -> schema -> table, and ``data`` samples rows over a warehouse.
"""

from __future__ import annotations

import pytest
from databricks.sdk.errors import DatabricksError

from tabsdatak.api import Secret
from tabsdatak.conn.common.sql._explorer import SQLEntity
from tabsdatak.conn.databricks import DatabricksSrcConn
from tabsdatak.conn.databricks._explorer import DatabricksConnectionExplorer

from tabsdatax.itest.fixture.backends.databricks import (
    DatabricksDomain,
    DatabricksSystem,
    databricks_domain,
    databricks_system,
)

pytestmark = pytest.mark.databricks


def _conn_from(cfg: DatabricksDomain) -> DatabricksSrcConn:
    return DatabricksSrcConn(
        host_url=Secret(cfg.host),
        token=Secret(cfg.token),
        volume=Secret(cfg.volume),
        warehouse_id=Secret(cfg.warehouse_id),
        catalog=Secret(cfg.catalog),
        schema=Secret(cfg.schema),
    )


def _entity_by_table(entities: list[SQLEntity], table: str) -> SQLEntity:
    matches = [e for e in entities if e.table == table]
    assert matches, f"no entity for {table!r} in {[e.table for e in entities]}"
    return matches[0]


# ---------------------------------------------------------------------------
# check_connect
# ---------------------------------------------------------------------------


@databricks_system("databricks")
@databricks_domain("databricks_world_xs")
def test_check_connect_succeeds(
    system_databricks: DatabricksSystem,
    domain_databricks_world_xs: DatabricksDomain,
):
    conn = _conn_from(domain_databricks_world_xs)
    assert DatabricksConnectionExplorer().check_connect(conn) is None


@databricks_system("databricks")
@databricks_domain("databricks_world_xs")
def test_check_connect_bad_credentials_raises(
    system_databricks: DatabricksSystem,
    domain_databricks_world_xs: DatabricksDomain,
):
    cfg = domain_databricks_world_xs
    bad = DatabricksSrcConn(
        host_url=Secret(cfg.host),
        token=Secret("definitely-wrong-token"),
        volume=Secret(cfg.volume),
        warehouse_id=Secret(cfg.warehouse_id),
        catalog=Secret(cfg.catalog),
        schema=Secret(cfg.schema),
    )
    with pytest.raises(DatabricksError):
        DatabricksConnectionExplorer().check_connect(bad)


# ---------------------------------------------------------------------------
# metadata
# ---------------------------------------------------------------------------


@databricks_system("databricks")
@databricks_domain("databricks_world_xs")
def test_metadata_lists_seeded_tables(
    system_databricks: DatabricksSystem,
    domain_databricks_world_xs: DatabricksDomain,
):
    conn = _conn_from(domain_databricks_world_xs)
    entities = DatabricksConnectionExplorer().metadata(conn)

    assert entities and all(isinstance(e, SQLEntity) for e in entities)

    # The scan is pinned to the connection's catalog and restricted to the
    # configured schema, so only the three seeded tables show up.
    assert {e.table for e in entities} == {
        "wld_country",
        "wld_city",
        "wld_country_language",
    }

    # Unity Catalog's catalog -> schema maps onto the entity's
    # database -> schema.
    users = _entity_by_table(entities, "wld_country")
    assert users.database == domain_databricks_world_xs.catalog
    assert users.schema == domain_databricks_world_xs.schema


@databricks_system("databricks")
@databricks_domain("databricks_world_xs")
def test_metadata_reflects_columns(
    system_databricks: DatabricksSystem, domain_databricks_world_xs: DatabricksDomain
):
    conn = _conn_from(domain_databricks_world_xs)
    users = _entity_by_table(DatabricksConnectionExplorer().metadata(conn), "wld_city")

    by_name = {c.name: c for c in users.columns}
    assert set(by_name) == {"id", "name", "country_code", "district", "population"}

    # Unity Catalog exposes no primary-key flag, so no column reflects as one.
    assert all(c.primary_key is False for c in users.columns)

    # Columns reflect with the canonical Unity Catalog type name: STRING for
    # STRING, and BIGINT / INT reflect as LONG / INT respectively.
    assert by_name["name"].type == "STRING"
    assert by_name["country_code"].type == "STRING"
    assert by_name["district"].type == "STRING"
    assert by_name["id"].type == "LONG"
    assert by_name["population"].type == "INT"


# ---------------------------------------------------------------------------
# data
# ---------------------------------------------------------------------------


@databricks_system("databricks")
@databricks_domain("databricks_world_xs")
def test_data_returns_rows(
    system_databricks: DatabricksSystem,
    domain_databricks_world_xs: DatabricksDomain,
):
    conn = _conn_from(domain_databricks_world_xs)
    explorer = DatabricksConnectionExplorer()
    users = _entity_by_table(explorer.metadata(conn), "wld_city")

    rows = explorer.data(conn, users, limit=3)

    assert isinstance(rows, list)
    assert len(rows) == 3
    assert all(
        set(r) == {"id", "name", "country_code", "district", "population"} for r in rows
    )


@databricks_system("databricks")
@databricks_domain("databricks_world_xs")
def test_data_respects_limit(
    system_databricks: DatabricksSystem,
    domain_databricks_world_xs: DatabricksDomain,
):
    conn = _conn_from(domain_databricks_world_xs)
    explorer = DatabricksConnectionExplorer()
    users = _entity_by_table(explorer.metadata(conn), "wld_country_language")

    rows = explorer.data(conn, users, limit=2)
    assert len(rows) == 2
