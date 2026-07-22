#
#  Copyright 2026 Tabsdata Inc.
#

"""End-to-end ``SnowflakeConnectionExplorer`` tests against a real
Snowflake account.

The ``snowflake_world_xs`` domain (provisioned by tdx) seeds the configured
schema with a ``wld_country`` table (``code``), a ``wld_city`` table (``id``,
``name``, ``country_code``, ``district``, ``population``) and a
``wld_country_language`` table (``country_code`` + ``language``). Snowflake
treats PK/FK as informational and the seed DDL declares none, so unlike the
container backends there are no reflected primary keys. These tests exercise
the catalog introspection surface of ``SnowflakeConnectionExplorer`` --
``check_connect`` / ``metadata`` / ``data`` -- against the live account; the
three methods are inherited from ``SqlAlchemyConnectionExplorer`` and driven
through the Snowflake-specific ``schema_filter`` / ``system_schemas`` /
``make_url`` overrides.
"""

from __future__ import annotations

import pytest
from sqlalchemy.exc import DatabaseError

from tabsdatak.api import Secret
from tabsdatak.conn.common.sql._explorer import SQLEntity
from tabsdatak.conn.common.types import UserPassword
from tabsdatak.conn.snowflake import SnowflakeDestConn
from tabsdatak.conn.snowflake._explorer import SnowflakeConnectionExplorer

from tabsdatax.itest.fixture.backends.snowflake import (
    SnowflakeDomain,
    SnowflakeSystem,
    snowflake_domain,
    snowflake_system,
)

pytestmark = [
    pytest.mark.snowflake,
    pytest.mark.snowflake_live,
]


def _conn_from(cfg: SnowflakeDomain) -> SnowflakeDestConn:
    return SnowflakeDestConn(
        account=cfg.account,
        credentials=UserPassword(user=Secret(cfg.username), password=Secret(cfg.pat)),
        database=cfg.database,
        schema=cfg.schema,
        warehouse=cfg.warehouse,
    )


def _entity_by_table(entities: list[SQLEntity], table: str) -> SQLEntity:
    matches = [e for e in entities if e.table == table]
    assert matches, f"no entity for {table!r} in {[e.table for e in entities]}"
    return matches[0]


# ---------------------------------------------------------------------------
# check_connect
# ---------------------------------------------------------------------------


@snowflake_system("snowflake")
@snowflake_domain("snowflake_world_xs")
def test_check_connect_succeeds(
    system_snowflake: SnowflakeSystem, domain_snowflake_world_xs: SnowflakeDomain
):
    conn = _conn_from(domain_snowflake_world_xs)
    assert SnowflakeConnectionExplorer().check_connect(conn) is None


@snowflake_system("snowflake")
@snowflake_domain("snowflake_world_xs")
def test_check_connect_bad_credentials_raises(
    system_snowflake: SnowflakeSystem, domain_snowflake_world_xs: SnowflakeDomain
):
    cfg = domain_snowflake_world_xs
    bad = SnowflakeDestConn(
        account=cfg.account,
        credentials=UserPassword(
            user=Secret(cfg.username),
            password=Secret("definitely-wrong-password"),
        ),
        database=cfg.database,
        schema=cfg.schema,
        warehouse=cfg.warehouse,
    )
    with pytest.raises(DatabaseError):
        SnowflakeConnectionExplorer().check_connect(bad)


# ---------------------------------------------------------------------------
# metadata
# ---------------------------------------------------------------------------


@snowflake_system("snowflake")
@snowflake_domain("snowflake_world_xs")
def test_metadata_lists_seeded_tables(
    system_snowflake: SnowflakeSystem, domain_snowflake_world_xs: SnowflakeDomain
):
    conn = _conn_from(domain_snowflake_world_xs)
    entities = SnowflakeConnectionExplorer().metadata(conn)

    assert entities and all(isinstance(e, SQLEntity) for e in entities)

    # The scan is restricted to the configured schema, so only the three
    # seeded tables show up.
    assert {e.table for e in entities} == {
        "wld_country",
        "wld_city",
        "wld_country_language",
    }

    # No `map_entity_fields` override, so the base maps the iterated schema
    # name into both `database` and `schema`.
    users = _entity_by_table(entities, "wld_country")
    assert users.schema == conn.schema


@snowflake_system("snowflake")
@snowflake_domain("snowflake_world_xs")
def test_metadata_reflects_columns(
    system_snowflake: SnowflakeSystem, domain_snowflake_world_xs: SnowflakeDomain
):
    conn = _conn_from(domain_snowflake_world_xs)
    users = _entity_by_table(SnowflakeConnectionExplorer().metadata(conn), "wld_city")

    by_name = {c.name: c for c in users.columns}
    assert set(by_name) == {"id", "name", "country_code", "district", "population"}

    # Snowflake PK/FK are informational and the seed DDL declares none, so no
    # column reflects as a primary key.
    assert all(c.primary_key is False for c in users.columns)

    # STRING columns reflect as VARCHAR.
    assert by_name["name"].type == "VARCHAR"
    assert by_name["country_code"].type == "VARCHAR"
    assert by_name["district"].type == "VARCHAR"


# ---------------------------------------------------------------------------
# data
# ---------------------------------------------------------------------------


@snowflake_system("snowflake")
@snowflake_domain("snowflake_world_xs")
def test_data_returns_rows(
    system_snowflake: SnowflakeSystem, domain_snowflake_world_xs: SnowflakeDomain
):
    conn = _conn_from(domain_snowflake_world_xs)
    explorer = SnowflakeConnectionExplorer()
    users = _entity_by_table(explorer.metadata(conn), "wld_city")

    rows = explorer.data(conn, users, limit=3)

    assert isinstance(rows, list)
    assert len(rows) == 3
    assert all(
        set(r) == {"id", "name", "country_code", "district", "population"} for r in rows
    )


@snowflake_system("snowflake")
@snowflake_domain("snowflake_world_xs")
def test_data_respects_limit(
    system_snowflake: SnowflakeSystem, domain_snowflake_world_xs: SnowflakeDomain
):
    conn = _conn_from(domain_snowflake_world_xs)
    explorer = SnowflakeConnectionExplorer()
    users = _entity_by_table(explorer.metadata(conn), "wld_country_language")

    rows = explorer.data(conn, users, limit=2)
    assert len(rows) == 2
