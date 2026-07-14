#
#  Copyright 2026. Tabs Data Inc.
#

"""End-to-end ``OracleConnectionExplorer`` tests against a real Oracle
instance.

The ``oracle_world_xs`` domain (provisioned by tdx) seeds the connecting
user's schema with a ``wld_country`` table (``code`` PK), a ``wld_city``
table (``id`` PK, ``name``, ``country_code``, ``district``, ``population``)
and a ``wld_country_language`` table (``country_code`` + ``language`` PK).
Oracle stores unquoted identifiers upper-cased, but the SQLAlchemy oracledb
dialect normalizes those case-insensitive names back to lower-case on
reflection -- so the catalog reports lower-case tables and columns. The
owner is the uppercased username. These tests exercise the catalog
introspection surface of ``OracleConnectionExplorer`` -- ``check_connect``
/ ``metadata`` / ``data`` -- against the live server. The three methods are
inherited from ``SqlAlchemyConnectionExplorer``; driving them through
``OracleConnectionExplorer`` covers the Oracle-specific ``system_schemas``
/ ``map_entity_fields`` / ``make_url`` overrides at the same time.
"""

from __future__ import annotations

import pytest
from sqlalchemy.exc import DatabaseError
from sqlalchemy.engine import make_url

from tabsdatak.api import Secret
from tabsdatak.conn.common.types import UserPassword
from tabsdatak.conn.oracle import OracleSrcConn
from tabsdatak.conn.oracle._explorer import OracleConnectionExplorer
from tabsdatak.conn.common.sql._explorer import SQLEntity

from tabsdatax.itest.fixture.backends.oracle import (
    OracleDomain,
    OracleSystem,
    oracle_domain,
    oracle_system,
)

pytestmark = pytest.mark.oracle


def _clean_uri(uri: str) -> str:
    """Return a URI with the username and password stripped out, for use in
    a connection object that carries the credentials separately."""
    url = make_url(uri)
    return url.set(username=None, password=None).render_as_string(hide_password=False)


def _src_conn_from(cfg: OracleDomain) -> OracleSrcConn:
    clean_uri = _clean_uri(cfg.sqlalchemy_uri)
    return OracleSrcConn(
        uri=clean_uri,
        credentials=UserPassword(user=Secret(cfg.user), password=Secret(cfg.password)),
    )


def _entity_by_table(entities: list[SQLEntity], table: str) -> SQLEntity:
    matches = [e for e in entities if e.table == table]
    assert matches, f"no entity for {table!r} in {[e.table for e in entities]}"
    return matches[0]


# ---------------------------------------------------------------------------
# check_connect
# ---------------------------------------------------------------------------


@oracle_system("oracle")
@oracle_domain("oracle_world_xs")
def test_check_connect_succeeds(
    system_oracle: OracleSystem, domain_oracle_world_xs: OracleDomain
):
    conn = _src_conn_from(domain_oracle_world_xs)
    # Contract: returns None on success and must not raise.
    assert OracleConnectionExplorer().check_connect(conn) is None


@oracle_system("oracle")
@oracle_domain("oracle_world_xs")
def test_check_connect_bad_credentials_raises(
    system_oracle: OracleSystem, domain_oracle_world_xs: OracleDomain
):
    clean_uri = _clean_uri(domain_oracle_world_xs.sqlalchemy_uri)
    bad = OracleSrcConn(
        uri=clean_uri,
        credentials=UserPassword(
            user=Secret(domain_oracle_world_xs.user),
            password=Secret("definitely-wrong-password"),
        ),
    )
    # Oracle surfaces a bad logon as ORA-01017 wrapped in DatabaseError.
    with pytest.raises(DatabaseError):
        OracleConnectionExplorer().check_connect(bad)


# ---------------------------------------------------------------------------
# metadata
# ---------------------------------------------------------------------------


@oracle_system("oracle")
@oracle_domain("oracle_world_xs")
def test_metadata_lists_seeded_tables(
    system_oracle: OracleSystem, domain_oracle_world_xs: OracleDomain
):
    conn = _src_conn_from(domain_oracle_world_xs)
    entities = OracleConnectionExplorer().metadata(conn)

    assert entities and all(isinstance(e, SQLEntity) for e in entities)

    # Only the connecting user's schema holds tables (names normalized to
    # lower-case), so exactly the three seeded tables show up.
    assert {e.table for e in entities} == {
        "wld_country",
        "wld_city",
        "wld_country_language",
    }

    # `map_entity_fields` leaves `database` blank and maps the iterated
    # schema name (the owner) into `schema`.
    users = _entity_by_table(entities, "wld_country")
    assert users.schema == domain_oracle_world_xs.user


@oracle_system("oracle")
@oracle_domain("oracle_world_xs")
def test_metadata_reflects_columns_and_primary_key(
    system_oracle: OracleSystem,
    domain_oracle_world_xs: OracleDomain,
):
    conn = _src_conn_from(domain_oracle_world_xs)
    users = _entity_by_table(OracleConnectionExplorer().metadata(conn), "wld_city")

    by_name = {c.name: c for c in users.columns}
    assert set(by_name) == {"id", "name", "country_code", "district", "population"}

    # `id` is the sole primary key; the others are not.
    assert by_name["id"].primary_key is True
    assert by_name["name"].primary_key is False
    assert by_name["country_code"].primary_key is False
    assert by_name["district"].primary_key is False
    assert by_name["population"].primary_key is False

    # Types are reflected and simplified to a short canonical name.
    # Oracle maps NUMBER(10) -> NUMBER and VARCHAR2(35) -> VARCHAR2.
    assert by_name["id"].type == "NUMBER"
    assert by_name["name"].type.startswith("VARCHAR")
    assert by_name["country_code"].type == "CHAR"
    assert by_name["district"].type.startswith("VARCHAR")
    assert by_name["population"].type == "NUMBER"


# ---------------------------------------------------------------------------
# data
# ---------------------------------------------------------------------------


@oracle_system("oracle")
@oracle_domain("oracle_world_xs")
def test_data_returns_all_rows(
    system_oracle: OracleSystem, domain_oracle_world_xs: OracleDomain
):
    conn = _src_conn_from(domain_oracle_world_xs)
    explorer = OracleConnectionExplorer()
    users = _entity_by_table(explorer.metadata(conn), "wld_city")

    rows = explorer.data(conn, users, limit=3)

    assert isinstance(rows, list)
    assert len(rows) == 3
    assert all(
        set(r) == {"id", "name", "country_code", "district", "population"} for r in rows
    )
    assert {r["name"] for r in rows} == {"Kabul", "Qandahar", "Herat"}

    by_id = {r["id"]: r for r in rows}
    assert by_id[1]["name"] == "Kabul"
    assert by_id[1]["country_code"] == "AFG"
    assert by_id[1]["district"] == "Kabol"
    assert by_id[1]["population"] == 1780000


@oracle_system("oracle")
@oracle_domain("oracle_world_xs")
def test_data_respects_limit(
    system_oracle: OracleSystem, domain_oracle_world_xs: OracleDomain
):
    conn = _src_conn_from(domain_oracle_world_xs)
    explorer = OracleConnectionExplorer()
    users = _entity_by_table(explorer.metadata(conn), "wld_country_language")

    rows = explorer.data(conn, users, limit=2)
    assert len(rows) == 2
