"""End-to-end ``Db2ConnectionExplorer`` tests against a real Db2 instance.

The ``db2_world_xs`` domain (provisioned by tdx) seeds the connecting user's
schema with a ``wld_country`` table (``code`` PK), a ``wld_city`` table
(``id`` PK, ``name``, ``country_code``, ``district``, ``population``) and a
``wld_country_language`` table (``country_code`` + ``language`` PK). Db2
stores unquoted identifiers upper-cased, but the ibm_db_sa dialect normalizes
them back to lower-case on reflection, so the catalog reports lower-case
tables and columns. The owner is passed through ``schema_filter`` as the
uppercased username. These tests exercise the catalog introspection surface
of ``Db2ConnectionExplorer`` -- ``check_connect`` / ``metadata`` / ``data`` --
against the live server; the three methods are inherited from
``SqlAlchemyConnectionExplorer`` and driven through the Db2-specific
``schema_filter`` / ``system_schemas`` / ``make_url`` overrides.
"""

from __future__ import annotations

import pytest
from sqlalchemy.exc import DBAPIError
from sqlalchemy.engine import make_url

from tabsdatak.api import Secret
from tabsdatak.conn.common.sql._explorer import SQLEntity
from tabsdatak.conn.common.types import UserPassword
from tabsdatak.conn.db2 import Db2SrcConn
from tabsdatak.conn.db2._explorer import Db2ConnectionExplorer

from tabsdatax.itest.fixture.backends.db2 import (
    Db2Domain,
    Db2System,
    db2_domain,
    db2_system,
)

pytestmark = pytest.mark.db2


def _clean_uri(uri: str) -> str:
    """Return a URI with the username and password stripped out, for use in
    a connection object that carries the credentials separately."""
    url = make_url(uri)
    return url.set(username=None, password=None).render_as_string(hide_password=False)


def _src_conn_from(cfg: Db2Domain) -> Db2SrcConn:
    clean_uri = _clean_uri(cfg.sqlalchemy_uri)
    return Db2SrcConn(
        uri=clean_uri,
        credentials=UserPassword(user=Secret(cfg.user), password=Secret(cfg.password)),
    )


def _owner(cfg: Db2Domain) -> str:
    # Db2's schema/owner is the uppercased username.
    return cfg.user.upper()


def _entity_by_table(entities: list[SQLEntity], table: str) -> SQLEntity:
    matches = [e for e in entities if e.table == table]
    assert matches, f"no entity for {table!r} in {[e.table for e in entities]}"
    return matches[0]


# ---------------------------------------------------------------------------
# check_connect
# ---------------------------------------------------------------------------


@db2_system("db2")
@db2_domain("db2_world_xs")
def test_check_connect_succeeds(system_db2: Db2System, domain_db2_world_xs: Db2Domain):
    conn = _src_conn_from(domain_db2_world_xs)
    assert Db2ConnectionExplorer().check_connect(conn) is None


@db2_system("db2")
@db2_domain("db2_world_xs")
def test_check_connect_bad_credentials_raises(
    system_db2: Db2System, domain_db2_world_xs: Db2Domain
):
    clean_uri = _clean_uri(domain_db2_world_xs.sqlalchemy_uri)
    bad = Db2SrcConn(
        uri=clean_uri,
        credentials=UserPassword(
            user=Secret(domain_db2_world_xs.user),
            password=Secret("definitely-wrong-password"),
        ),
    )
    with pytest.raises(DBAPIError):
        Db2ConnectionExplorer().check_connect(bad)


# ---------------------------------------------------------------------------
# metadata
# ---------------------------------------------------------------------------


@db2_system("db2")
@db2_domain("db2_world_xs")
def test_metadata_lists_seeded_tables(
    system_db2: Db2System, domain_db2_world_xs: Db2Domain
):
    conn = _src_conn_from(domain_db2_world_xs)
    entities = Db2ConnectionExplorer().metadata(conn)

    assert entities and all(isinstance(e, SQLEntity) for e in entities)

    # The scan is restricted to the connecting user's own schema, so only the
    # three seeded tables show up (names normalized to lower-case).
    assert {e.table for e in entities} == {
        "wld_country",
        "wld_city",
        "wld_country_language",
    }

    # No `map_entity_fields` override, so the base maps the iterated schema
    # name (the owner) into both `database` and `schema`.
    users = _entity_by_table(entities, "wld_country")
    assert users.database == domain_db2_world_xs.db.upper()
    assert users.schema == domain_db2_world_xs.schema.upper()


@db2_system("db2")
@db2_domain("db2_world_xs")
def test_metadata_reflects_columns_and_primary_key(
    system_db2: Db2System, domain_db2_world_xs: Db2Domain
):
    conn = _src_conn_from(domain_db2_world_xs)
    users = _entity_by_table(Db2ConnectionExplorer().metadata(conn), "wld_city")

    by_name = {c.name: c for c in users.columns}
    assert set(by_name) == {"id", "name", "country_code", "district", "population"}

    # `id` is the sole primary key; the others are not.
    assert by_name["id"].primary_key is True
    assert by_name["name"].primary_key is False
    assert by_name["country_code"].primary_key is False
    assert by_name["district"].primary_key is False
    assert by_name["population"].primary_key is False

    # Types are reflected and simplified to a short canonical name.
    assert by_name["id"].type.startswith("INT")
    assert by_name["name"].type == "VARCHAR"
    assert by_name["country_code"].type == "CHAR"
    assert by_name["district"].type == "VARCHAR"
    assert by_name["population"].type.startswith("INT")


# ---------------------------------------------------------------------------
# data
# ---------------------------------------------------------------------------


@db2_system("db2")
@db2_domain("db2_world_xs")
def test_data_returns_all_rows(system_db2: Db2System, domain_db2_world_xs: Db2Domain):
    conn = _src_conn_from(domain_db2_world_xs)
    explorer = Db2ConnectionExplorer()
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


@db2_system("db2")
@db2_domain("db2_world_xs")
def test_data_respects_limit(system_db2: Db2System, domain_db2_world_xs: Db2Domain):
    conn = _src_conn_from(domain_db2_world_xs)
    explorer = Db2ConnectionExplorer()
    users = _entity_by_table(explorer.metadata(conn), "wld_country_language")

    rows = explorer.data(conn, users, limit=2)
    assert len(rows) == 2
