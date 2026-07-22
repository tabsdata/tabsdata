#
#  Copyright 2026 Tabsdata Inc.
#

"""End-to-end ``MSSQLServerConnectionExplorer`` tests against a real SQL
Server instance.

The ``mssqlserver_world_xs`` domain (provisioned by tdx) seeds a
``wld_country`` table (``code`` PK), a ``wld_city`` table (``id`` PK,
``name``, ``country_code``, ``district``, ``population``) and a
``wld_country_language`` table (``country_code`` + ``language`` PK) in the
default ``dbo`` schema. These tests exercise the catalog introspection
surface of ``MSSQLServerConnectionExplorer`` -- ``check_connect`` /
``metadata`` / ``data`` -- against the live server. The three methods are
inherited from ``SqlAlchemyConnectionExplorer``; driving them through
``MSSQLServerConnectionExplorer`` covers the SQL-Server-specific
``system_schemas`` / ``make_url`` overrides at the same time.
"""

from __future__ import annotations

import pytest
from sqlalchemy.exc import DBAPIError
from sqlalchemy.engine import make_url

from tabsdatak.api import Secret
from tabsdatak.conn.common.types import UserPassword
from tabsdatak.conn.mssql import MSSQLSrcConn
from tabsdatak.conn.mssql._explorer import MSSQLServerConnectionExplorer
from tabsdatak.conn.common.sql._explorer import SQLEntity

from tabsdatax.itest.fixture.backends.mssqlserver import (
    MssqlserverDomain,
    MssqlserverSystem,
    mssqlserver_domain,
    mssqlserver_system,
)

pytestmark = pytest.mark.mssql


def _clean_uri(uri: str) -> str:
    """Return a URI with the username and password stripped out, for use in
    a connection object that carries the credentials separately."""
    url = make_url(uri)
    return url.set(username=None, password=None).render_as_string(hide_password=False)


def _src_conn_from(cfg: MssqlserverDomain) -> MSSQLSrcConn:
    clean_uri = _clean_uri(cfg.sqlalchemy_uri)
    return MSSQLSrcConn(
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


@mssqlserver_system("mssqlserver")
@mssqlserver_domain("mssqlserver_world_xs")
def test_check_connect_succeeds(
    system_mssqlserver: MssqlserverSystem,
    domain_mssqlserver_world_xs: MssqlserverDomain,
):
    conn = _src_conn_from(domain_mssqlserver_world_xs)
    # Contract: returns None on success and must not raise.
    assert MSSQLServerConnectionExplorer().check_connect(conn) is None


@mssqlserver_system("mssqlserver")
@mssqlserver_domain("mssqlserver_world_xs")
def test_check_connect_bad_credentials_raises(
    system_mssqlserver: MssqlserverSystem,
    domain_mssqlserver_world_xs: MssqlserverDomain,
):
    clean_uri = _clean_uri(domain_mssqlserver_world_xs.sqlalchemy_uri)
    bad = MSSQLSrcConn(
        uri=clean_uri,
        credentials=UserPassword(
            user=Secret(domain_mssqlserver_world_xs.user),
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


@mssqlserver_system("mssqlserver")
@mssqlserver_domain("mssqlserver_world_xs")
def test_metadata_lists_seeded_tables(
    system_mssqlserver: MssqlserverSystem,
    domain_mssqlserver_world_xs: MssqlserverDomain,
):
    conn = _src_conn_from(domain_mssqlserver_world_xs)
    entities = MSSQLServerConnectionExplorer().metadata(conn)

    assert entities and all(isinstance(e, SQLEntity) for e in entities)

    # Only ``dbo`` holds user tables; sys / INFORMATION_SCHEMA and the fixed
    # database roles are skipped, so exactly the three seeded tables show up.
    assert {e.table for e in entities} == {
        "wld_country",
        "wld_city",
        "wld_country_language",
    }

    # No `map_entity_fields` override, so the base maps the iterated schema
    # name (``dbo``) into both `database` and `schema`.
    users = _entity_by_table(entities, "wld_country")
    assert users.database == "dbo"
    assert users.schema == "dbo"


@mssqlserver_system("mssqlserver")
@mssqlserver_domain("mssqlserver_world_xs")
def test_metadata_reflects_columns_and_primary_key(
    system_mssqlserver: MssqlserverSystem,
    domain_mssqlserver_world_xs: MssqlserverDomain,
):
    conn = _src_conn_from(domain_mssqlserver_world_xs)
    users = _entity_by_table(MSSQLServerConnectionExplorer().metadata(conn), "wld_city")

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


@mssqlserver_system("mssqlserver")
@mssqlserver_domain("mssqlserver_world_xs")
def test_data_returns_all_rows(
    system_mssqlserver: MssqlserverSystem,
    domain_mssqlserver_world_xs: MssqlserverDomain,
):
    conn = _src_conn_from(domain_mssqlserver_world_xs)
    explorer = MSSQLServerConnectionExplorer()
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


@mssqlserver_system("mssqlserver")
@mssqlserver_domain("mssqlserver_world_xs")
def test_data_respects_limit(
    system_mssqlserver: MssqlserverSystem,
    domain_mssqlserver_world_xs: MssqlserverDomain,
):
    conn = _src_conn_from(domain_mssqlserver_world_xs)
    explorer = MSSQLServerConnectionExplorer()
    users = _entity_by_table(explorer.metadata(conn), "wld_country_language")

    rows = explorer.data(conn, users, limit=2)
    assert len(rows) == 2
