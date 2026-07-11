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
from sqlalchemy.engine import make_url

from tabsdatak.api import Secret
from tabsdatak.conn.common.types import UserPassword
from tabsdatak.conn.mysql import MySQLSrcConn
from tabsdatak.conn.mysql._explorer import MySQLConnectionExplorer
from tabsdatak.conn.common.sql._explorer import SQLEntity

from tabsdatax.itest.fixture.backends.mysql import (
    MysqlDomain,
    MysqlSystem,
    mysql_domain,
    mysql_system,
)

# Share the serialization domain of the other ``fresh_mysql`` consumers:
# the fixture re-seeds the shared session container per test, so every
# test that touches it must run on one worker to avoid a concurrent
# re-seed corrupting a read in flight.
pytestmark = pytest.mark.mysql


def _clean_uri(uri: str) -> str:
    """Return a URI with the username and password stripped out, for use in
    a connection object that carries the credentials separately."""
    url = make_url(uri)
    return url.set(username=None, password=None).render_as_string(hide_password=False)


def _src_conn_from(cfg: MysqlDomain) -> MySQLSrcConn:
    clean_uri = _clean_uri(cfg.sqlalchemy_uri)
    return MySQLSrcConn(
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


@mysql_system("mysql")
@mysql_domain("mysql_world_xs")
def test_check_connect_succeeds(
    system_mysql: MysqlSystem, domain_mysql_world_xs: MysqlDomain
):
    conn = _src_conn_from(domain_mysql_world_xs)
    # Contract: returns None on success and must not raise.
    assert MySQLConnectionExplorer().check_connect(conn) is None


@mysql_system("mysql")
@mysql_domain("mysql_world_xs")
def test_check_connect_bad_credentials_raises(
    system_mysql: MysqlSystem, domain_mysql_world_xs: MysqlDomain
):
    clean_uri = _clean_uri(domain_mysql_world_xs.sqlalchemy_uri)
    bad = MySQLSrcConn(
        uri=clean_uri,
        credentials=UserPassword(
            user=Secret(domain_mysql_world_xs.user),
            password=Secret("definitely-wrong-password"),
        ),
    )
    with pytest.raises(OperationalError):
        MySQLConnectionExplorer().check_connect(bad)


# ---------------------------------------------------------------------------
# metadata
# ---------------------------------------------------------------------------


@mysql_system("mysql")
@mysql_domain("mysql_world_xs")
def test_metadata_lists_seeded_tables(
    system_mysql: MysqlSystem, domain_mysql_world_xs: MysqlDomain
):
    conn = _src_conn_from(domain_mysql_world_xs)
    entities = MySQLConnectionExplorer().metadata(conn)

    assert entities and all(isinstance(e, SQLEntity) for e in entities)

    # The scan is restricted to the URI-path database, so only the two
    # seeded tables show up -- nothing from mysql/sys/information_schema.
    assert {e.table for e in entities} == {
        "wld_country",
        "wld_city",
        "wld_country_language",
    }

    # MySQL maps the URI-path database into `database`; `schema` is blank.
    users = _entity_by_table(entities, "wld_country")
    assert users.database == domain_mysql_world_xs.database
    assert users.schema == ""


@mysql_system("mysql")
@mysql_domain("mysql_world_xs")
def test_metadata_reflects_columns_and_primary_key(domain_mysql_world_xs: MysqlDomain):
    conn = _src_conn_from(domain_mysql_world_xs)
    users = _entity_by_table(MySQLConnectionExplorer().metadata(conn), "wld_city")

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


@mysql_system("mysql")
@mysql_domain("mysql_world_xs")
def test_data_returns_all_rows(
    system_mysql: MysqlSystem, domain_mysql_world_xs: MysqlDomain
):
    conn = _src_conn_from(domain_mysql_world_xs)
    explorer = MySQLConnectionExplorer()
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


@mysql_system("mysql")
@mysql_domain("mysql_world_xs")
def test_data_respects_limit(
    system_mysql: MysqlSystem, domain_mysql_world_xs: MysqlDomain
):
    conn = _src_conn_from(domain_mysql_world_xs)
    explorer = MySQLConnectionExplorer()
    users = _entity_by_table(explorer.metadata(conn), "wld_country_language")

    rows = explorer.data(conn, users, limit=2)
    assert len(rows) == 2
