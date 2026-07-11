#
#  Copyright 2026. Tabs Data Inc.
#

"""End-to-end ``StarRocksConnectionExplorer`` tests against a real
StarRocks instance.

The ``starrocks_world_xs`` domain (provisioned by tdx) seeds the database
with a ``wld_country`` table (``code``), a ``wld_city`` table (``id``,
``name``, ``country_code``, ``district``, ``population``) and a
``wld_country_language`` table (``country_code`` + ``language``). These
tests exercise the catalog introspection surface of
``StarRocksConnectionExplorer`` -- ``check_connect`` / ``metadata`` /
``data`` -- against the live server.

StarRocks speaks the MySQL wire protocol and has a single namespace level
(a *database*), so identifiers come back lower-case and the explorer maps
the database into ``database`` with an empty ``schema`` (like MySQL). The
primary-key flag is not asserted: how StarRocks surfaces its key model
through MySQL reflection isn't relied on here. Constructing a
``StarRocksDestConn`` requires a cloud staging area, but the explorer never
touches it, so a placeholder ``S3Location`` is used.
"""

from __future__ import annotations

import pytest
from sqlalchemy.exc import DBAPIError

from tabsdatak.api import Secret
from tabsdatak.conn.common.sql._explorer import SQLEntity
from tabsdatak.conn.common.types import AwsAccessSecretKey, UserPassword
from tabsdatak.conn.starrocks import S3Location, StarRocksDestConn
from tabsdatak.conn.starrocks._explorer import StarRocksConnectionExplorer

from tabsdatax.itest.fixture.backends.starrocks import (
    StarrocksDomain,
    StarrocksSystem,
    starrocks_domain,
    starrocks_system,
)

pytestmark = pytest.mark.starrocks

# The explorer only issues catalog SQL over the MySQL protocol; the staging
# area is never used, so a placeholder is enough to construct the conn.
_STAGING = S3Location(
    bucket="unused",
    region="us-east-1",
    credentials=AwsAccessSecretKey(
        access_key_id=Secret("unused"), secret_access_key=Secret("unused")
    ),
)


def _conn_from(cfg: StarrocksDomain) -> StarRocksDestConn:
    return StarRocksDestConn(
        host=cfg.host,
        port=cfg.port,
        database=cfg.database,
        staging=_STAGING,
        credentials=UserPassword(user=Secret(cfg.user), password=Secret(cfg.password)),
    )


def _entity_by_table(entities: list[SQLEntity], table: str) -> SQLEntity:
    matches = [e for e in entities if e.table == table]
    assert matches, f"no entity for {table!r} in {[e.table for e in entities]}"
    return matches[0]


# ---------------------------------------------------------------------------
# check_connect
# ---------------------------------------------------------------------------


@starrocks_system("starrocks")
@starrocks_domain("starrocks_world_xs")
def test_check_connect_succeeds(
    system_starrocks: StarrocksSystem, domain_starrocks_world_xs: StarrocksDomain
):
    conn = _conn_from(domain_starrocks_world_xs)
    assert StarRocksConnectionExplorer().check_connect(conn) is None


@starrocks_system("starrocks")
@starrocks_domain("starrocks_world_xs")
def test_check_connect_bad_credentials_raises(
    system_starrocks: StarrocksSystem, domain_starrocks_world_xs: StarrocksDomain
):
    cfg = domain_starrocks_world_xs
    bad = StarRocksDestConn(
        host=cfg.host,
        port=cfg.port,
        database=cfg.database,
        staging=_STAGING,
        credentials=UserPassword(
            user=Secret(cfg.user),
            password=Secret("definitely-wrong-password"),
        ),
    )
    with pytest.raises(DBAPIError):
        StarRocksConnectionExplorer().check_connect(bad)


# ---------------------------------------------------------------------------
# metadata
# ---------------------------------------------------------------------------


@starrocks_system("starrocks")
@starrocks_domain("starrocks_world_xs")
def test_metadata_lists_seeded_tables(
    system_starrocks: StarrocksSystem, domain_starrocks_world_xs: StarrocksDomain
):
    conn = _conn_from(domain_starrocks_world_xs)
    entities = StarRocksConnectionExplorer().metadata(conn)

    assert entities and all(isinstance(e, SQLEntity) for e in entities)

    # The scan is pinned to the configured database, so only the three seeded
    # tables show up (identifiers are lower-case).
    assert {e.table for e in entities} == {
        "wld_country",
        "wld_city",
        "wld_country_language",
    }

    # StarRocks has no schema level: the database fills `database` and `schema`
    # is left blank.
    users = _entity_by_table(entities, "wld_country")
    assert users.database == domain_starrocks_world_xs.database
    assert users.schema == ""


@starrocks_system("starrocks")
@starrocks_domain("starrocks_world_xs")
def test_metadata_reflects_columns(domain_starrocks_world_xs: StarrocksDomain):
    conn = _conn_from(domain_starrocks_world_xs)
    users = _entity_by_table(StarRocksConnectionExplorer().metadata(conn), "wld_city")

    by_name = {c.name: c for c in users.columns}
    assert set(by_name) == {"id", "name", "country_code", "district", "population"}

    # Types are reflected and simplified to a short canonical name.
    assert by_name["id"].type == "BIGINT"
    assert by_name["name"].type == "VARCHAR"
    assert by_name["country_code"].type == "CHAR"
    assert by_name["district"].type == "VARCHAR"
    assert by_name["population"].type.startswith("INT")


# ---------------------------------------------------------------------------
# data
# ---------------------------------------------------------------------------


@starrocks_system("starrocks")
@starrocks_domain("starrocks_world_xs")
def test_data_returns_rows(
    system_starrocks: StarrocksSystem, domain_starrocks_world_xs: StarrocksDomain
):
    conn = _conn_from(domain_starrocks_world_xs)
    explorer = StarRocksConnectionExplorer()
    users = _entity_by_table(explorer.metadata(conn), "wld_city")

    rows = explorer.data(conn, users, limit=3)

    assert isinstance(rows, list)
    assert len(rows) == 3
    assert all(
        set(r) == {"id", "name", "country_code", "district", "population"} for r in rows
    )


@starrocks_system("starrocks")
@starrocks_domain("starrocks_world_xs")
def test_data_respects_limit(
    system_starrocks: StarrocksSystem, domain_starrocks_world_xs: StarrocksDomain
):
    conn = _conn_from(domain_starrocks_world_xs)
    explorer = StarRocksConnectionExplorer()
    users = _entity_by_table(explorer.metadata(conn), "wld_country_language")

    rows = explorer.data(conn, users, limit=2)
    assert len(rows) == 2
