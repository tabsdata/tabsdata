#
# Copyright 2026 Tabsdata Inc.
#

"""End-to-end ``MongoDBConnectionExplorer`` tests against a real MongoDB
instance.

The ``mongodb`` domain (provisioned by tdx) has no fixed seed, so the
``seeded`` fixture below populates the connection's default database with two
throwaway collections -- ``td_explorer_countries_<uuid>`` and
``td_explorer_cities_<uuid>`` -- and drops them again on teardown. The uuid
suffix keeps concurrent / repeated runs against the shared database from
colliding. These tests exercise the catalog introspection surface of
``MongoDBConnectionExplorer`` -- ``check_connect`` / ``metadata`` / ``data`` --
against the live server: ``metadata`` lists the non-system collections in the
default database (recording each one's type and indexes) and ``data`` samples
documents.
"""

from __future__ import annotations

import uuid
from types import SimpleNamespace

import pytest
from pymongo.errors import OperationFailure

from tabsdatak.api import Secret
from tabsdatak.conn.common.types import UserPassword
from tabsdatak.conn.mongodb import MongoDBDestConn
from tabsdatak.conn.mongodb._explorer._explorer import MongoDBConnectionExplorer
from tabsdatak.conn.mongodb._explorer._model import MongoDBEntity

from tabsdatax.itest.fixture.backends.mongodb import MongodbDomain, mongodb_domain

pytestmark = pytest.mark.mongodb

_COUNTRIES = [
    {"code": "AFG", "name": "Afghanistan", "continent": "Asia"},
    {"code": "NLD", "name": "Netherlands", "continent": "Europe"},
]
_CITIES = [
    {"name": "Kabul", "country_code": "AFG", "population": 1780000},
    {"name": "Qandahar", "country_code": "AFG", "population": 237500},
    {"name": "Herat", "country_code": "AFG", "population": 186800},
]


def _clean_uri(cfg: MongodbDomain) -> str:
    # `MongoDBDestConn.uri` must carry no credentials -- those ride separately.
    return f"mongodb://{cfg.host}:{cfg.port}/{cfg.db}"


def _conn_from(cfg: MongodbDomain) -> MongoDBDestConn:
    return MongoDBDestConn(
        uri=_clean_uri(cfg),
        credentials=UserPassword(user=Secret(cfg.user), password=Secret(cfg.password)),
    )


def _mongo_client(cfg: MongodbDomain):
    from pymongo import MongoClient

    return MongoClient(_clean_uri(cfg), username=cfg.user, password=cfg.password)


def _entity_by_collection(
    entities: list[MongoDBEntity], collection: str
) -> MongoDBEntity:
    matches = [e for e in entities if e.collection == collection]
    names = [e.collection for e in entities]
    assert matches, f"no entity for {collection!r} in {names}"
    return matches[0]


@pytest.fixture
def seeded(domain_mongodb: MongodbDomain) -> SimpleNamespace:
    """Seed two uuid-suffixed throwaway collections in the default database and
    drop them after; yields their names."""
    suffix = uuid.uuid4().hex
    countries = f"td_explorer_countries_{suffix}"
    cities = f"td_explorer_cities_{suffix}"
    client = _mongo_client(domain_mongodb)
    db = client.get_database()
    try:
        # Copy the docs -- ``insert_many`` stamps ``_id`` onto the inputs.
        db[countries].insert_many([dict(d) for d in _COUNTRIES])
        db[cities].insert_many([dict(d) for d in _CITIES])
        yield SimpleNamespace(countries=countries, cities=cities)
    finally:
        db.drop_collection(countries)
        db.drop_collection(cities)
        client.close()


# ---------------------------------------------------------------------------
# check_connect
# ---------------------------------------------------------------------------


@mongodb_domain("mongodb")
def test_check_connect_succeeds(domain_mongodb: MongodbDomain):
    conn = _conn_from(domain_mongodb)
    assert MongoDBConnectionExplorer().check_connect(conn) is None


@mongodb_domain("mongodb")
def test_check_connect_bad_credentials_raises(domain_mongodb: MongodbDomain):
    bad = MongoDBDestConn(
        uri=_clean_uri(domain_mongodb),
        credentials=UserPassword(
            user=Secret(domain_mongodb.user),
            password=Secret("definitely-wrong-password"),
        ),
    )
    with pytest.raises(OperationFailure):
        MongoDBConnectionExplorer().check_connect(bad)


# ---------------------------------------------------------------------------
# metadata
# ---------------------------------------------------------------------------


@mongodb_domain("mongodb")
def test_metadata_lists_seeded_collections(
    domain_mongodb: MongodbDomain, seeded: SimpleNamespace
):
    conn = _conn_from(domain_mongodb)
    entities = MongoDBConnectionExplorer().metadata(conn)

    assert entities and all(isinstance(e, MongoDBEntity) for e in entities)

    # The database is shared, so assert the seeded collections are present
    # rather than that they are the only ones.
    names = {e.collection for e in entities}
    assert {seeded.countries, seeded.cities} <= names

    cities = _entity_by_collection(entities, seeded.cities)
    assert cities.database == domain_mongodb.database
    assert cities.collection_type == "collection"


@mongodb_domain("mongodb")
def test_metadata_reflects_indexes(
    domain_mongodb: MongodbDomain, seeded: SimpleNamespace
):
    conn = _conn_from(domain_mongodb)
    cities = _entity_by_collection(
        MongoDBConnectionExplorer().metadata(conn), seeded.cities
    )

    # Every collection carries the default primary-key index on ``_id``.
    assert any(idx.name == "_id_" for idx in cities.indexes)


# ---------------------------------------------------------------------------
# data
# ---------------------------------------------------------------------------


@mongodb_domain("mongodb")
def test_data_returns_documents(domain_mongodb: MongodbDomain, seeded: SimpleNamespace):
    conn = _conn_from(domain_mongodb)
    explorer = MongoDBConnectionExplorer()
    cities = _entity_by_collection(explorer.metadata(conn), seeded.cities)

    rows = explorer.data(conn, cities, limit=10)

    assert isinstance(rows, list)
    assert len(rows) == 3
    assert {r["name"] for r in rows} == {"Kabul", "Qandahar", "Herat"}
    # ``_id`` is stringified so callers get a JSON-safe dict.
    assert all(isinstance(r["_id"], str) for r in rows)


@mongodb_domain("mongodb")
def test_data_respects_limit(domain_mongodb: MongodbDomain, seeded: SimpleNamespace):
    conn = _conn_from(domain_mongodb)
    explorer = MongoDBConnectionExplorer()
    cities = _entity_by_collection(explorer.metadata(conn), seeded.cities)

    rows = explorer.data(conn, cities, limit=2)
    assert len(rows) == 2
