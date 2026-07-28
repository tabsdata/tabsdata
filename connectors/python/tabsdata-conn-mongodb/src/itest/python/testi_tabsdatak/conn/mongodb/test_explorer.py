#
# Copyright 2026 Tabsdata Inc.
#

"""MongoDB connection-explorer integration tests"""

from __future__ import annotations

from collections.abc import Iterator

import pytest
from pymongo.errors import OperationFailure
from tabsdatax.itest.fixture.backends.mongodb import MongoDBDomain, mongodb_domain
from testi_tabsdatak.conn.mongodb.backend import dest_conn, store

from tabsdatak.api import Secret
from tabsdatak.conn.common.types import UserPassword
from tabsdatak.conn.mongodb import MongoDBDestConn
from tabsdatak.conn.mongodb._explorer._explorer import MongoDBConnectionExplorer
from tabsdatak.conn.mongodb._explorer._model import MongoDBEntity

pytestmark = pytest.mark.mongodb

CITIES = "td_explorer_cities"
CITY_DOCUMENTS = [
    {"name": "Kabul", "country_code": "AFG", "population": 1780000},
    {"name": "Qandahar", "country_code": "AFG", "population": 237500},
    {"name": "Herat", "country_code": "AFG", "population": 186800},
]


@pytest.fixture
def seeded(domain_mongodb: MongoDBDomain) -> Iterator[str]:
    """The mongodb domain carries no model, so the explorer is introspected against
    a collection the test writes itself; yields its name."""
    db = store(domain_mongodb)
    db.write_collection(CITIES, CITY_DOCUMENTS)
    try:
        yield CITIES
    finally:
        db.drop_collection(CITIES)


def _entity_by_collection(
    entities: list[MongoDBEntity], collection: str
) -> MongoDBEntity:
    matches = [e for e in entities if e.collection == collection]
    names = [e.collection for e in entities]
    assert matches, f"no entity for {collection!r} in {names}"
    return matches[0]


# ---------------------------------------------------------------------------
# check_connect
# ---------------------------------------------------------------------------


@mongodb_domain("mongodb")
def test_check_connect_succeeds(domain_mongodb: MongoDBDomain):
    conn = dest_conn(domain_mongodb)
    assert MongoDBConnectionExplorer().check_connect(conn) is None


@mongodb_domain("mongodb")
def test_check_connect_bad_credentials_raises(domain_mongodb: MongoDBDomain):
    bad = MongoDBDestConn(
        uri=domain_mongodb.uri,
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
def test_metadata_lists_created_collection(domain_mongodb: MongoDBDomain, seeded: str):
    conn = dest_conn(domain_mongodb)
    entities = MongoDBConnectionExplorer().metadata(conn)

    assert entities and all(isinstance(e, MongoDBEntity) for e in entities)

    cities = _entity_by_collection(entities, seeded)
    assert cities.database == domain_mongodb.database
    assert cities.collection_type == "collection"


@mongodb_domain("mongodb")
def test_metadata_reflects_indexes(domain_mongodb: MongoDBDomain, seeded: str):
    conn = dest_conn(domain_mongodb)
    cities = _entity_by_collection(MongoDBConnectionExplorer().metadata(conn), seeded)

    # Every collection carries the default primary-key index on `_id`.
    assert any(idx.name == "_id_" for idx in cities.indexes)


# ---------------------------------------------------------------------------
# data
# ---------------------------------------------------------------------------


@mongodb_domain("mongodb")
def test_data_returns_documents(domain_mongodb: MongoDBDomain, seeded: str):
    conn = dest_conn(domain_mongodb)
    explorer = MongoDBConnectionExplorer()
    cities = _entity_by_collection(explorer.metadata(conn), seeded)

    rows = explorer.data(conn, cities, limit=10)

    assert isinstance(rows, list)
    assert len(rows) == len(CITY_DOCUMENTS)
    assert {r["name"] for r in rows} == {d["name"] for d in CITY_DOCUMENTS}
    # `_id` is stringified so callers get a JSON-safe dict.
    assert all(isinstance(r["_id"], str) for r in rows)


@mongodb_domain("mongodb")
def test_data_respects_limit(domain_mongodb: MongoDBDomain, seeded: str):
    conn = dest_conn(domain_mongodb)
    explorer = MongoDBConnectionExplorer()
    cities = _entity_by_collection(explorer.metadata(conn), seeded)

    rows = explorer.data(conn, cities, limit=2)
    assert len(rows) == 2
