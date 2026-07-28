#
# Copyright 2026 Tabsdata Inc.
#

"""Adapters from domain fixture to connector connections and driver direct access"""

from __future__ import annotations

import polars as pl
from tabsdatax.itest.fixture.backends.mongodb import MongodbDomain
from tabsdatax.itest.fixture.functions.sql import sql_credentials

from tabsdatak.conn.mongodb import MongoDBDestConn


def dest_conn(domain: MongodbDomain) -> MongoDBDestConn:
    return MongoDBDestConn(uri=domain.uri, credentials=sql_credentials(domain))


class MongoStore:
    """A store to seed / read back / drop what a test wrote. MongoDB is
    destination-only, so this is the independent oracle every test verifies
    against -- the connector never reads back."""

    def __init__(self, domain: MongodbDomain) -> None:
        import pymongo

        # The domain's `uri` carries no credentials (they ride separately on the
        # connection), so pymongo gets them as its own kwargs.
        self._client = pymongo.MongoClient(
            domain.uri, username=domain.user, password=domain.password
        )
        self._db = self._client[domain.database]

    def read_collection(self, collection: str) -> pl.DataFrame:
        """Every document in `collection`, with the auto ``_id`` projected out so
        the frame holds only what was written."""
        return pl.DataFrame(list(self._db[collection].find({}, {"_id": 0})))

    def count(self, collection: str) -> int:
        return self._db[collection].count_documents({})

    def write_collection(self, collection: str, documents: list[dict]) -> None:
        # ``insert_many`` stamps ``_id`` onto its inputs, so it gets copies.
        self._db[collection].insert_many([dict(d) for d in documents])

    def drop_collection(self, collection: str) -> None:
        self._db[collection].drop()


def store(domain: MongodbDomain) -> MongoStore:
    return MongoStore(domain)
