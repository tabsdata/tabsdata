#
# Copyright 2026 Tabsdata Inc.
#

"""Shared MongoDB test fixtures for this package's integration and e2e suites: a
pymongo-backed store over whichever mongodb backend the test declared (its
``domain``) to read collections back and drop scratch collections -- the MongoDB
analogue of the object-store adapter the file connectors use and the
``SqlAlchemyStore`` the SQL connectors use.

MongoDB is a destination-only connector (no ``MongoDBSrc``), so the store is the
independent oracle the tests verify against; the connector never reads back.
Documents come back as a ``pl.DataFrame`` (with the auto ``_id`` projected out) so
``assert_frames_match`` can compare them order- and dtype-agnostically."""

from __future__ import annotations

import polars as pl
import pytest


class _MongoStore:
    """pymongo-backed verification client over the mongodb backend the test
    declared. Splices the backend credentials in via pymongo's ``username`` /
    ``password`` kwargs, leaving the domain's credential-less ``uri`` intact."""

    def __init__(self, backend) -> None:
        import pymongo

        self._client = pymongo.MongoClient(
            backend.uri, username=backend.user, password=backend.password
        )
        self._db = self._client[backend.database]

    def read_collection(self, name: str) -> pl.DataFrame:
        docs = list(self._db[name].find({}, {"_id": 0}))
        return pl.DataFrame(docs)

    def count(self, name: str) -> int:
        return self._db[name].count_documents({})

    def drop_collection(self, name: str) -> None:
        self._db[name].drop()


@pytest.fixture
def mongo():
    """Factory building a ``_MongoStore`` over the mongodb backend the test
    declared -- call it with ``domain_mongodb``."""
    return _MongoStore
