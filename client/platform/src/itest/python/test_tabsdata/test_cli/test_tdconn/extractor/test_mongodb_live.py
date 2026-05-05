#
# Copyright 2026 Tabs Data Inc.
#
"""Live MongoDB extractor tests — gated by the ``MONGODB`` env var."""

from __future__ import annotations

from tabsdata_testutils.external import external
from tabsdata_testutils.mongodb import MONGODB

from tabsdata._cli.tdconn._internal.extractor.connector.mongodb.extractor import (
    MongoDBConnectionExtractor,
)


def _info(envs: dict) -> dict:
    """Build a MongoDBConn-shaped info dict from the system env vars."""
    uri = envs.get("URI")
    if not uri:
        host = envs.get("HOST") or "localhost"
        port = envs.get("PORT") or "27017"
        uri = f"mongodb://{host}:{port}"
    info: dict = {
        "uri": uri,
        "database": envs.get("DATABASE") or "",
    }
    user = envs.get("USERNAME")
    password = envs.get("PASSWORD")
    if user or password:
        info["credentials"] = {"user": user or "", "password": password or ""}
    return info


@external(system=MONGODB)
def test_mongodb_check_connect(envs: dict):
    MongoDBConnectionExtractor().check_connect(_info(envs))


@external(system=MONGODB)
def test_mongodb_metadata(envs: dict):
    assert isinstance(MongoDBConnectionExtractor().metadata(_info(envs)), list)
