#
# Copyright 2026 Tabs Data Inc.
#
"""Live Db2 extractor tests — gated by the ``DB2`` env var."""

from __future__ import annotations

from tabsdata_testutils.db2 import DB2
from tabsdata_testutils.external import external

from tabsdata._cli.tdconn._internal.extractor.connector.db2.extractor import (
    Db2ConnectionExtractor,
)


def _info(envs: dict) -> dict:
    host = envs.get("HOST") or "localhost"
    port = envs.get("PORT") or "50000"
    database = envs.get("DATABASE") or ""
    uri = f"db2://{host}:{port}/{database}" if database else f"db2://{host}:{port}"
    info: dict = {
        "uri": uri,
        "credentials": {
            "user": envs.get("USERNAME") or "",
            "password": envs.get("PASSWORD") or "",
        },
    }
    schema = envs.get("SCHEMA")
    if schema:
        info["schema"] = schema
    return info


@external(system=DB2)
def test_db2_check_connect(envs: dict):
    Db2ConnectionExtractor().check_connect(_info(envs))


@external(system=DB2)
def test_db2_metadata(envs: dict):
    assert isinstance(Db2ConnectionExtractor().metadata(_info(envs)), list)
