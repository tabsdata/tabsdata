#
# Copyright 2026 Tabs Data Inc.
#
"""Live Oracle extractor tests — gated by the ``ORACLE`` env var."""

from __future__ import annotations

from tabsdata_testutils.external import external
from tabsdata_testutils.oracle import ORACLE

from tabsdata._cli.tdconn._internal.extractor.connector.oracle.extractor import (
    OracleConnectionExtractor,
)


def _info(envs: dict) -> dict:
    host = envs.get("HOST") or "localhost"
    port = envs.get("PORT") or "1521"
    database = envs.get("DATABASE") or ""
    uri = (
        f"oracle://{host}:{port}/{database}" if database else f"oracle://{host}:{port}"
    )
    info: dict = {
        "uri": uri,
        "credentials": {
            "user": envs.get("USERNAME") or "",
            "password": envs.get("PASSWORD") or "",
        },
    }
    owner = envs.get("OWNER")
    if owner:
        info["owner"] = owner
    if (envs.get("SERVICE_NAME") or "").lower() in ("1", "true", "yes"):
        info["service_name"] = True
    return info


@external(system=ORACLE)
def test_oracle_check_connect(envs: dict):
    OracleConnectionExtractor().check_connect(_info(envs))


@external(system=ORACLE)
def test_oracle_metadata(envs: dict):
    assert isinstance(OracleConnectionExtractor().metadata(_info(envs)), list)
