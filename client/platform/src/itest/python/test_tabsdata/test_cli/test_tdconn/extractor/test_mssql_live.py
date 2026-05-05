#
# Copyright 2026 Tabs Data Inc.
#
"""Live SQL Server extractor tests — gated by the ``MSSQL`` env var."""

from __future__ import annotations

from tabsdata_testutils.external import external
from tabsdata_testutils.mssql import MSSQL

from tabsdata._cli.tdconn._internal.extractor.connector.mssqlserver.extractor import (
    MSSQLServerConnectionExtractor,
)


def _info(envs: dict) -> dict:
    host = envs.get("HOST") or "localhost"
    port = envs.get("PORT") or "1433"
    database = envs.get("DATABASE") or ""
    uri = f"mssql://{host}:{port}/{database}" if database else f"mssql://{host}:{port}"
    info: dict = {
        "connection_string_or_uri": uri,
        "credentials": {
            "user": envs.get("USERNAME") or "",
            "password": envs.get("PASSWORD") or "",
        },
    }
    driver = envs.get("DRIVER")
    schema = envs.get("SCHEMA")
    if driver:
        info["driver"] = driver
    if schema:
        info["schema"] = schema
    if (envs.get("TRUSTED_CONNECTION") or "").lower() in ("1", "true", "yes"):
        info["trusted_connection"] = True
    return info


@external(system=MSSQL)
def test_mssql_check_connect(envs: dict):
    MSSQLServerConnectionExtractor().check_connect(_info(envs))


@external(system=MSSQL)
def test_mssql_metadata(envs: dict):
    assert isinstance(MSSQLServerConnectionExtractor().metadata(_info(envs)), list)
