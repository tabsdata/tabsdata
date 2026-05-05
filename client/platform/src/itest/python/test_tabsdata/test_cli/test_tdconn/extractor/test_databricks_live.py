#
# Copyright 2026 Tabs Data Inc.
#
"""Live Databricks extractor tests — gated by the ``DATABRICKS`` env var."""

from __future__ import annotations

from tabsdata_testutils.databricks import DATABRICKS
from tabsdata_testutils.external import external

from tabsdata._cli.tdconn._internal.extractor.connector.databricks.extractor import (
    DatabricksConnectionExtractor,
)


def _info(envs: dict) -> dict:
    info: dict = {
        "host_url": envs.get("HOST") or "",
        "token": envs.get("TOKEN") or "",
    }
    catalog = envs.get("CATALOG")
    schema = envs.get("SCHEMA")
    if catalog:
        info["catalog"] = catalog
    if schema:
        info["schema"] = schema
    return info


@external(system=DATABRICKS)
def test_databricks_check_connect(envs: dict):
    DatabricksConnectionExtractor().check_connect(_info(envs))


@external(system=DATABRICKS)
def test_databricks_metadata(envs: dict):
    assert isinstance(DatabricksConnectionExtractor().metadata(_info(envs)), list)
