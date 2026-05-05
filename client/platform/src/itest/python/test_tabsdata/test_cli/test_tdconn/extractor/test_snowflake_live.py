#
# Copyright 2026 Tabs Data Inc.
#
"""Live Snowflake extractor tests — gated by the ``SNOWFLAKE`` env var."""

from __future__ import annotations

from tabsdata_testutils.external import external
from tabsdata_testutils.snowflake import SNOWFLAKE

from tabsdata._cli.tdconn._internal.extractor.connector.snowflake.extractor import (
    SnowflakeConnectionExtractor,
)


def _info(envs: dict) -> dict:
    # All seven fields are required by ``SnowflakeConn``; empty strings
    # are used for any slot the environment does not supply.
    return {
        "account": envs.get("ACCOUNT") or "",
        "user": envs.get("USERNAME") or "",
        "password": envs.get("PASSWORD") or "",
        "role": envs.get("ROLE") or "",
        "database": envs.get("DATABASE") or "",
        "schema": envs.get("SCHEMA") or "",
        "warehouse": envs.get("WAREHOUSE") or "",
    }


@external(system=SNOWFLAKE)
def test_snowflake_check_connect(envs: dict):
    SnowflakeConnectionExtractor().check_connect(_info(envs))


@external(system=SNOWFLAKE)
def test_snowflake_metadata(envs: dict):
    assert isinstance(SnowflakeConnectionExtractor().metadata(_info(envs)), list)
