#
# Copyright 2026 Tabs Data Inc.
#
"""Live ClickHouse extractor tests — gated by the ``CLICKHOUSE`` env var."""

from __future__ import annotations

from tabsdata_testutils.clickhouse import CLICKHOUSE
from tabsdata_testutils.external import external

from tabsdata._cli.tdconn._internal.extractor.connector.clickhouse.extractor import (
    ClickHouseConnectionExtractor,
)

# ClickHouseConn requires a ``staging`` config (the bulk-load destination).
# The extractor never touches it, so the itest supplies a minimal S3 staging
# with placeholder values just to satisfy validation.
_DUMMY_STAGING = {
    "provider": "s3",
    "bucket": "itest-staging",
    "region": "us-east-1",
    "base_path": "staging",
    "credentials": {"aws_access_key_id": "x", "aws_secret_access_key": "x"},
}


def _info(envs: dict) -> dict:
    info: dict = {
        "host": envs.get("HOST") or "localhost",
        "port": int(envs.get("PORT") or "9000"),
        "database": envs.get("DATABASE") or "default",
        "staging": _DUMMY_STAGING,
    }
    user = envs.get("USERNAME")
    password = envs.get("PASSWORD")
    if user or password:
        info["credentials"] = {"user": user or "", "password": password or ""}
    return info


@external(system=CLICKHOUSE)
def test_clickhouse_check_connect(envs: dict):
    ClickHouseConnectionExtractor().check_connect(_info(envs))


@external(system=CLICKHOUSE)
def test_clickhouse_metadata(envs: dict):
    assert isinstance(ClickHouseConnectionExtractor().metadata(_info(envs)), list)
