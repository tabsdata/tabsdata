#
# Copyright 2026 Tabs Data Inc.
#
"""Live StarRocks extractor tests — gated by the ``STARROCKS`` env var."""

from __future__ import annotations

from tabsdata_testutils.external import external
from tabsdata_testutils.starrocks import STARROCKS

from tabsdata._cli.tdconn._internal.extractor.connector.starrocks.extractor import (
    StarRocksConnectionExtractor,
)

# StarRocksConn requires a ``staging`` config. The extractor never touches
# it, so the itest supplies a minimal S3 staging with placeholder values.
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
        "port": int(envs.get("PORT") or "9030"),
        "database": envs.get("DATABASE") or "default",
        "staging": _DUMMY_STAGING,
    }
    user = envs.get("USERNAME")
    password = envs.get("PASSWORD")
    if user or password:
        info["credentials"] = {"user": user or "", "password": password or ""}
    return info


@external(system=STARROCKS)
def test_starrocks_check_connect(envs: dict):
    StarRocksConnectionExtractor().check_connect(_info(envs))


@external(system=STARROCKS)
def test_starrocks_metadata(envs: dict):
    assert isinstance(StarRocksConnectionExtractor().metadata(_info(envs)), list)
