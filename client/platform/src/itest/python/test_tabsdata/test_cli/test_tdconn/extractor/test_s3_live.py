#
# Copyright 2026 Tabs Data Inc.
#
"""Live S3 extractor tests — gated by the ``S3`` env var."""

from __future__ import annotations

from tabsdata_testutils.external import external
from tabsdata_testutils.s3 import S3

from tabsdata._cli.tdconn._internal.extractor.connector.s3.extractor import (
    S3ConnectionExtractor,
)


def _info(envs: dict) -> dict:
    """Build an S3Conn-shaped info dict from the system env vars."""
    bucket = envs.get("BUCKET") or ""
    info: dict = {
        "bucket_uri": f"s3://{bucket}",
        "credentials": {
            "aws_access_key_id": envs.get("ACCESS_KEY") or "",
            "aws_secret_access_key": envs.get("SECRET_KEY") or "",
        },
    }
    region = envs.get("REGION")
    if region:
        info["region"] = region
    endpoint = envs.get("ENDPOINT_URL")
    if endpoint:
        info["endpoint_url"] = endpoint
    return info


@external(system=S3)
def test_s3_check_connect(envs: dict):
    S3ConnectionExtractor().check_connect(_info(envs))


@external(system=S3)
def test_s3_metadata_and_data_are_empty(envs: dict):
    ext = S3ConnectionExtractor()
    info = _info(envs)
    assert ext.metadata(info) == []
    assert ext.data(info, entity=None, limit=1) == []  # type: ignore[arg-type]
