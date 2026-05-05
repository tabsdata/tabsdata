#
# Copyright 2026 Tabs Data Inc.
#
"""Live Druid extractor tests — gated by the ``DRUID`` env var."""

from __future__ import annotations

from tabsdata_testutils.druid import DRUID
from tabsdata_testutils.external import external

from tabsdata._cli.tdconn._internal.extractor.connector.druid.extractor import (
    DruidConnectionExtractor,
)

# DruidConn requires a ``staging`` config. The extractor never touches it,
# so the itest supplies a minimal S3 staging with placeholder values.
_DUMMY_STAGING = {
    "provider": "s3",
    "bucket": "itest-staging",
    "region": "us-east-1",
    "base_path": "staging",
    "credentials": {"aws_access_key_id": "x", "aws_secret_access_key": "x"},
}


def _info(envs: dict) -> dict:
    host = envs.get("HOST") or "localhost"
    port = envs.get("PORT") or "8082"
    tls = (envs.get("TLS") or "").lower() in ("1", "true", "yes")
    scheme = "https" if tls else "http"
    info: dict = {
        "overlord_url": f"{scheme}://{host}:{port}",
        "staging": _DUMMY_STAGING,
    }
    user = envs.get("USERNAME")
    password = envs.get("PASSWORD")
    if user or password:
        info["credentials"] = {"user": user or "", "password": password or ""}
    path = envs.get("PATH")
    if path:
        info["path"] = path
    if tls:
        info["tls"] = True
    return info


@external(system=DRUID)
def test_druid_check_connect(envs: dict):
    DruidConnectionExtractor().check_connect(_info(envs))


@external(system=DRUID)
def test_druid_metadata(envs: dict):
    assert isinstance(DruidConnectionExtractor().metadata(_info(envs)), list)
