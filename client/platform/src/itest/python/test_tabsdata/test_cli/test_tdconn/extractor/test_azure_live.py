#
# Copyright 2026 Tabs Data Inc.
#
"""Live Azure Blob Storage extractor tests — gated by the ``AZURE`` env var."""

from __future__ import annotations

from tabsdata_testutils.azure import AZURE
from tabsdata_testutils.external import external

from tabsdata._cli.tdconn._internal.extractor.connector.azure.extractor import (
    AzureConnectionExtractor,
)


def _info(envs: dict) -> dict:
    """Build an AzureConn-shaped info dict from the system env vars."""
    container = envs.get("CONTAINER") or ""
    info: dict = {
        "container_uri": f"az://{container}",
        "credentials": {
            "account_name": envs.get("ACCOUNT") or "",
            "account_key": envs.get("ACCOUNT_KEY") or "",
        },
    }
    cs = envs.get("CONNECTION_STRING")
    if cs:
        info["connection_string"] = cs
    return info


@external(system=AZURE)
def test_azure_check_connect(envs: dict):
    AzureConnectionExtractor().check_connect(_info(envs))


@external(system=AZURE)
def test_azure_metadata_and_data_are_empty(envs: dict):
    ext = AzureConnectionExtractor()
    info = _info(envs)
    assert ext.metadata(info) == []
    assert ext.data(info, entity=None, limit=1) == []  # type: ignore[arg-type]
