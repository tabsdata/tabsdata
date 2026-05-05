#
# Copyright 2026 Tabs Data Inc.
#
"""Live GCS extractor tests — gated by the ``GCS`` env var."""

from __future__ import annotations

from pathlib import Path

from tabsdata_testutils.external import external
from tabsdata_testutils.gcs import GCS

from tabsdata._cli.tdconn._internal.extractor.connector.gcs.extractor import (
    GCSConnectionExtractor,
)


def _info(envs: dict) -> dict:
    """Build a GCSConn-shaped info dict from the system env vars.

    GCSConn takes the service-account key inline; this helper reads it
    from a file path if only ``CREDENTIALS_FILE`` is provided.
    """
    bucket = envs.get("BUCKET") or ""
    key = envs.get("CREDENTIALS_JSON")
    if not key:
        creds_file = envs.get("CREDENTIALS_FILE")
        if creds_file:
            key = Path(creds_file).read_text()
    info: dict = {
        "bucket_uri": f"gs://{bucket}",
        "credentials": {"service_account_key": key or "{}"},
    }
    project = envs.get("PROJECT")
    if project:
        info["project"] = project
    return info


@external(system=GCS)
def test_gcs_check_connect(envs: dict):
    GCSConnectionExtractor().check_connect(_info(envs))


@external(system=GCS)
def test_gcs_metadata_and_data_are_empty(envs: dict):
    ext = GCSConnectionExtractor()
    info = _info(envs)
    assert ext.metadata(info) == []
    assert ext.data(info, entity=None, limit=1) == []  # type: ignore[arg-type]
