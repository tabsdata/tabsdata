#
# Copyright 2026 Tabs Data Inc.
#
"""Live BigQuery extractor tests — gated by the ``BIGQUERY`` env var.

Service-account credentials arrive inline via ``CREDENTIALS_JSON``
(the JSON content of the key), or as a file path via
``CREDENTIALS_FILE`` which the test reads and forwards inline.
``BigQueryConn`` JSON carries the key directly — no file-path
variant exists — so the test normalizes both forms.
"""

from __future__ import annotations

from pathlib import Path

from tabsdata_testutils.bigquery import BIGQUERY
from tabsdata_testutils.external import external

from tabsdata._cli.tdconn._internal.extractor.connector.bigquery.extractor import (
    BigQueryConnectionExtractor,
)


def _info(envs: dict) -> dict:
    """Build a BigQueryConn-shaped info dict from the system env vars."""
    key = envs.get("CREDENTIALS_JSON")
    if not key:
        creds_file = envs.get("CREDENTIALS_FILE")
        if creds_file:
            key = Path(creds_file).read_text()
    info: dict = {
        # gcs_folder is required by BigQueryConn for bulk loads; the
        # extractor never writes so we just pass a placeholder.
        "gcs_folder": envs.get("GCS_FOLDER") or "gs://itest-staging/",
        "credentials": {"service_account_key": key or "{}"},
    }
    project = envs.get("PROJECT")
    dataset = envs.get("DATASET")
    if project:
        info["project"] = project
    if dataset:
        info["dataset"] = dataset
    return info


@external(system=BIGQUERY)
def test_bigquery_check_connect(envs: dict):
    BigQueryConnectionExtractor().check_connect(_info(envs))


@external(system=BIGQUERY)
def test_bigquery_metadata(envs: dict):
    assert isinstance(BigQueryConnectionExtractor().metadata(_info(envs)), list)
