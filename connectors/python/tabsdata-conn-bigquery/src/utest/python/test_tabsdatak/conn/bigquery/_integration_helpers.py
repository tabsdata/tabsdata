#
# Copyright 2026 Tabs Data Inc.
#

"""Shared helpers for the BigQuery integration test files.

Lives in a private (underscore-prefixed) module so pytest doesn't
collect it as tests. The ``fresh_bigquery`` fixture in ``conftest.py``,
``test__plugin_integration.py`` and ``test__explorer_integration.py``
all import from here rather than reaching into ``conftest`` -- importing
a conftest module by name is fragile and discouraged.
"""

from __future__ import annotations

from tabsdatak.api import Secret
from tabsdatak.conn.bigquery import BigQueryDestConn, GCSLocation
from tabsdatak.conn.common.types import GCPJson


class _BqConfig:
    def __init__(
        self,
        *,
        sa_json: str,
        project: str,
        dataset: str,
        bucket: str,
        base_path: str,
    ) -> None:
        self.sa_json = sa_json
        self.project = project
        self.dataset = dataset
        self.bucket = bucket
        self.base_path = base_path


def _split_gs_uri(uri: str) -> tuple[str, str]:
    """Split ``gs://<bucket>[/<path>]`` into ``(bucket, base_path)``.

    Path defaults to ``"staging/"`` when the URI carries only a bucket.
    The returned ``base_path`` is normalized to always end with ``/``
    so callers don't have to worry about ``GCSLocation.full_path``
    join semantics.
    """
    prefix = "gs://"
    if not uri.startswith(prefix):
        raise ValueError(f"GCP storage URI must start with gs://: {uri!r}")
    rest = uri[len(prefix) :].strip("/")
    if "/" in rest:
        bucket, base_path = rest.split("/", 1)
        base_path = base_path.strip("/") + "/"
    else:
        bucket, base_path = rest, "staging/"
    return bucket, base_path


def _conn_from(cfg: _BqConfig) -> BigQueryDestConn:
    return BigQueryDestConn(
        staging=GCSLocation(
            bucket=cfg.bucket,
            credentials=GCPJson(json=Secret(cfg.sa_json)),
            base_path=cfg.base_path,
        ),
        credentials=GCPJson(json=Secret(cfg.sa_json)),
        project=cfg.project,
        dataset=cfg.dataset,
    )


def _table_fqn(cfg: _BqConfig, table: str) -> str:
    """Build the user-facing FQN (outer-only backticks per BigQuery
    syntax). The plugin strips the wrapping backticks before
    forwarding to the SDK."""
    return f"`{cfg.project}.{cfg.dataset}.{table}`"


def _bq_client(cfg: _BqConfig):
    from google.cloud import bigquery
    import json as _json

    return bigquery.Client.from_service_account_info(
        _json.loads(cfg.sa_json),
        project=cfg.project,
    )
