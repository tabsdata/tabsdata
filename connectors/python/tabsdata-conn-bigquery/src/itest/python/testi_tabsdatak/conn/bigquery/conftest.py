#
# Copyright 2026 Tabsdata Inc.
#

"""Shared BigQuery test fixtures for this package's integration and e2e suites: a
``google-cloud-bigquery``-backed store over whichever bigquery backend the test
declared (its ``domain``) to read tables back and drop scratch tables, plus a
``staging`` factory for the GCS load-staging bucket.

BigQuery is destination-only (no ``BigQuerySrc``). It loads via GCS staging
(parquet -> GCS -> BigQuery load job) and auto-creates the table from the parquet
schema. The domain carries its own staging ``bucket`` + ``base_path`` and a GCP
service-account JSON (``service_account_key``) used for both BigQuery and GCS.
BigQuery preserves identifier case, so no case normalization is needed."""

from __future__ import annotations

import json
import uuid

import polars as pl
import pytest

from tabsdatak.api import Secret
from tabsdatak.conn.bigquery import GCPJson, GCSLocation


def _bq_client(backend):
    from google.cloud import bigquery
    from google.oauth2 import service_account

    info = json.loads(backend.service_account_key)
    creds = service_account.Credentials.from_service_account_info(info)
    return bigquery.Client(project=backend.project, credentials=creds)


class _BigQueryStore:
    """google-cloud-bigquery-backed verification client over the bigquery backend
    the test declared. Reads/drops in the connection's project + dataset."""

    def __init__(self, backend) -> None:
        self._project = backend.project
        self._dataset = backend.dataset
        self._client = _bq_client(backend)

    def _fq(self, table: str) -> str:
        return f"`{self._project}`.`{self._dataset}`.`{table}`"

    def read_table(self, table: str) -> pl.DataFrame:
        rows = self._client.query(f"SELECT * FROM {self._fq(table)}").result()
        return pl.DataFrame([dict(row) for row in rows])

    def count(self, table: str) -> int:
        rows = self._client.query(
            f"SELECT count(*) AS n FROM {self._fq(table)}"
        ).result()
        return list(rows)[0]["n"]

    def drop_table(self, table: str) -> None:
        self._client.delete_table(
            f"{self._project}.{self._dataset}.{table}", not_found_ok=True
        )


@pytest.fixture
def bigquery():
    """Factory building a ``_BigQueryStore`` over the bigquery backend the test
    declared -- call it with ``domain_bigquery``."""
    return _BigQueryStore


@pytest.fixture
def staging():
    """Factory building the BigQuery ``GCSLocation`` load-staging area from the
    bigquery backend's own bucket + service-account credentials, under a fresh
    unique prefix each call -- call it with ``domain_bigquery``."""

    def make(backend) -> GCSLocation:
        base = str(backend.base_path).strip("/") or f"bigquery/{backend.dataset}"
        return GCSLocation(
            bucket=backend.bucket,
            credentials=GCPJson(json=Secret(backend.service_account_key)),
            base_path=f"/{base}/bq-staging/{uuid.uuid4().hex}",
        )

    return make
