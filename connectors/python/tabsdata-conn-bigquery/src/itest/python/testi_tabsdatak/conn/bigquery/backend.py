#
# Copyright 2026 Tabsdata Inc.
#

"""Adapters from domain fixture to connector connections and driver direct access"""

from __future__ import annotations

import json

import polars as pl
from tabsdatax.itest.fixture.backends.bigquery import BigQueryDomain
from tabsdatax.itest.fixture.functions.sql import SqlStore

from tabsdatak.api import Secret
from tabsdatak.conn.bigquery import BigQueryDestConn, GCPJson, GCSLocation

# BigQuery wraps a whole table FQN in one backtick pair, not each part.
DELIMITERS = ("`", "`")


def table_fqn(table: str) -> str:
    """`table` in the outer-backtick form ``BigQueryDest.tables`` requires."""
    open_, close = DELIMITERS
    return f"{open_}{table}{close}"


def dest_conn(domain: BigQueryDomain) -> BigQueryDestConn:
    """The connection, staging through the GCS bucket the bigquery domain itself
    carries -- the load job reads the parquet from there, so the same
    service-account key authenticates both the upload and the load."""
    credentials = GCPJson(json=Secret(domain.service_account_key))
    return BigQueryDestConn(
        staging=GCSLocation(
            bucket=domain.bucket,
            credentials=credentials,
            base_path=f"/{str(domain.base_path).strip('/')}/staging",
        ),
        credentials=credentials,
        project=domain.project,
        dataset=domain.dataset,
    )


def store(domain: BigQueryDomain) -> BigQueryStore:
    """A store to seed / read back / drop what a test wrote, over the domain's own
    credentials."""
    return BigQueryStore(domain)


class BigQueryStore(SqlStore):
    """A google-cloud-bigquery-backed :class:`SqlStore`. The connector only ever
    writes, so every verification goes through this rather than through it."""

    def __init__(self, domain: BigQueryDomain) -> None:
        from google.cloud import bigquery

        self._project = domain.project
        self._dataset = domain.dataset
        self._client = bigquery.Client.from_service_account_info(
            json.loads(domain.service_account_key), project=domain.project
        )

    def quote(self, identifier: str) -> str:
        open_, close = DELIMITERS
        return f"{open_}{identifier}{close}"

    def read_table(self, table: str) -> pl.DataFrame:
        rows = self._client.query(f"SELECT * FROM {self._fqn(table)}").result()
        return pl.DataFrame([dict(row) for row in rows])

    def drop_table(self, table: str) -> None:
        self._client.delete_table(self._path(table), not_found_ok=True)

    def create_int_table(self, table: str, rows: list[dict]) -> None:
        fqn = self._fqn(table)
        self._client.query(
            f"CREATE OR REPLACE TABLE {fqn} (id INT64, name STRING)"
        ).result()
        values = ", ".join(f"({row['id']}, '{row['name']}')" for row in rows)
        self._client.query(f"INSERT INTO {fqn} (id, name) VALUES {values}").result()

    def count(self, table: str) -> int:
        rows = self._client.query(
            f"SELECT count(*) AS n FROM {self._fqn(table)}"
        ).result()
        return list(rows)[0]["n"]

    def _path(self, table: str) -> str:
        return f"{self._project}.{self._dataset}.{table}"

    def _fqn(self, table: str) -> str:
        return self.quote(self._path(table))
