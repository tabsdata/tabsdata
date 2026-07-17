#
# Copyright 2026 Tabsdata Inc.
#

"""Shared Druid test fixtures for this package's integration and e2e suites.

Druid is a destination-only connector (no ``DruidSrc``) that ingests *through*
cloud staging: the plugin writes parquet to an object store, then POSTs an
ingestion task to the Overlord and polls it. A test therefore declares the
``@druid_domain`` load target (Overlord/Router URLs + basic-auth creds) plus an
``@s3_domain`` backend purely to build the staging ``S3Location`` from its
credentials -- no separate staging secrets needed.

Ingestion is asynchronous and Druid is eventually consistent: the Overlord
accepts a task before the Broker can resolve the datasource, and segment handoff
adds further delay, so verification queries poll until the datasource reaches the
expected state. Writes are verified over Druid SQL (``POST
{router_url}/druid/v2/sql`` with basic auth), never through the connector.
Datasources are created implicitly by ingestion, so tests need no pre-created
table; each uses a unique scratch datasource name, dropped on teardown."""

from __future__ import annotations

import polars as pl
import pytest
import requests
from requests.auth import HTTPBasicAuth
from tabsdatax.itest.fixture.backends.druid import DruidDomain
from tenacity import (
    retry,
    retry_if_exception_type,
    stop_after_delay,
    wait_fixed,
)

from tabsdatak.api import Secret
from tabsdatak.conn.druid import AwsAccessSecretKey, S3Location


class _DruidStore:
    """Druid-SQL verification client over the druid backend the test declared.

    Queries run against the Router (which proxies the Broker for SQL and the
    Coordinator for datasource management). All reads poll to absorb Druid's
    asynchronous ingestion + segment-handoff latency."""

    def __init__(self, backend: DruidDomain) -> None:
        self._sql_url = f"{backend.router_url}/druid/v2/sql"
        self._datasources_url = f"{backend.router_url}/druid/coordinator/v1/datasources"
        self._auth = HTTPBasicAuth(backend.user, backend.password)

    def _query(self, sql: str) -> list[dict]:
        resp = requests.post(
            self._sql_url, json={"query": sql}, auth=self._auth, timeout=30
        )
        resp.raise_for_status()
        return resp.json()

    def wait_count(self, datasource: str, expected: int, timeout: int = 180) -> int:
        @retry(
            retry=retry_if_exception_type(Exception),
            wait=wait_fixed(5),
            stop=stop_after_delay(timeout),
            reraise=True,
        )
        def _poll() -> int:
            rows = self._query(f'SELECT COUNT(*) AS n FROM "{datasource}"')
            got = rows[0]["n"] if rows else 0
            if got != expected:
                raise AssertionError(
                    f"expected {expected} rows in {datasource}, got {got}"
                )
            return got

        return _poll()

    def read_table(
        self, datasource: str, expected_rows: int, timeout: int = 180
    ) -> pl.DataFrame:
        @retry(
            retry=retry_if_exception_type(Exception),
            wait=wait_fixed(5),
            stop=stop_after_delay(timeout),
            reraise=True,
        )
        def _poll() -> list[dict]:
            rows = self._query(f'SELECT * FROM "{datasource}"')
            if len(rows) != expected_rows:
                raise AssertionError(
                    f"expected {expected_rows} rows in {datasource}, got {len(rows)}"
                )
            return rows

        rows = _poll()
        df = pl.DataFrame(rows)
        if "__time" in df.columns:
            df = df.drop("__time")
        return df

    def drop_datasource(self, datasource: str) -> None:
        try:
            requests.delete(
                f"{self._datasources_url}/{datasource}",
                auth=self._auth,
                timeout=30,
            )
        except Exception:
            pass


@pytest.fixture
def druid():
    """Factory building a ``_DruidStore`` over the druid backend the test
    declared -- call it with ``domain_druid``."""
    return _DruidStore


@pytest.fixture
def staging():
    """Factory building the Druid staging ``S3Location`` from an ``s3`` backend's
    credentials -- call it with ``domain_s3`` and a unique ``base_path`` so
    concurrent runs never share a prefix."""

    def make(backend, base_path: str) -> S3Location:
        return S3Location(
            bucket=backend.bucket,
            region=backend.region,
            credentials=AwsAccessSecretKey(
                access_key_id=Secret(backend.access_key),
                secret_access_key=Secret(backend.secret_key),
            ),
            base_path=base_path,
        )

    return make
