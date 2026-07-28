#
# Copyright 2026 Tabsdata Inc.
#

"""Adapters from domain fixture to connector connections and driver direct access"""

from __future__ import annotations

import json
import uuid

import polars as pl
import requests
from requests.auth import HTTPBasicAuth
from tabsdatax.itest.fixture.backends.azureblob import AzureblobDomain
from tabsdatax.itest.fixture.backends.druid import DruidDomain
from tabsdatax.itest.fixture.backends.s3 import S3Domain
from tenacity import retry, retry_if_exception_type, stop_after_delay, wait_fixed

from tabsdatak.api import Secret
from tabsdatak.conn.common.types import (
    AwsAccessSecretKey,
    AzureAccountKey,
    UserPassword,
)
from tabsdatak.conn.druid import (
    AzBlobLocation,
    CloudLocation,
    DruidDestConn,
    S3Location,
)

# Druid quotes identifiers with double quotes.
DELIMITERS = ('"', '"')

StagingDomain = S3Domain | AzureblobDomain

# Druid never inserts row by row: it lands parquet in a cloud bucket and runs a
# native batch ingestion against it, so a write test runs once per staging
# provider. Only s3 and azblob: Druid's `google` input source takes no per-task
# credentials, so GCS staging has no public surface (see the commented-out
# GCSLocation in the connector).
STAGING_PROVIDERS = ("s3", "azblob")


def staging_domain(
    provider: str,
    domain_s3: S3Domain,
    domain_azureblob: AzureblobDomain,
) -> StagingDomain:
    """The object-store domain a :data:`STAGING_PROVIDERS` id names."""
    return {"s3": domain_s3, "azblob": domain_azureblob}[provider]


# A Druid ingestion task reports SUCCESS before the Broker can resolve the
# datasource, and segment handoff lags further still, so every read polls.
POLL_SECONDS = 5
POLL_TIMEOUT_SECONDS = 180

# Druid needs a primary timestamp column on every datasource; seeded rows carry
# none, so the ingestion spec names one that is not there and lets Druid fill it
# with a constant -- the same trick the plugin uses for a `time_column`-less
# DruidDest.
_SEED_TIME_COLUMN = "__td_seed_time"
_SEED_TIME_VALUE = "2000-01-01T00:00:00.000Z"


def dest_conn(domain: DruidDomain, staging: StagingDomain) -> DruidDestConn:
    return DruidDestConn(
        overlord_url=domain.overlord_url,
        staging=_staging(staging),
        credentials=UserPassword(
            user=Secret(domain.user), password=Secret(domain.password)
        ),
    )


def explorer_conn(domain: DruidDomain) -> DruidDestConn:
    """A connection for the explorer, which reads the catalog over Druid SQL and
    stages nothing -- so ``overlord_url`` carries the router URL (SQL is served by
    the Broker, which the router proxies, and the explorer builds its URL from the
    host and port it finds here) and the mandatory ``staging`` field gets a
    location that is never reached."""
    return DruidDestConn(
        overlord_url=domain.router_url,
        staging=S3Location(
            bucket="unused",
            region="us-east-1",
            credentials=AwsAccessSecretKey(
                access_key_id=Secret("unused"), secret_access_key=Secret("unused")
            ),
        ),
        credentials=UserPassword(
            user=Secret(domain.user), password=Secret(domain.password)
        ),
    )


def store(domain: DruidDomain) -> DruidStore:
    """A store to seed / read back / drop what a test wrote, over the domain's own
    endpoints and credentials."""
    return DruidStore(domain)


class DruidStore:
    """A Druid-SQL store over the domain's router, with ingestion straight to its
    overlord. The connector only ever writes, so every verification goes through
    this rather than through it.

    A Druid table is a datasource: it is created by ingesting into it, and
    dropping it means marking its segments unused.
    """

    def __init__(self, domain: DruidDomain) -> None:
        self._sql_url = f"{domain.router_url}/druid/v2/sql"
        self._task_url = f"{domain.overlord_url}/druid/indexer/v1/task"
        self._datasource_url = f"{domain.router_url}/druid/coordinator/v1/datasources"
        self._auth = HTTPBasicAuth(domain.user, domain.password)

    def quote(self, identifier: str) -> str:
        open_, close = DELIMITERS
        return f"{open_}{identifier}{close}"

    def read_table(self, table: str, rows: int) -> pl.DataFrame:
        """Everything `table` holds once all `rows` are queryable, without the
        synthetic ``__time`` column Druid adds on ingestion."""
        frame = pl.DataFrame(self._wait_rows(table, rows))
        return frame.drop("__time") if "__time" in frame.columns else frame

    def wait_count(self, table: str, expected: int) -> int:
        """`table`'s row count, once it reaches `expected`."""

        @retry(
            retry=retry_if_exception_type(Exception),
            wait=wait_fixed(POLL_SECONDS),
            stop=stop_after_delay(POLL_TIMEOUT_SECONDS),
            reraise=True,
        )
        def poll() -> int:
            got = self._count(table)
            assert got == expected, f"{table}: expected {expected} rows, got {got}"
            return got

        return poll()

    def dimensions(self, table: str) -> set[str]:
        """The columns `table`'s live segments hold, straight from ``sys.segments``
        -- so not the synthetic ``__time``, and not a column of a load that has
        been replaced. The Broker's SQL catalog keeps reporting a replaced
        datasource's old columns (as all-null) until its next metadata refresh, a
        minute out by default, so a test asserting on the ingested schema reads the
        segments rather than a ``SELECT *``."""
        rows = self._query(
            "SELECT dimensions FROM sys.segments "
            "WHERE datasource = ? AND is_active = 1",
            table,
        )
        return {name for row in rows for name in json.loads(row["dimensions"])}

    def drop_table(self, table: str) -> None:
        # Best-effort: a datasource an aborted write never created answers 404, and
        # teardown must not mask the failure that got it there.
        requests.delete(f"{self._datasource_url}/{table}", auth=self._auth, timeout=30)

    def create_int_table(self, table: str, rows: list[dict]) -> None:
        """Create/replace `table` (integer ``id`` + text ``name``) seeded with
        `rows`, and return once every row is queryable. Ingestion is inline, so no
        cloud staging is involved -- the explorer's catalog surface is all this
        seeds for."""
        self.drop_table(table)
        task = {
            "type": "index_parallel",
            "spec": {
                "dataSchema": {
                    "dataSource": table,
                    "timestampSpec": {
                        "column": _SEED_TIME_COLUMN,
                        "format": "auto",
                        "missingValue": _SEED_TIME_VALUE,
                    },
                    "dimensionsSpec": {"useSchemaDiscovery": True},
                    "granularitySpec": {
                        "segmentGranularity": "ALL",
                        "queryGranularity": "none",
                        "rollup": False,
                        "intervals": ["1970-01-01/2100-01-01"],
                    },
                },
                "ioConfig": {
                    "type": "index_parallel",
                    "inputSource": {
                        "type": "inline",
                        "data": "\n".join(json.dumps(row) for row in rows),
                    },
                    "inputFormat": {"type": "json"},
                    "dropExisting": True,
                },
                "tuningConfig": {"type": "index_parallel"},
            },
        }
        response = requests.post(self._task_url, json=task, auth=self._auth, timeout=30)
        response.raise_for_status()
        self._wait_task_success(response.json()["task"])
        self._wait_rows(table, len(rows))

    def _query(self, sql: str, *parameters: str) -> list[dict]:
        payload = {
            "query": sql,
            "parameters": [{"type": "VARCHAR", "value": p} for p in parameters],
        }
        response = requests.post(
            self._sql_url, json=payload, auth=self._auth, timeout=30
        )
        response.raise_for_status()
        return response.json()

    def _count(self, table: str) -> int:
        rows = self._query(f"SELECT COUNT(*) AS n FROM {self.quote(table)}")
        return rows[0]["n"] if rows else 0

    def _wait_rows(self, table: str, rows: int) -> list[dict]:
        @retry(
            retry=retry_if_exception_type(Exception),
            wait=wait_fixed(POLL_SECONDS),
            stop=stop_after_delay(POLL_TIMEOUT_SECONDS),
            reraise=True,
        )
        def poll() -> list[dict]:
            got = self._query(f"SELECT * FROM {self.quote(table)}")
            assert len(got) == rows, f"{table}: expected {rows} rows, got {len(got)}"
            return got

        return poll()

    @retry(
        retry=retry_if_exception_type(AssertionError),
        wait=wait_fixed(POLL_SECONDS),
        stop=stop_after_delay(POLL_TIMEOUT_SECONDS),
        reraise=True,
    )
    def _wait_task_success(self, task_id: str) -> None:
        response = requests.get(
            f"{self._task_url}/{task_id}/status", auth=self._auth, timeout=30
        )
        response.raise_for_status()
        status = response.json()["status"]["status"]
        if status in ("FAILED", "UNKNOWN"):
            raise RuntimeError(f"seed ingestion task {task_id} ended {status}")
        assert status == "SUCCESS", f"seed ingestion task {task_id} still {status}"


def _staging(domain: StagingDomain) -> CloudLocation:
    """The staging area built from an object-store domain's own credentials. Each
    call takes a fresh prefix so two writes never share a subtree."""
    base_path = f"/{str(domain.base_path).strip('/')}/staging/{uuid.uuid4().hex}"
    if isinstance(domain, S3Domain):
        return S3Location(
            bucket=domain.bucket,
            region=domain.region,
            credentials=AwsAccessSecretKey(
                access_key_id=Secret(domain.access_key),
                secret_access_key=Secret(domain.secret_key),
            ),
            base_path=base_path,
        )
    if isinstance(domain, AzureblobDomain):
        return AzBlobLocation(
            container=domain.container,
            credentials=AzureAccountKey(
                account_name=Secret(domain.account_name),
                account_key=Secret(domain.account_key),
            ),
            base_path=base_path,
        )
    raise TypeError(f"unsupported staging domain: {type(domain).__name__}")
