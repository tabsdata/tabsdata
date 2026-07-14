#
# Copyright 2026 Tabsdata Inc.
#

"""Shared ClickHouse test fixtures for this package's integration and e2e suites.

ClickHouse is a destination-only connector (no ``ClickHouseSrc``) that bulk-loads
*through* cloud staging, so a test declares the ``@clickhouse_domain`` load target
plus one object-store backend (``@s3_domain`` / ``@azureblob_domain`` /
``@gcs_domain``) purely to build the staging ``CloudLocation`` from its
credentials. The ``staging`` factory dispatches on the backend type, so the
integration tests parametrize the same body over all three providers -- unlike
StarRocks, ClickHouse supports GCS (via HMAC keys fed to its ``s3()`` path).
Unlike StarRocks, ClickHouse also creates the target table itself, so tests do not
pre-create it. Writes are verified with a ``clickhouse_connect`` HTTP client (the
connector never reads back)."""

from __future__ import annotations

import polars as pl
import pytest
from tabsdatax.itest.fixture.backends.azureblob import AzureblobDomain
from tabsdatax.itest.fixture.backends.gcs import GcsDomain
from tabsdatax.itest.fixture.backends.s3 import S3Domain

from tabsdatak.api import Secret
from tabsdatak.conn.clickhouse import (
    AwsAccessSecretKey,
    AzBlobLocation,
    AzureAccountKey,
    GCPHmac,
    GCPJson,
    GCSLocation,
    S3Location,
)


class _ClickHouseStore:
    """clickhouse_connect-backed verification client over the clickhouse backend
    the test declared. Reads land in the connection's default database (the same
    one the connector auto-qualifies bare table names into)."""

    def __init__(self, backend) -> None:
        import clickhouse_connect

        self._db = backend.database
        self._client = clickhouse_connect.get_client(
            host=backend.host,
            port=int(backend.port),
            username=backend.user,
            password=backend.password,
            database=backend.database,
            secure=False,
        )

    def _fq(self, table: str) -> str:
        return f"`{self._db}`.`{table}`"

    def read_table(self, table: str) -> pl.DataFrame:
        result = self._client.query(f"SELECT * FROM {self._fq(table)}")
        return pl.DataFrame(
            result.result_rows, schema=list(result.column_names), orient="row"
        )

    def count(self, table: str) -> int:
        return self._client.query(f"SELECT count() FROM {self._fq(table)}").result_rows[
            0
        ][0]

    def drop_table(self, table: str) -> None:
        self._client.command(f"DROP TABLE IF EXISTS {self._fq(table)}")


@pytest.fixture
def clickhouse():
    """Factory building a ``_ClickHouseStore`` over the clickhouse backend the test
    declared -- call it with ``domain_clickhouse``."""
    return _ClickHouseStore


@pytest.fixture
def staging():
    """Factory building a ClickHouse staging ``CloudLocation`` from an object-store
    backend's credentials, dispatching on the backend type -- call it with
    ``domain_s3`` / ``domain_azureblob`` / ``domain_gcs`` and a unique
    ``base_path`` so concurrent runs never share a prefix."""

    def make(backend, base_path: str):
        if isinstance(backend, S3Domain):
            return S3Location(
                bucket=backend.bucket,
                region=backend.region,
                credentials=AwsAccessSecretKey(
                    access_key_id=Secret(backend.access_key),
                    secret_access_key=Secret(backend.secret_key),
                ),
                base_path=base_path,
            )
        if isinstance(backend, AzureblobDomain):
            return AzBlobLocation(
                container=backend.container,
                credentials=AzureAccountKey(
                    account_name=Secret(backend.account_name),
                    account_key=Secret(backend.account_key),
                ),
                base_path=base_path,
            )
        if isinstance(backend, GcsDomain):
            return GCSLocation(
                bucket=backend.bucket,
                credentials=GCPJson(json=Secret(backend.service_account_key)),
                hmac_credentials=GCPHmac(
                    access_key=Secret(backend.hmac_access_key),
                    secret_key=Secret(backend.hmac_secret_key),
                ),
                base_path=base_path,
            )
        raise TypeError(f"unsupported staging backend: {type(backend).__name__}")

    return make
