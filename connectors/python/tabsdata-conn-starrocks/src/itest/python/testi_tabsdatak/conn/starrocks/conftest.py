#
# Copyright 2026 Tabsdata Inc.
#

"""Shared StarRocks test fixtures for this package's integration and e2e suites.

StarRocks is a destination-only connector (no ``StarRocksSrc``) that bulk-loads
*through* cloud staging, so a test declares the ``@starrocks_domain`` load target
plus one object-store backend (``@s3_domain`` / ``@azureblob_domain`` / ``@gcs_domain``)
purely to build the staging ``CloudLocation`` from its credentials -- no separate
staging secrets needed. The ``staging`` factory dispatches on the backend type, so
the integration tests parametrize the same body over S3, Azure Blob and GCS. (GCS
uploads via the native GCS API using ``GCPJson`` and is read back by StarRocks'
``FILES()`` through the S3-compatible endpoint using the ``GCPHmac`` keys.) Writes
are verified over StarRocks' MySQL wire protocol with a ``SqlAlchemyStore``
(backtick quoting), the same verification client the SQL connectors use."""

from __future__ import annotations

import pytest
from tabsdatax.itest.fixture.backends.azureblob import AzureblobDomain
from tabsdatax.itest.fixture.backends.gcs import GcsDomain
from tabsdatax.itest.fixture.backends.s3 import S3Domain
from tabsdatax.itest.fixture.functions.sql import SqlAlchemyStore

from tabsdatak.api import Secret
from tabsdatak.conn.starrocks import (
    AwsAccessSecretKey,
    AzBlobLocation,
    AzureAccountKey,
    GCPHmac,
    GCPJson,
    GCSLocation,
    S3Location,
)

# StarRocks bulk-load (INSERT ... SELECT FROM FILES) requires the target table to
# already exist -- the connector never issues DDL (StarRocks tables need explicit
# key/distribution that can't be inferred). Every test's data is (id, name), so a
# single duplicate-key schema serves them all; replication_num=1 for the single-BE
# all-in-one container.
_CREATE_DDL = (
    "CREATE TABLE {table} (id BIGINT, name VARCHAR(255)) "
    "DUPLICATE KEY(id) DISTRIBUTED BY HASH(id) BUCKETS 1 "
    'PROPERTIES ("replication_num" = "1")'
)


@pytest.fixture
def sql():
    """Factory building a ``SqlStore`` over the starrocks backend the test
    declared (MySQL protocol, backtick quoting) -- call it with
    ``domain_starrocks``."""
    return lambda backend: SqlAlchemyStore(
        backend.sqlalchemy_uri, delimiters=("`", "`")
    )


@pytest.fixture
def create_table():
    """Create (replacing any prior) an ``(id BIGINT, name VARCHAR)`` StarRocks
    table the connector can load into -- call it with ``domain_starrocks`` and the
    table name. The connector loads into existing tables only, so tests must
    provision the schema themselves."""
    from sqlalchemy import create_engine, text

    def make(backend, table: str) -> None:
        engine = create_engine(backend.sqlalchemy_uri)
        try:
            with engine.begin() as conn:
                conn.execute(text(f"DROP TABLE IF EXISTS `{table}`"))
                conn.execute(text(_CREATE_DDL.format(table=f"`{table}`")))
        finally:
            engine.dispose()

    return make


@pytest.fixture
def staging():
    """Factory building a StarRocks staging ``CloudLocation`` from an object-store
    backend's credentials, dispatching on the backend type -- call it with
    ``domain_s3``, ``domain_azureblob`` or ``domain_gcs`` and a unique ``base_path``
    so concurrent runs never share a prefix."""

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
