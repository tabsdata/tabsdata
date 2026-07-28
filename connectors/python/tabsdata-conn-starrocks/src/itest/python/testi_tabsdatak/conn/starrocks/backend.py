#
# Copyright 2026 Tabsdata Inc.
#

"""Adapters from domain fixture to connector connections and driver direct access"""

from __future__ import annotations

import uuid

import polars as pl
from tabsdatax.itest.fixture.backends.azureblob import AzureblobDomain
from tabsdatax.itest.fixture.backends.gcs import GcsDomain
from tabsdatax.itest.fixture.backends.s3 import S3Domain
from tabsdatax.itest.fixture.backends.starrocks import StarrocksDomain
from tabsdatax.itest.fixture.functions.sql import SqlAlchemyStore, sql_credentials

from tabsdatak.api import Secret
from tabsdatak.conn.common.types import (
    AwsAccessSecretKey,
    AzureAccountKey,
    GCPHmac,
    GCPJson,
)
from tabsdatak.conn.starrocks import (
    AzBlobLocation,
    CloudLocation,
    GCSLocation,
    S3Location,
    StarRocksDestConn,
)

# StarRocks speaks the MySQL wire protocol, which quotes identifiers with backticks.
DELIMITERS = ("`", "`")

StagingDomain = S3Domain | AzureblobDomain | GcsDomain

# StarRocks never inserts row by row: it lands parquet in a cloud bucket and
# ingests it with INSERT ... SELECT FROM FILES(), so a write test runs once per
# staging provider. GCS is reached through its S3-compatible endpoint, hence the
# HMAC keys.
STAGING_PROVIDERS = ("s3", "azblob", "gcs")

# The explorer reads the catalog over the MySQL protocol and stages nothing, so the
# mandatory ``staging`` field gets a location that is never reached.
UNUSED_STAGING = S3Location(
    bucket="unused",
    region="us-east-1",
    credentials=AwsAccessSecretKey(
        access_key_id=Secret("unused"), secret_access_key=Secret("unused")
    ),
)

# The polars types the tests load, in StarRocks spelling.
_COLUMN_TYPES = {
    pl.Int64: "BIGINT",
    pl.String: "VARCHAR(255)",
    pl.Date: "DATE",
}


def staging_domain(
    provider: str,
    domain_s3: S3Domain,
    domain_azureblob: AzureblobDomain,
    domain_gcs: GcsDomain,
) -> StagingDomain:
    """The object-store domain a :data:`STAGING_PROVIDERS` id names."""
    return {"s3": domain_s3, "azblob": domain_azureblob, "gcs": domain_gcs}[provider]


def dest_conn(domain: StarrocksDomain, staging: StagingDomain) -> StarRocksDestConn:
    return StarRocksDestConn(
        host=domain.host,
        port=str(domain.port),
        credentials=sql_credentials(domain),
        staging=_staging(staging),
        database=domain.database,
    )


def explorer_conn(domain: StarrocksDomain) -> StarRocksDestConn:
    """A connection for the explorer, over :data:`UNUSED_STAGING`."""
    return StarRocksDestConn(
        host=domain.host,
        port=str(domain.port),
        credentials=sql_credentials(domain),
        staging=UNUSED_STAGING,
        database=domain.database,
    )


def column_spec(schema: pl.Schema) -> dict[str, str]:
    """`schema` as the ``{column: starrocks_type}`` spec of a table holding a frame
    of that shape."""
    return {name: _COLUMN_TYPES[dtype.base_type()] for name, dtype in schema.items()}


def store(domain: StarrocksDomain) -> StarRocksStore:
    """A store to create / seed / read back / drop what a test wrote, over the
    domain's own credential-bearing URI."""
    return StarRocksStore(domain)


class StarRocksStore(SqlAlchemyStore):
    """A :class:`SqlAlchemyStore` that also creates and seeds tables: the connector
    only ever loads into an existing table (a StarRocks table needs a key and a
    distribution that cannot be inferred from the data), so a test provisions its
    own target."""

    def __init__(self, domain: StarrocksDomain) -> None:
        super().__init__(domain.sqlalchemy_uri, delimiters=DELIMITERS)

    def create_table(self, table: str, columns: dict[str, str]) -> None:
        """Create (replacing any prior) `table` from a ``{column: starrocks_type}``
        spec -- see :func:`column_spec`."""
        from sqlalchemy import text

        spec = ", ".join(
            f"{self.quote(name)} {type_}" for name, type_ in columns.items()
        )
        with self._engine.begin() as conn:
            conn.execute(text(f"DROP TABLE IF EXISTS {self.quote(table)}"))
            conn.execute(text(f"CREATE TABLE {self.quote(table)} ({spec})"))

    def insert_rows(self, table: str, rows: list[dict]) -> None:
        from sqlalchemy import text

        names = list(rows[0])
        columns = ", ".join(self.quote(name) for name in names)
        values = ", ".join(f":{name}" for name in names)
        with self._engine.begin() as conn:
            conn.execute(
                text(f"INSERT INTO {self.quote(table)} ({columns}) VALUES ({values})"),
                rows,
            )


def _staging(domain: StagingDomain) -> CloudLocation:
    """The staging area built from an object-store domain's own credentials. Each
    call takes a fresh prefix, so the parquet one write leaves in the bucket is
    never reachable from another write's ``FILES()`` path."""
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
    if isinstance(domain, GcsDomain):
        return GCSLocation(
            bucket=domain.bucket,
            credentials=GCPJson(json=Secret(domain.service_account_key)),
            hmac_credentials=GCPHmac(
                access_key=Secret(domain.hmac_access_key),
                secret_key=Secret(domain.hmac_secret_key),
            ),
            base_path=base_path,
        )
    raise TypeError(f"unsupported staging domain: {type(domain).__name__}")
