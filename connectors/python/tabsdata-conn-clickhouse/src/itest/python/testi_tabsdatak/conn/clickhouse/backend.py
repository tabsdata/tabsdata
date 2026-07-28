#
# Copyright 2026 Tabsdata Inc.
#

"""Adapters from domain fixture to connector connections and driver direct access"""

from __future__ import annotations

import uuid

import polars as pl
from tabsdatax.itest.fixture.backends.azureblob import AzureblobDomain
from tabsdatax.itest.fixture.backends.clickhouse import ClickhouseDomain
from tabsdatax.itest.fixture.backends.gcs import GcsDomain
from tabsdatax.itest.fixture.backends.s3 import S3Domain
from tabsdatax.itest.fixture.functions.sql import SqlStore, sql_credentials

from tabsdatak.api import Secret
from tabsdatak.conn.clickhouse import (
    AzBlobLocation,
    ClickHouseDestConn,
    CloudLocation,
    GCSLocation,
    S3Location,
)
from tabsdatak.conn.common.types import (
    AwsAccessSecretKey,
    AzureAccountKey,
    GCPHmac,
    GCPJson,
)

# ClickHouse quotes identifiers with backticks.
DELIMITERS = ("`", "`")

StagingDomain = S3Domain | AzureblobDomain | GcsDomain

# ClickHouse never inserts row by row: it lands parquet in a cloud bucket and
# ingests it from there, so a write test runs once per staging provider. GCS is
# reached through its S3-compatible endpoint, hence the HMAC keys.
STAGING_PROVIDERS = ("s3", "azblob", "gcs")


def staging_domain(
    provider: str,
    domain_s3: S3Domain,
    domain_azureblob: AzureblobDomain,
    domain_gcs: GcsDomain,
) -> StagingDomain:
    """The object-store domain a :data:`STAGING_PROVIDERS` id names."""
    return {"s3": domain_s3, "azblob": domain_azureblob, "gcs": domain_gcs}[provider]


def dest_conn(domain: ClickhouseDomain, staging: StagingDomain) -> ClickHouseDestConn:
    return ClickHouseDestConn(
        host=domain.host,
        port=str(domain.port),
        credentials=sql_credentials(domain),
        staging=_staging(staging),
        database=domain.database,
        secure="false",
    )


def explorer_conn(domain: ClickhouseDomain) -> ClickHouseDestConn:
    """A connection for the explorer, which reads the catalog over
    clickhouse-connect and stages nothing -- so the mandatory ``staging`` field
    gets a location that is never reached."""
    return ClickHouseDestConn(
        host=domain.host,
        port=str(domain.port),
        credentials=sql_credentials(domain),
        staging=S3Location(
            bucket="unused",
            region="us-east-1",
            credentials=AwsAccessSecretKey(
                access_key_id=Secret("unused"), secret_access_key=Secret("unused")
            ),
        ),
        database=domain.database,
        secure="false",
    )


def store(domain: ClickhouseDomain) -> ClickHouseStore:
    """A store to seed / read back / drop what a test wrote, over the domain's own
    credentials."""
    return ClickHouseStore(domain)


class ClickHouseStore(SqlStore):
    """A clickhouse_connect-backed :class:`SqlStore`. The connector only ever
    writes, so every verification goes through this rather than through it."""

    def __init__(self, domain: ClickhouseDomain) -> None:
        import clickhouse_connect

        self._database = domain.database
        self._client = clickhouse_connect.get_client(
            host=domain.host,
            port=str(domain.port),
            username=domain.user,
            password=domain.password,
            database=domain.database,
            secure="false",
        )

    def quote(self, identifier: str) -> str:
        open_, close = DELIMITERS
        return f"{open_}{identifier}{close}"

    def read_table(self, table: str) -> pl.DataFrame:
        result = self._client.query(f"SELECT * FROM {self._fqn(table)}")
        return pl.DataFrame(
            result.result_rows, schema=list(result.column_names), orient="row"
        )

    def drop_table(self, table: str) -> None:
        self._client.command(f"DROP TABLE IF EXISTS {self._fqn(table)}")

    def create_int_table(self, table: str, rows: list[dict]) -> None:
        fqn = self._fqn(table)
        self._client.command(f"DROP TABLE IF EXISTS {fqn}")
        self._client.command(
            f"CREATE TABLE {fqn} (id Int32, name String) ENGINE = MergeTree ORDER BY id"
        )
        self._client.insert(
            table=table,
            database=self._database,
            data=[[row["id"], row["name"]] for row in rows],
            column_names=["id", "name"],
        )

    def count(self, table: str) -> int:
        query = f"SELECT count() FROM {self._fqn(table)}"
        return self._client.query(query).result_rows[0][0]

    def column_types(self, table: str) -> dict[str, str]:
        """``{column: clickhouse_type}`` in position order, as the server holds it
        -- nullability rides in the type (``Nullable(String)``)."""
        rows = self._client.query(
            "SELECT name, type FROM system.columns "
            "WHERE database = {db:String} AND table = {tbl:String} "
            "ORDER BY position",
            parameters={"db": self._database, "tbl": table},
        ).result_rows
        return dict(rows)

    def _fqn(self, table: str) -> str:
        return f"{self.quote(self._database)}.{self.quote(table)}"


def _staging(domain: StagingDomain) -> CloudLocation:
    """The staging area built from an object-store domain's own credentials. Each
    call takes a fresh prefix: the loader's cleanup sweep removes every upload id
    up to the one it wrote, so two writes must not share a subtree unless they
    are meant to."""
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
