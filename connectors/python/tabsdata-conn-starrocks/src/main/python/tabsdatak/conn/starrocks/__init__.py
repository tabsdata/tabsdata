#
# Copyright 2026 Tabsdata Inc.
#

"""StarRocks connector -- public API.

Six classes, imported from `tabsdatak.conn.starrocks`:

* `StarRocksDestConn` -- the connection: `host` / `port`,
  `UserPassword` credentials, a `database` and a `staging` cloud
  location.
* `StarRocksDest` -- the `@subscriber` destination: a list of
  target `tables`, one output slot per table. Writes stage parquet
  in the cloud location, then ingest it with StarRocks' native
  `FILES()` table function.
* `CloudLocation` and its concrete forms `S3Location` /
  `AzBlobLocation` / `GCSLocation` -- the `staging` area the
  destination lands parquet in.

StarRocks is destination-only -- there is no `StarRocksSrc`. The
connection (with credentials) is registered on a collection, not
passed to a decorator; the runtime resolves it at run time. A flow
function only references `StarRocksDest`.

Examples:
    .. code-block:: python

        @subscriber(destination=StarRocksDest(tables=["wld_country"]),
                    input_tables=["mycoll/cities"])
        def publish(rows: TableFrameSpec) -> TableFrameSpec:
            return rows
`port` is a string-or-secret field; the plugin reads its parsed value
through the `_port` getter.
"""

from __future__ import annotations

from abc import ABC
from collections.abc import Mapping
from typing import Annotated, Any, Literal, TypeAlias

# noinspection PyProtectedMember
import tabsdatak._api as _api

# noinspection PyProtectedMember
from tabsdatak.api import (
    Dest,
    StrOrSecretSpec,
)

# noinspection PyProtectedMember
from tabsdatak.conn.common import _validation as _common_validation

# noinspection PyProtectedMember
from tabsdatak.conn.common.types import (
    AwsAccessSecretKey,
    AwsRegionSpec,
    AzContainerSpec,
    AzureAccountKey,
    CfgSpec,
    CloudRelativePathSpec,
    EntityWhitelistSpec,
    GCPHmac,
    GCPJson,
    GCSBucketSpec,
    HostSpec,
    PortSpec,
    S3BucketSpec,
    UserPassword,
    _convert,
)

# noinspection PyProtectedMember
from tabsdatak.conn.starrocks import _validation
from tabsdatak.conn.starrocks.error import StarRocksErrorCode
from tabsdatak.spi import Conn

# --- Type aliases ----------------------------------------------------

CredentialsSpec: TypeAlias = UserPassword
"""The credential type for StarRocks connections: `UserPassword`
(user + password)."""

# host / port are the shared string-or-secret specs from
# `tabsdatak.conn.common.types` (HostSpec / PortSpec). Read their parsed
# values through the `_host` / `_port` getters.

DatabaseSpec: TypeAlias = Annotated[
    StrOrSecretSpec,
    _validation.IDENTIFIER_VALIDATION,
]
StarRocksTableNameSpec: TypeAlias = Annotated[
    str,
    _validation.STARROCKS_TABLE_FQN_VALIDATION,
]
StarRocksTableNamesSpec: TypeAlias = list[StarRocksTableNameSpec]
IfTableExistsSpec: TypeAlias = Literal["append", "replace"]
SchemaEvolutionSpec: TypeAlias = Literal["strict", "iceberg"]


# --- StarRocksDest.dest_cfg keys ------------------------------------
DestCfgKey: TypeAlias = Literal["starrocks.logging_level",]

DestCfgSpec: TypeAlias = Annotated[
    Mapping[DestCfgKey, Any], _common_validation.CONN_CFG_VALIDATION
]


# --- Cloud locations -------------------------------------------------
#
# StarRocks carries a per-Conn `staging` field whose value is one of
# the concrete CloudLocation subclasses below. They mirror the legacy
# conn-common shapes but live here so starrocks owns its own location
# surface and routes init failures through StarRocksErrorCode.


class CloudLocation(_api.DescriptorBase, ABC):
    """Abstract base for a StarRocks `staging` cloud location.

    :categories: destination

    Concrete forms are `S3Location` / `AzBlobLocation` /
    `GCSLocation`; set one as `StarRocksDestConn.staging`.
    """

    __kind__ = "starRocksCloudLocation"


@_api.dataclass(StarRocksErrorCode.STARROCKS_8, kw_only=True)
class S3Location(CloudLocation):
    """S3 staging area for `StarRocksDest`.

    :categories: destination

    The plugin uploads parquet here, then ingests it via a StarRocks
    `FILES()` reference.

    Args:
        bucket: A valid S3 bucket name.
        region: A valid AWS region, e.g. `us-east-1`.
        credentials: AWS access-key / secret-key credentials.
        base_path: Object-key prefix from the bucket root; starts with
            `/`, has no trailing `/` and no empty segments (`//`)
            (the leading `/` is trimmed when forming the key).
            Defaults to `/`.
        cfg: Optional connector-config dict for the S3 client.
    """

    __kind__ = "starRocksS3Location"
    bucket: S3BucketSpec
    region: AwsRegionSpec
    credentials: AwsAccessSecretKey
    base_path: CloudRelativePathSpec = "/"
    cfg: CfgSpec | None = None

    def _bucket(self) -> str:
        return _convert(self.bucket, str)

    def _region(self) -> str:
        return _convert(self.region, str)

    def _base_path(self) -> str:
        return _convert(self.base_path, str)


@_api.dataclass(StarRocksErrorCode.STARROCKS_9, kw_only=True)
class AzBlobLocation(CloudLocation):
    """Azure Blob staging area for `StarRocksDest`.

    :categories: destination

    The plugin uploads parquet to the container, then ingests it via a
    StarRocks `FILES()` reference.

    Args:
        container: A valid Azure Blob container name.
        credentials: Azure storage account name / key credentials.
        base_path: Blob-name prefix from the container root; starts with
            `/`, has no trailing `/` and no empty segments (`//`)
            (the leading `/` is trimmed when forming the blob name).
            Defaults to `/`.
        cfg: Optional connector-config dict for the Azure Blob client.
    """

    __kind__ = "starRocksAzBlobLocation"
    container: AzContainerSpec
    credentials: AzureAccountKey
    base_path: CloudRelativePathSpec = "/"
    cfg: CfgSpec | None = None

    def _container(self) -> str:
        return _convert(self.container, str)

    def _base_path(self) -> str:
        return _convert(self.base_path, str)


@_api.dataclass(StarRocksErrorCode.STARROCKS_10, kw_only=True)
class GCSLocation(CloudLocation):
    """GCS staging area for `StarRocksDest`.

    :categories: destination

    Carries two credentials, one per code path (mirroring ClickHouse):
    `credentials` (GCP service-account JSON) uploads parquet via the
    native GCS API, while `hmac_credentials` (GCS HMAC keys) let
    StarRocks' `FILES()` table function read the staged file back
    through the S3-compatible endpoint (`https://storage.googleapis.com`,
    the only protocol StarRocks speaks to GCS). Both are required.

    Args:
        bucket: A valid GCS bucket name.
        credentials: GCP service-account JSON credentials (upload).
        hmac_credentials: GCS HMAC access-key / secret-key credentials
            (StarRocks' S3-compatible read-back).
        base_path: Object-key prefix from the bucket root; starts with
            `/`, has no trailing `/` and no empty segments (`//`)
            (the leading `/` is trimmed when forming the key).
            Defaults to `/`.
        cfg: Optional connector-config dict for the GCS client.
    """

    __kind__ = "starRocksGCSLocation"
    bucket: GCSBucketSpec
    credentials: GCPJson
    hmac_credentials: GCPHmac
    base_path: CloudRelativePathSpec = "/"
    cfg: CfgSpec | None = None

    def _bucket(self) -> str:
        return _convert(self.bucket, str)

    def _base_path(self) -> str:
        return _convert(self.base_path, str)


# --- Connection ------------------------------------------------------


@_api.dataclass(StarRocksErrorCode.STARROCKS_1, kw_only=True)
class StarRocksDestConn(Conn):
    """StarRocks connection.

    :categories: destination

    Registered on a collection; the runtime resolves it and hands it to
    the plugin at run time. Each string field accepts a plain string or
    a `Secret`; the plugin reads parsed values through the `_host` /
    `_port` / `_database` getters.

    Args:
        host: A DNS host name or IPv4 literal for the StarRocks FE.
        port: The MySQL-protocol port, 1-65535. Defaults to `"9030"`.
        credentials: `UserPassword` (user + password).
        staging: A `CloudLocation` (`S3Location` / `AzBlobLocation`
            / `GCSLocation`) where parquet is staged before ingest.
        database: The default database (a StarRocks identifier: a letter
            or underscore, then letters, digits and underscores).
            Defaults to `"default"`.
        entity_whitelist: Optional list of regex patterns narrowing
            which tables the connector considers; `None` (the default)
            filters nothing.
        conn_cfg: Optional connector-config dict (see `CfgSpec`).
    """

    host: HostSpec
    port: PortSpec = "9030"
    credentials: CredentialsSpec
    staging: CloudLocation
    database: DatabaseSpec = "default"
    entity_whitelist: EntityWhitelistSpec | None = None
    conn_cfg: CfgSpec | None = None

    def _host(self) -> str:
        return _convert(self.host, str)

    def _port(self) -> int:
        return _convert(self.port, int)

    def _database(self) -> str:
        return _convert(self.database, str)


# --- Destination -----------------------------------------------------


@_api.dataclass(StarRocksErrorCode.STARROCKS_2, kw_only=True)
class StarRocksDest(Dest):
    """StarRocks destination for `@subscriber` -- one output slot per table.

    :categories: destination

    The target tables must already exist; this destination does not
    create them (StarRocks table creation needs physical layout -- a key
    definition and a `DISTRIBUTED BY` clause -- that cannot be inferred
    from the data). Writing to a missing table fails with a StarRocks
    table-not-found error.

    Args:
        tables: Target table names, one write slot per entry; each must
            reference an existing table. Each is a 1- or 2-part name
            (`table` or `database.table`); backtick-quote a part for
            special characters.
        if_table_exists: `"append"` (the default) issues
            `INSERT INTO`; `"replace"` issues `TRUNCATE` +
            `INSERT INTO` against the existing table.
        schema_evolution: `"strict"` (the default) rejects schema
            changes; `"iceberg"` applies Iceberg-style schema deltas.
        dest_cfg: Optional connector config; the only supported key is
            `starrocks.logging_level`.

    Examples:
        Append to an existing table -- row count grows each run:

        .. code-block:: python

            @subscriber(
                destination=StarRocksDest(
                    tables=["wld_country"], if_table_exists="append"
                ),
                input_tables=["subs_tables/wld_country"],
            )
            def out_append(country: TableFrameSpec) -> TableFrameSpec:
                return country

        Replace truncates the target table, then reloads it:

        .. code-block:: python

            @subscriber(
                destination=StarRocksDest(
                    tables=["wld_city"], if_table_exists="replace"
                ),
                input_tables=["subs_tables/wld_city"],
            )
            def out_replace(city: TableFrameSpec) -> TableFrameSpec:
                return city
    """

    tables: StarRocksTableNamesSpec
    if_table_exists: IfTableExistsSpec = "append"
    schema_evolution: SchemaEvolutionSpec = "strict"
    dest_cfg: DestCfgSpec | None = None
