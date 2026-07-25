#
# Copyright 2026 Tabsdata Inc.
#

"""ClickHouse connector -- public API.

Six classes, imported from `tabsdatak.conn.clickhouse`:

* `ClickHouseDestConn` -- the connection: `host` / `port`,
  `UserPassword` credentials, a `database` and a `staging` cloud
  location.
* `ClickHouseDest` -- the `@subscriber` destination: a list of
  target `tables`, one output slot per table. Writes land parquet in
  the staging location, then read it back through ClickHouse's
  `s3()` / `azureBlobStorage()` table function (no row-by-row
  inserts).
* `CloudLocation` and its concrete forms `S3Location` /
  `AzBlobLocation` / `GCSLocation` -- the `staging` area the
  destination lands parquet in.

ClickHouse is destination-only -- there is no `ClickHouseSrc`. The
connection (with credentials) is registered on a collection, not
passed to a decorator; the runtime resolves it at run time. A flow
function only references `ClickHouseDest`.

Examples:
    .. code-block:: python

        @subscriber(destination=ClickHouseDest(tables=["cap_rows"]),
                    input_tables=["mycoll/cities"])
        def publish(rows: TableFrameSpec) -> TableFrameSpec:
            return rows
`port` and `secure` are string-or-secret fields; the plugin reads
their parsed values through the `_port` / `_secure` getters.
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
from tabsdatak.conn.clickhouse import _validation
from tabsdatak.conn.clickhouse.error import ClickHouseErrorCode

# noinspection PyProtectedMember
from tabsdatak.conn.common import _validation as _common_validation

# noinspection PyProtectedMember
from tabsdatak.conn.common.types import (
    AwsAccessSecretKey,
    AwsRegionSpec,
    AzContainerSpec,
    AzureAccountKey,
    BoolSpec,
    CfgSpec,
    CloudRelativePathSpec,
    EntityWhitelistSpec,
    GCPHmac,
    GCPJson,
    GCSBucketSpec,
    HostSpec,
    PortSpec,
    S3BucketSpec,
    StrOrSecretConstraints,
    UserPassword,
    _convert,
)
from tabsdatak.spi import Conn

# --- Type aliases ----------------------------------------------------

CredentialsSpec: TypeAlias = UserPassword
"""The credential type for ClickHouse connections: `UserPassword`
(user + password)."""


# host / port / secure are the shared string-or-secret specs from
# `tabsdatak.conn.common.types` (HostSpec / PortSpec / BoolSpec). Read
# their parsed values through the `_host` / `_port` / `_secure` getters.

# Database: ClickHouse identifier shape (alphanumeric + underscore).
_IDENTIFIER_PATTERN = r"^(?:[A-Za-z_][A-Za-z0-9_]*)$"
DatabaseSpec: TypeAlias = Annotated[
    StrOrSecretSpec, StrOrSecretConstraints(pattern=_IDENTIFIER_PATTERN)
]
ClickHouseTableNameSpec: TypeAlias = Annotated[
    str,
    _validation.CLICKHOUSE_TABLE_FQN_VALIDATOR,
]
ClickHouseTableNamesSpec: TypeAlias = list[ClickHouseTableNameSpec]
IfTableExistsSpec: TypeAlias = Literal["append", "replace"]
SchemaEvolutionSpec: TypeAlias = Literal["strict", "iceberg"]


# --- ClickHouseDest.dest_cfg keys ------------------------------------
#
# Restricts `ClickHouseDest.dest_cfg` to the keys the destination
# plugin actually consumes. Pydantic enforces the literal membership
# of keys at validation time, so unknown keys are rejected at
# construction; `CONN_CFG_VALIDATOR` reuses the conn_cfg basic-value
# checks (str/int/float/bool/None, plus list/dict compositions).
DestCfgKey: TypeAlias = Literal["clickhouse.logging_level",]

DestCfgSpec: TypeAlias = Annotated[
    Mapping[DestCfgKey, Any], _common_validation.CONN_CFG_VALIDATOR
]


# --- Cloud locations (clickhouse-local) ------------------------------
#
# ClickHouse ingests via cloud-staged bulk load and reads the staged
# files through its native `s3()` / `azureBlobStorage()` table
# functions. The location types live here (rather than reusing
# `tabsdatak.conn.common.types`) so the GCS variant can carry
# `GCPHmac` credentials directly: ClickHouse reads GCS exclusively
# through the S3-compatible endpoint and has no path that uses the
# GCS-native `GCPJson` service-account credential. Keeping the
# types local also lets the dataclass init failures surface via
# `ClickHouseErrorCode`.


class CloudLocation(_api.DescriptorBase, ABC):
    """Abstract base for a ClickHouse `staging` cloud location.

    :categories: destination

    Concrete forms are `S3Location` / `AzBlobLocation` /
    `GCSLocation`; set one as `ClickHouseDestConn.staging`.
    """

    __kind__ = "clickHouseCloudLocation"


@_api.dataclass(ClickHouseErrorCode.CLICKHOUSE_8, kw_only=True)
class S3Location(CloudLocation):
    """S3 staging area for `ClickHouseDest`.

    :categories: destination

    The plugin lands parquet in this bucket, then references the object
    via ClickHouse's `s3()` table function to ingest it.

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

    __kind__ = "clickHouseS3Location"
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


@_api.dataclass(ClickHouseErrorCode.CLICKHOUSE_9, kw_only=True)
class AzBlobLocation(CloudLocation):
    """Azure Blob staging area for `ClickHouseDest`.

    :categories: destination

    The plugin uploads parquet to the container, then references it via
    ClickHouse's `azureBlobStorage()` table function to ingest it.

    Args:
        container: A valid Azure Blob container name.
        credentials: Azure storage account name / key credentials.
        base_path: Blob-name prefix from the container root; starts with
            `/`, has no trailing `/` and no empty segments (`//`)
            (the leading `/` is trimmed when forming the blob name).
            Defaults to `/`.
        cfg: Optional connector-config dict for the Azure Blob client.
    """

    __kind__ = "clickHouseAzBlobLocation"
    container: AzContainerSpec
    credentials: AzureAccountKey
    base_path: CloudRelativePathSpec = "/"
    cfg: CfgSpec | None = None

    def _container(self) -> str:
        return _convert(self.container, str)

    def _base_path(self) -> str:
        return _convert(self.base_path, str)


@_api.dataclass(ClickHouseErrorCode.CLICKHOUSE_10, kw_only=True)
class GCSLocation(CloudLocation):
    """GCS staging area for `ClickHouseDest`.

    :categories: destination

    Carries two credentials, one per code path: `credentials` (GCP
    service-account JSON) uploads parquet via the native GCS API, while
    `hmac_credentials` (GCS HMAC keys) let ClickHouse's `s3()` table
    function fetch the staged file back through the S3-compatible
    endpoint (the only protocol ClickHouse speaks to GCS). Both are
    required.

    Args:
        bucket: A valid GCS bucket name.
        credentials: GCP service-account JSON credentials (upload).
        hmac_credentials: GCS HMAC access-key / secret-key credentials
            (ClickHouse's S3-compatible read-back).
        base_path: Object-key prefix from the bucket root; starts with
            `/`, has no trailing `/` and no empty segments (`//`)
            (the leading `/` is trimmed when forming the key).
            Defaults to `/`.
        cfg: Optional connector-config dict for the GCS client.
    """

    __kind__ = "clickHouseGcsLocation"
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


@_api.dataclass(ClickHouseErrorCode.CLICKHOUSE_1, kw_only=True)
class ClickHouseDestConn(Conn):
    """ClickHouse connection.

    :categories: destination

    Registered on a collection; the runtime resolves it and hands it to
    the plugin at run time. Each string field accepts a plain string or
    a `Secret`; the plugin reads parsed values through the `_host` /
    `_port` / `_database` / `_secure` getters.

    Args:
        host: A DNS host name or IPv4 literal for the ClickHouse server.
        port: The HTTP(S) port, 1-65535. Defaults to `"8123"`.
        credentials: `UserPassword` (user + password).
        staging: A `CloudLocation` (`S3Location` / `AzBlobLocation`
            / `GCSLocation`) where parquet is staged before ingest.
        database: The default database (a ClickHouse identifier: a
            letter or underscore, then letters, digits and underscores).
            Defaults to `"default"`.
        secure: `true` for HTTPS, `false` for HTTP (boolean as
            text). Defaults to `"false"`.
        entity_whitelist: Optional list of regex patterns narrowing
            which tables the connector considers; `None` (the default)
            filters nothing.
        conn_cfg: Optional connector-config dict (see `CfgSpec`).
    """

    host: HostSpec
    port: PortSpec = "8123"
    credentials: CredentialsSpec
    staging: CloudLocation
    database: DatabaseSpec = "default"
    secure: BoolSpec = "false"
    entity_whitelist: EntityWhitelistSpec | None = None
    conn_cfg: CfgSpec | None = None

    def _host(self) -> str:
        return _convert(self.host, str)

    def _port(self) -> int:
        return _convert(self.port, int)

    def _database(self) -> str:
        return _convert(self.database, str)

    def _secure(self) -> bool:
        return _convert(self.secure, bool)


# --- Destination -----------------------------------------------------


@_api.dataclass(ClickHouseErrorCode.CLICKHOUSE_2, kw_only=True)
class ClickHouseDest(Dest):
    """ClickHouse destination for `@subscriber` -- one output slot per table.

    :categories: destination

    Args:
        tables: Target table names (non-empty), one write slot per
            entry. Each is a 1- or 2-part name (`table` or
            `database.table`); backtick-quote a part for special
            characters.
        if_table_exists: `"append"` (the default) adds rows;
            `"replace"` overwrites the table.
        schema_evolution: `"strict"` (the default) rejects schema
            changes; `"iceberg"` applies Iceberg-style schema deltas.
        dest_cfg: Optional connector config; the only supported key is
            `clickhouse.logging_level`.

    Examples:
        Write a table with the `replace` disposition -- the first run
        creates it, later runs drop and recreate it (row count stays):

        .. code-block:: python

            @subscriber(
                destination=ClickHouseDest(
                    tables=["cap_replace"],
                    if_table_exists="replace",
                ),
                input_tables=["caps_input/cap_rows"],
            )
            def out_replace(rows: TableFrameSpec) -> TableFrameSpec:
                return rows

        Append rows with a wider schema and let ClickHouse evolve the
        table -- `schema_evolution="iceberg"` adds the new column rather
        than failing on the mismatch (`"strict"` would reject it):

        .. code-block:: python

            @subscriber(
                destination=ClickHouseDest(
                    tables=["cap_evolve"],
                    if_table_exists="append",
                    schema_evolution="iceberg",
                ),
                input_tables=["caps_input/cap_evo_v2"],
            )
            def evolve(rows: TableFrameSpec) -> TableFrameSpec:
                return rows
    """

    tables: ClickHouseTableNamesSpec
    if_table_exists: IfTableExistsSpec = "append"
    schema_evolution: SchemaEvolutionSpec = "strict"
    dest_cfg: DestCfgSpec | None = None

    def validate(self) -> None:
        """Cross-field validation: `tables` must be non-empty.

        Called by the framework; raises `ClickHouseValidateException`.
        """
        if not self.tables:
            raise ClickHouseErrorCode.CLICKHOUSE_3.exception()
