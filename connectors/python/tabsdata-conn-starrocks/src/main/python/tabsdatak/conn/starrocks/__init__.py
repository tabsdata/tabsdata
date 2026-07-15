"""StarRocks connector — public API.

* :class:`StarRocksDestConn` -- per-cluster StarRocks connection
  (host, port, credentials, database, cloud staging area).
* :class:`StarRocksDest` -- destination: list of table FQNs;
  one parquet slot per table. Writes go through an S3 staging
  area + native ``FILES()`` ingestion.

Every string-or-secret field carries the value as a plain string
or a ``Secret``; the plugin reads the parsed value through the
``_host`` / ``_port`` / ``_database`` getters. Iceberg-style
schema evolution is supported on the dest.
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
    EntityWhitelistSpec,
    FullPathSpec,
    GCPJson,
    GCSBucketSpec,
    HostSpec,
    PortSpec,
    S3BucketSpec,
    StrOrSecretConstraints,
    UserPassword,
    _convert,
)

# noinspection PyProtectedMember
from tabsdatak.conn.starrocks import _validation
from tabsdatak.conn.starrocks.error import StarRocksErrorCode
from tabsdatak.spi import Conn

# --- Type aliases ----------------------------------------------------

CredentialsSpec: TypeAlias = UserPassword

# host / port are the shared string-or-secret specs from
# ``tabsdatak.conn.common.types`` (HostSpec / PortSpec). Read their parsed
# values through the ``_host`` / ``_port`` getters.

# Database: StarRocks identifier shape (alphanumeric + underscore).
_IDENTIFIER_PATTERN = r"^(?:[A-Za-z_][A-Za-z0-9_]*)$"
DatabaseSpec: TypeAlias = Annotated[
    StrOrSecretSpec, StrOrSecretConstraints(pattern=_IDENTIFIER_PATTERN)
]
StarRocksTableNameSpec: TypeAlias = Annotated[
    str,
    _validation.STARROCKS_TABLE_FQN_VALIDATOR,
]
StarRocksTableNamesSpec: TypeAlias = list[StarRocksTableNameSpec]
IfTableExistsSpec: TypeAlias = Literal["append", "replace"]
SchemaEvolutionSpec: TypeAlias = Literal["strict", "iceberg"]


# --- StarRocksDest.dest_cfg keys ------------------------------------
DestCfgKey: TypeAlias = Literal["starrocks.logging_level",]

DestCfgSpec: TypeAlias = Annotated[
    Mapping[DestCfgKey, Any], _common_validation.CONN_CFG_VALIDATOR
]


# --- Cloud locations -------------------------------------------------
#
# StarRocks carries a per-Conn ``staging`` field whose value is one of
# the concrete CloudLocation subclasses below. They mirror the legacy
# conn-common shapes but live here so starrocks owns its own location
# surface and routes init failures through StarRocksErrorCode.


class CloudLocation(_api.DescriptorBase, ABC):
    __kind__ = "startRocksCloudLocation"


@_api.dataclass(StarRocksErrorCode.STARROCKS_8, kw_only=True)
class S3Location(CloudLocation):
    """S3 staging area used by ``StarRocksDest``. The plugin
    uploads parquet files here, then issues a StarRocks
    ``BROKER LOAD`` / ``FILES()`` reference for ingest.

    :param bucket: S3 bucket name.
    :param region: AWS region (e.g. ``"us-east-1"``).
    :param credentials: AWS access-key / secret-key pair.
    :param base_path: Full path (key prefix) from the bucket root:
        starts with ``/``, no trailing ``/``. The leading ``/`` is
        trimmed when forming the object key. Defaults to ``/``.
    :param cfg: Optional cfg dict for the S3 client.
    """

    __kind__ = "startRocksS3Location"
    bucket: S3BucketSpec
    region: AwsRegionSpec
    credentials: AwsAccessSecretKey
    base_path: FullPathSpec = "/"
    cfg: CfgSpec | None = None

    def _bucket(self) -> str:
        return _convert(self.bucket, str)

    def _region(self) -> str:
        return _convert(self.region, str)

    def _base_path(self) -> str:
        return _convert(self.base_path, str)


@_api.dataclass(StarRocksErrorCode.STARROCKS_9, kw_only=True)
class AzBlobLocation(CloudLocation):
    """Azure Blob staging area used by ``StarRocksDest``.

    :param container: Azure Blob container name.
    :param credentials: Storage-account key credentials.
    :param base_path: Full path (path prefix) from the container root:
        starts with ``/``, no trailing ``/``. The leading ``/`` is
        trimmed when forming the blob name. Defaults to ``/``.
    :param cfg: Optional cfg dict for the Azure Blob client.
    """

    __kind__ = "startRocksAzBlobLocation"
    container: AzContainerSpec
    credentials: AzureAccountKey
    base_path: FullPathSpec = "/"
    cfg: CfgSpec | None = None

    def _container(self) -> str:
        return _convert(self.container, str)

    def _base_path(self) -> str:
        return _convert(self.base_path, str)


@_api.dataclass(StarRocksErrorCode.STARROCKS_10, kw_only=True)
class GCSLocation(CloudLocation):
    """GCS staging area used by ``StarRocksDest``.

    :param bucket: GCS bucket name.
    :param credentials: GCP service-account JSON credentials.
    :param base_path: Full path (object-key prefix) from the bucket
        root: starts with ``/``, no trailing ``/``. The leading ``/`` is
        trimmed when forming the object key. Defaults to ``/``.
    :param cfg: Optional cfg dict for the GCS client.
    """

    __kind__ = "startRocksGcsLocation"
    bucket: GCSBucketSpec
    credentials: GCPJson
    base_path: FullPathSpec = "/"
    cfg: CfgSpec | None = None

    def _bucket(self) -> str:
        return _convert(self.bucket, str)

    def _base_path(self) -> str:
        return _convert(self.base_path, str)


# --- Connection ------------------------------------------------------


@_api.dataclass(StarRocksErrorCode.STARROCKS_1, kw_only=True)
class StarRocksDestConn(Conn):
    """StarRocks connection.

    Every string-or-secret field carries the value as a plain string or a
    ``Secret``; the plugin reads the parsed value through the ``_host`` /
    ``_port`` / ``_database`` getters.

    :param host: StarRocks FE host.
    :param port: MySQL-protocol port; default ``"9030"``.
    :param credentials: :class:`UserPassword`.
    :param staging: :class:`CloudLocation` (S3 / AzBlob / GCS).
    :param database: Default database; default ``"default"``.
    :param entity_whitelist: Optional regex-based table filter.
    :param conn_cfg: Optional driver-side config dict.
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
    """StarRocks destination — one parquet slot per table.

    :param tables: FQN table names; one parquet slot per entry.
    :param if_table_exists: ``"append"`` (default) or
        ``"replace"``.
    :param schema_evolution: ``"strict"`` (default) or
        ``"iceberg"``.
    :param dest_cfg: Optional dest cfg dict.
    """

    tables: StarRocksTableNamesSpec
    if_table_exists: IfTableExistsSpec = "append"
    schema_evolution: SchemaEvolutionSpec = "strict"
    dest_cfg: DestCfgSpec | None = None
