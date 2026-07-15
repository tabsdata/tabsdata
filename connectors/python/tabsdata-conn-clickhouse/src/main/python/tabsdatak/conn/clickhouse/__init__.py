"""ClickHouse connector — public API.

* :class:`ClickHouseDestConn` -- per-cluster ClickHouse connection
  (host, port, credentials, database, cloud staging area).
* :class:`ClickHouseDest` -- destination: list of table FQNs;
  one parquet slot per table. Writes go through a cloud-staged
  bulk loader (S3 / GCS / Azure) rather than row-by-row inserts.

``port`` and ``secure`` are string-or-secret fields (``PortSpec`` /
``BoolSpec``); call ``_convert(...)`` from ``tabsdatak.conn.common.types``
at the use site to resolve them to a concrete ``int`` / ``bool`` for the
plugin. Iceberg-style schema evolution is supported on the dest.
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
    EntityWhitelistSpec,
    FullPathSpec,
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


# host / port / secure are the shared string-or-secret specs from
# ``tabsdatak.conn.common.types`` (HostSpec / PortSpec / BoolSpec). Read
# their parsed values through the ``_host`` / ``_port`` / ``_secure`` getters.

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
# Restricts ``ClickHouseDest.dest_cfg`` to the keys the destination
# plugin actually consumes. Pydantic enforces the literal membership
# of keys at validation time, so unknown keys are rejected at
# construction; ``CONN_CFG_VALIDATOR`` reuses the conn_cfg basic-value
# checks (str/int/float/bool/None, plus list/dict compositions).
DestCfgKey: TypeAlias = Literal["clickhouse.logging_level",]

DestCfgSpec: TypeAlias = Annotated[
    Mapping[DestCfgKey, Any], _common_validation.CONN_CFG_VALIDATOR
]


# --- Cloud locations (clickhouse-local) ------------------------------
#
# ClickHouse ingests via cloud-staged bulk load and reads the staged
# files through its native ``s3()`` / ``azureBlobStorage()`` table
# functions. The location types live here (rather than reusing
# ``tabsdatak.conn.common.types``) so the GCS variant can carry
# ``GCPHmac`` credentials directly: ClickHouse reads GCS exclusively
# through the S3-compatible endpoint and has no path that uses the
# GCS-native ``GCPJson`` service-account credential. Keeping the
# types local also lets the dataclass init failures surface via
# ``ClickHouseErrorCode``.


class CloudLocation(_api.DescriptorBase, ABC):
    __kind__ = "clickHouseCloudLocation"


@_api.dataclass(ClickHouseErrorCode.CLICKHOUSE_8, kw_only=True)
class S3Location(CloudLocation):
    """S3 staging area used by ``ClickHouseDest`` to land parquet
    files before issuing the ``COPY`` / ``INSERT INTO ... FROM s3()``
    statement. ClickHouse reads parquet directly from S3, so the
    plugin uploads to this bucket and references the resulting
    object via ClickHouse's S3 table function.

    :param bucket: S3 bucket name (e.g. ``"my-staging-bucket"``).
    :param region: AWS region (e.g. ``"us-east-1"``).
    :param credentials: AWS access-key / secret-key pair.
    :param base_path: Full path (key prefix) from the bucket root
        (e.g. ``"/staging/clickhouse"``): starts with ``/``, no trailing
        ``/``. The leading ``/`` is trimmed when forming the object key.
        Defaults to ``/``.
    :param cfg: Optional cfg dict passed to the underlying
        S3 client.
    """

    __kind__ = "clickHouseS3Location"
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


@_api.dataclass(ClickHouseErrorCode.CLICKHOUSE_9, kw_only=True)
class AzBlobLocation(CloudLocation):
    """Azure Blob Storage staging area used by ``ClickHouseDest``.
    The plugin uploads parquet files to the container and references
    them via ClickHouse's ``azureBlobStorage()`` table function.

    :param container: Azure Blob container name.
    :param credentials: Storage-account key credentials.
    :param base_path: Full path (path prefix) from the container root
        (e.g. ``"/staging/clickhouse"``): starts with ``/``, no trailing
        ``/``. The leading ``/`` is trimmed when forming the blob name.
        Defaults to ``/``.
    :param cfg: Optional cfg dict passed to the underlying
        Azure Blob client.
    """

    __kind__ = "clickHouseAzBlobLocation"
    container: AzContainerSpec
    credentials: AzureAccountKey
    base_path: FullPathSpec = "/"
    cfg: CfgSpec | None = None

    def _container(self) -> str:
        return _convert(self.container, str)

    def _base_path(self) -> str:
        return _convert(self.base_path, str)


@_api.dataclass(ClickHouseErrorCode.CLICKHOUSE_10, kw_only=True)
class GCSLocation(CloudLocation):
    """GCS staging for ClickHouse.

    Carries two required credentials -- one per code path:

    - :attr:`credentials` (:class:`~tabsdatak.conn.common.types.GCPJson`):
      service-account JSON used by ``google-cloud-storage`` to upload
      Parquet files to the bucket. The native GCS API is markedly
      faster than the S3-compat layer for uploads / lists / deletes.
    - :attr:`hmac_credentials` (:class:`~tabsdatak.conn.common.types.GCPHmac`):
      HMAC access / secret key pair fed to ClickHouse's ``s3()``
      table function so the server can fetch the staged file back
      via the S3-compatible endpoint (the only protocol ClickHouse
      speaks to GCS).

    Both are required: drop either and you lose a code path.
    """

    __kind__ = "clickHouseGcsLocation"
    bucket: GCSBucketSpec
    credentials: GCPJson
    hmac_credentials: GCPHmac
    base_path: FullPathSpec = "/"
    cfg: CfgSpec | None = None

    def _bucket(self) -> str:
        return _convert(self.bucket, str)

    def _base_path(self) -> str:
        return _convert(self.base_path, str)


# --- Connection ------------------------------------------------------


@_api.dataclass(ClickHouseErrorCode.CLICKHOUSE_1, kw_only=True)
class ClickHouseDestConn(Conn):
    """ClickHouse connection.

    Every string-or-secret field carries the value as a plain string or a
    ``Secret``; the plugin reads the parsed value through the ``_host`` /
    ``_port`` / ``_database`` / ``_secure`` getters.

    :param host: ClickHouse host.
    :param port: HTTP port; default ``"8123"``.
    :param credentials: :class:`UserPassword`.
    :param staging: :class:`CloudLocation` (S3 / AzBlob / GCS).
    :param database: Default database; default ``"default"``.
    :param secure: ``true`` for HTTPS, ``false`` for HTTP; default ``"false"``.
    :param entity_whitelist: Optional regex-based table filter.
    :param conn_cfg: Optional driver-side config dict.
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
    """ClickHouse destination — one parquet slot per table.

    :param tables: FQN table names (``[<db>.]<table>``); one
        parquet slot per entry.
    :param if_table_exists: ``"append"`` (default) or
        ``"replace"``.
    :param schema_evolution: ``"strict"`` (default) or
        ``"iceberg"`` (apply Iceberg-style schema deltas).
    :param dest_cfg: Optional dest cfg dict.
    """

    tables: ClickHouseTableNamesSpec
    if_table_exists: IfTableExistsSpec = "append"
    schema_evolution: SchemaEvolutionSpec = "strict"
    dest_cfg: DestCfgSpec | None = None

    def validate(self) -> None:
        if not self.tables:
            raise ClickHouseErrorCode.CLICKHOUSE_3.exception()
