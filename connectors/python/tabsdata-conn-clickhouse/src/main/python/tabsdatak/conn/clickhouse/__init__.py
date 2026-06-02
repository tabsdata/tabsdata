"""ClickHouse connector — public API.

* :class:`ClickHouseDestConn` -- per-cluster ClickHouse connection
  (host, port, credentials, database, cloud staging area).
* :class:`ClickHouseDest` -- destination: list of table FQNs;
  one parquet slot per table. Writes go through a cloud-staged
  bulk loader (S3 / GCS / Azure) rather than row-by-row inserts.

``port`` and ``secure`` accept ``int|bool|Var|Secret``; call
``_convert(...)`` from ``tabsdatak.conn.common.types`` at the
use site to resolve to a concrete ``int`` / ``bool`` for the
plugin. Iceberg-style schema evolution is supported on the dest.
"""

from __future__ import annotations

from abc import ABC
from collections.abc import Mapping
from typing import Annotated, Any, Literal, TypeAlias, Union

# noinspection PyProtectedMember
import tabsdatak._api as _api

# noinspection PyProtectedMember
from tabsdatak.api import (
    Conn,
    Dest,
    Secret,
    Var,
)

# noinspection PyProtectedMember
from tabsdatak.conn.clickhouse import _validation
from tabsdatak.conn.clickhouse.error import ClickHouseErrorCode

# noinspection PyProtectedMember
from tabsdatak.conn.common import _validation as _common_validation
from tabsdatak.conn.common.types import (
    AwsAccessSecretKey,
    AwsRegionSpec,
    AzContainerSpec,
    AzureAccountKey,
    BasePathSpec,
    CfgSpec,
    EntityWhitelistSpec,
    GCPHmac,
    GCPJson,
    GCSBucketSpec,
    S3BucketSpec,
    UserPassword,
    full_path,
)

# --- Type aliases ----------------------------------------------------

CredentialsSpec: TypeAlias = UserPassword


# Host: no regex shape; any non-empty string passes. The permissive
# ``.+`` pattern is what enables the bare-string shortcut at the field
# site (the ``EnvValuePattern`` / ``SecretValuePattern`` wrapper promotes
# a plain str to a literal); without it, callers would have to spell
# ``Var.lit("ch.example")`` themselves.
HostSpec: TypeAlias = Union[
    Annotated[Secret, _api.SecretValueConstraints(pattern=r".+")],
    Annotated[Var, _api.VarValueConstraints(pattern=r".+")],
]

# Port: plain int, or an Var / Secret whose resolved value parses as an
# unsigned integer. The Var / Secret arms carry a digit-only pattern so
# the value is validated at ``value()`` time. Call
# ``_convert(conn.port, int)`` at the use site to do the final cast.
_PORT_PATTERN = r"[0-9]+"
PortSpec: TypeAlias = Union[
    int,
    Annotated[Var, _api.VarValueConstraints(pattern=_PORT_PATTERN)],
    Annotated[Secret, _api.SecretValueConstraints(pattern=_PORT_PATTERN)],
]

# Secure: plain bool, or an Var / Secret whose resolved value is one of
# the accepted truthy / falsy tokens. Call ``_convert(conn.secure, bool)``
# at the use site to do the final cast.
_BOOL_PATTERN = r"(?i)(?:true|false|1|0|yes|no)"
SecureSpec: TypeAlias = Union[
    bool,
    Annotated[Var, _api.VarValueConstraints(pattern=_BOOL_PATTERN)],
    Annotated[Secret, _api.SecretValueConstraints(pattern=_BOOL_PATTERN)],
]


# Database: ClickHouse identifier shape (alphanumeric + underscore).
_IDENTIFIER_PATTERN = r"[A-Za-z_][A-Za-z0-9_]*"

DatabaseSpec: TypeAlias = Union[
    Annotated[Secret, _api.SecretValueConstraints(pattern=_IDENTIFIER_PATTERN)],
    Annotated[Var, _api.VarValueConstraints(pattern=_IDENTIFIER_PATTERN)],
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
    :param base_path: Optional key prefix under the bucket
        (e.g. ``"staging/clickhouse/"``); defaults to bucket root.
    :param cfg: Optional cfg dict passed to the underlying
        S3 client.
    """

    __kind__ = "clickHouseS3Location"
    bucket: S3BucketSpec
    region: AwsRegionSpec
    credentials: AwsAccessSecretKey
    base_path: BasePathSpec = ""
    cfg: CfgSpec | None = None

    def full_path(self, path: str) -> str:
        return full_path(self.base_path, path)


@_api.dataclass(ClickHouseErrorCode.CLICKHOUSE_9, kw_only=True)
class AzBlobLocation(CloudLocation):
    """Azure Blob Storage staging area used by ``ClickHouseDest``.
    The plugin uploads parquet files to the container and references
    them via ClickHouse's ``azureBlobStorage()`` table function.

    :param container: Azure Blob container name.
    :param credentials: Storage-account key credentials.
    :param base_path: Optional path prefix under the container
        (e.g. ``"staging/clickhouse/"``); defaults to container
        root.
    :param cfg: Optional cfg dict passed to the underlying
        Azure Blob client.
    """

    __kind__ = "clickHouseAzBlobLocation"
    container: AzContainerSpec
    credentials: AzureAccountKey
    base_path: BasePathSpec = ""
    cfg: CfgSpec | None = None

    def full_path(self, path: str) -> str:
        return full_path(self.base_path, path)


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
    base_path: BasePathSpec = ""
    cfg: CfgSpec | None = None

    def full_path(self, path: str) -> str:
        return full_path(self.base_path, path)


# --- Connection ------------------------------------------------------


@_api.dataclass(ClickHouseErrorCode.CLICKHOUSE_1, kw_only=True)
class ClickHouseDestConn(Conn):
    """ClickHouse connection.

    :param host: ClickHouse host.
    :param port: HTTP port; default 8123. Accepts ``int``,
        ``Var``, or ``Secret`` -- resolve at the use site with
        ``_convert(conn.port, int)``.
    :param credentials: :class:`UserPassword`.
    :param staging: :class:`CloudLocation` (S3 / AzBlob / GCS).
    :param database: Default database; default ``"default"``.
    :param secure: ``True`` for HTTPS, ``False`` for HTTP.
        Accepts ``bool``, ``Var``, or ``Secret`` -- resolve at the
        use site with ``_convert(conn.secure, bool)``.
    :param entity_whitelist: Optional regex-based table filter.
    :param conn_cfg: Optional driver-side config dict.
    """

    __kind__ = "clickHouseDestConn"

    host: HostSpec
    port: PortSpec = 8123
    credentials: CredentialsSpec
    staging: CloudLocation
    database: DatabaseSpec = "default"
    secure: SecureSpec = False
    entity_whitelist: EntityWhitelistSpec | None = None
    conn_cfg: CfgSpec | None = None


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
