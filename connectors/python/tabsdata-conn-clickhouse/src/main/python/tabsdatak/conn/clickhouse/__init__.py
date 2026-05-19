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
    Env,
    NamedConn,
    Secret,
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
# ``Env.lit("ch.example")`` themselves.
HostSpec: TypeAlias = Union[
    Annotated[Secret, _api.SecretValueConstraints(pattern=r".+")],
    Annotated[Env, _api.EnvValueConstraints(pattern=r".+")],
]
PortSpec: TypeAlias = int


# Database: ClickHouse identifier shape (alphanumeric + underscore).
_IDENTIFIER_PATTERN = r"[A-Za-z_][A-Za-z0-9_]*"

DatabaseSpec: TypeAlias = Union[
    Annotated[Secret, _api.SecretValueConstraints(pattern=_IDENTIFIER_PATTERN)],
    Annotated[Env, _api.EnvValueConstraints(pattern=_IDENTIFIER_PATTERN)],
]
ClickHouseTableNameSpec: TypeAlias = Annotated[
    str, _validation.CLICKHOUSE_TABLE_FQN_VALIDATOR,
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
DestCfgKey: TypeAlias = Literal[
    "clickhouse.logging_level",
]

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
class ClickHouseConn(Conn):
    host: HostSpec
    staging: CloudLocation
    port: PortSpec = 8123
    database: DatabaseSpec = "default"
    credentials: CredentialsSpec
    secure: bool = False
    entity_whitelist: EntityWhitelistSpec | None = None
    conn_cfg: CfgSpec | None = None


ConnSpec: TypeAlias = Union[NamedConn, ClickHouseConn]


# --- Destination -----------------------------------------------------


@_api.dataclass(ClickHouseErrorCode.CLICKHOUSE_2, kw_only=True)
class ClickHouseDest(Dest):
    conn: ConnSpec
    tables: ClickHouseTableNamesSpec
    if_table_exists: IfTableExistsSpec = "append"
    schema_evolution: SchemaEvolutionSpec = "strict"
    dest_cfg: DestCfgSpec | None = None

    def validate(self) -> None:
        if not self.tables:
            raise ClickHouseErrorCode.CLICKHOUSE_3.exception()
