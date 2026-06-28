"""StarRocks connector — public API.

* :class:`StarRocksDestConn` -- per-cluster StarRocks connection
  (host, port, credentials, database, cloud staging area).
* :class:`StarRocksDest` -- destination: list of table FQNs;
  one parquet slot per table. Writes go through an S3 staging
  area + native ``FILES()`` ingestion.

``port`` accepts ``int|Secret``; call
``_convert(conn.port, int)`` from
``tabsdatak.conn.common.types`` at the use site to resolve to a
runtime ``int``. Iceberg-style schema evolution is supported on
the dest.
"""

from __future__ import annotations

from abc import ABC
from collections.abc import Mapping
from typing import Annotated, Any, Literal, TypeAlias, Union

from pydantic import StringConstraints

# noinspection PyProtectedMember
import tabsdatak._api as _api

# noinspection PyProtectedMember
from tabsdatak.api import (
    Conn,
    Dest,
    Secret,
)

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
    GCPJson,
    GCSBucketSpec,
    S3BucketSpec,
    UserPassword,
    full_path,
)

# noinspection PyProtectedMember
from tabsdatak.conn.starrocks import _validation
from tabsdatak.conn.starrocks.error import StarRocksErrorCode

# --- Type aliases ----------------------------------------------------

CredentialsSpec: TypeAlias = UserPassword

# Host: no regex shape; permissive ``.+`` enables the bare-string
# shortcut at the field site.
HostSpec: TypeAlias = Union[
    Annotated[str, StringConstraints(pattern=r".+")],
    Annotated[Secret, _api.SecretValueConstraints(pattern=r".+")],
]

# Port: plain int, or a Secret whose resolved value parses as an
# unsigned integer. Call ``_convert(conn.port, int)`` at the use site to
# do the final cast.
_PORT_PATTERN = r"[0-9]+"
PortSpec: TypeAlias = Union[
    int,
    Annotated[Secret, _api.SecretValueConstraints(pattern=_PORT_PATTERN)],
]


_IDENTIFIER_PATTERN = r"^(?:[A-Za-z_][A-Za-z0-9_]*)$"
DatabaseSpec: TypeAlias = Union[
    Annotated[str, StringConstraints(pattern=_IDENTIFIER_PATTERN)],
    Annotated[Secret, _api.SecretValueConstraints(pattern=_IDENTIFIER_PATTERN)],
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
    :param base_path: Optional key prefix under the bucket.
    :param cfg: Optional cfg dict for the S3 client.
    """

    __kind__ = "startRocksS3Location"
    bucket: S3BucketSpec
    region: AwsRegionSpec
    credentials: AwsAccessSecretKey
    base_path: BasePathSpec = ""
    cfg: CfgSpec | None = None

    def full_path(self, path: str) -> str:
        return full_path(self.base_path, path)


@_api.dataclass(StarRocksErrorCode.STARROCKS_9, kw_only=True)
class AzBlobLocation(CloudLocation):
    """Azure Blob staging area used by ``StarRocksDest``.

    :param container: Azure Blob container name.
    :param credentials: Storage-account key credentials.
    :param base_path: Optional path prefix under the container.
    :param cfg: Optional cfg dict for the Azure Blob client.
    """

    __kind__ = "startRocksAzBlobLocation"
    container: AzContainerSpec
    credentials: AzureAccountKey
    base_path: BasePathSpec = ""
    cfg: CfgSpec | None = None

    def full_path(self, path: str) -> str:
        return full_path(self.base_path, path)


@_api.dataclass(StarRocksErrorCode.STARROCKS_10, kw_only=True)
class GCSLocation(CloudLocation):
    """GCS staging area used by ``StarRocksDest``.

    :param bucket: GCS bucket name.
    :param credentials: GCP service-account JSON credentials.
    :param base_path: Optional object-key prefix under the bucket.
    :param cfg: Optional cfg dict for the GCS client.
    """

    __kind__ = "startRocksGcsLocation"
    bucket: GCSBucketSpec
    credentials: GCPJson
    base_path: BasePathSpec = ""
    cfg: CfgSpec | None = None

    def full_path(self, path: str) -> str:
        return full_path(self.base_path, path)


# --- Connection ------------------------------------------------------


@_api.dataclass(StarRocksErrorCode.STARROCKS_1, kw_only=True)
class StarRocksDestConn(Conn):
    """StarRocks connection.

    :param host: StarRocks FE host.
    :param port: MySQL-protocol port; default 9030. Accepts
        ``int`` or ``Secret`` -- resolve at the use site
        with ``_convert(conn.port, int)``.
    :param credentials: :class:`UserPassword`.
    :param staging: :class:`CloudLocation` (S3 / AzBlob / GCS).
    :param database: Default database; default ``"default"``.
    :param entity_whitelist: Optional regex-based table filter.
    :param conn_cfg: Optional driver-side config dict.
    """

    __kind__ = "starRocksDestConn"

    host: HostSpec
    port: PortSpec = 9030
    credentials: CredentialsSpec
    staging: CloudLocation
    database: DatabaseSpec = "default"
    entity_whitelist: EntityWhitelistSpec | None = None
    conn_cfg: CfgSpec | None = None


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
