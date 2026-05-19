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
from tabsdatak.conn.common import _validation as _common_validation
from tabsdatak.conn.common.types import (
    AwsAccessSecretKey,
    AwsRegionSpec,
    AzContainerSpec,
    AzureAccountKey,
    BasePathSpec,
    CfgSpec,
    GCPJson,
    GCSBucketSpec,
    S3BucketSpec,
    UserPassword,
    full_path,
)

# noinspection PyProtectedMember
from tabsdatak.conn.druid import _validation
from tabsdatak.conn.druid.error import DruidErrorCode

# --- Type aliases ----------------------------------------------------

CredentialsSpec: TypeAlias = UserPassword

UrlSpec: TypeAlias = Union[
    Annotated[Secret, _api.SecretValueConstraints(pattern=_validation.URL_PATTERN)],
    Annotated[Env, _api.EnvValueConstraints(pattern=_validation.URL_PATTERN)],
]
DatasourceSpec: TypeAlias = Annotated[str, _validation.DATASOURCE_VALIDATOR]
DruidTableNameSpec: TypeAlias = DatasourceSpec
DruidTableNamesSpec: TypeAlias = list[DruidTableNameSpec]
IfTableExistsSpec: TypeAlias = Literal["append", "replace"]
# Druid's accepted segment-granularity tokens (subset, the most-used
# values; the engine accepts more but these cover the common cases).
SegmentGranularitySpec: TypeAlias = Literal[
    "ALL",
    "NONE",
    "SECOND",
    "MINUTE",
    "FIFTEEN_MINUTE",
    "THIRTY_MINUTE",
    "HOUR",
    "SIX_HOUR",
    "DAY",
    "WEEK",
    "MONTH",
    "QUARTER",
    "YEAR",
]


# --- DruidDest.dest_cfg keys ----------------------------------------
DestCfgKey: TypeAlias = Literal[
    "druid.logging_level",
    "druid.poll_interval_seconds",
    "druid.retry_attempts",
    "druid.retry_backoff_seconds",
]

DestCfgSpec: TypeAlias = Annotated[
    Mapping[DestCfgKey, Any], _common_validation.CONN_CFG_VALIDATOR
]


# --- Cloud locations -------------------------------------------------
#
# Druid carries a per-Conn ``staging`` field whose value is one of the
# concrete CloudLocation subclasses below. The classes mirror the
# legacy conn-common shapes but live here so druid owns its own
# location surface (and routes init failures through DruidErrorCode).


class CloudLocation(_api.DescriptorBase, ABC):
    __kind__ = "druidCloudLocation"


@_api.dataclass(DruidErrorCode.DRUID_12, kw_only=True)
class S3Location(CloudLocation):
    __kind__ = "druidS3Location"
    bucket: S3BucketSpec
    region: AwsRegionSpec
    credentials: AwsAccessSecretKey
    base_path: BasePathSpec = ""
    cfg: CfgSpec | None = None

    def full_path(self, path: str) -> str:
        return full_path(self.base_path, path)


@_api.dataclass(DruidErrorCode.DRUID_13, kw_only=True)
class AzBlobLocation(CloudLocation):
    __kind__ = "druidAzBlobLocation"
    container: AzContainerSpec
    credentials: AzureAccountKey
    base_path: BasePathSpec = ""
    cfg: CfgSpec | None = None

    def full_path(self, path: str) -> str:
        return full_path(self.base_path, path)


@_api.dataclass(DruidErrorCode.DRUID_14, kw_only=True)
class GCSLocation(CloudLocation):
    __kind__ = "druidGcsLocation"
    bucket: GCSBucketSpec
    credentials: GCPJson
    base_path: BasePathSpec = ""
    cfg: CfgSpec | None = None

    def full_path(self, path: str) -> str:
        return full_path(self.base_path, path)


# --- Connection ------------------------------------------------------


@_api.dataclass(DruidErrorCode.DRUID_1, kw_only=True)
class DruidConn(Conn):
    overlord_url: UrlSpec
    staging: CloudLocation
    credentials: CredentialsSpec
    conn_cfg: CfgSpec | None = None


ConnSpec: TypeAlias = Union[NamedConn, DruidConn]


# --- Destination -----------------------------------------------------


@_api.dataclass(DruidErrorCode.DRUID_2, kw_only=True)
class DruidDest(Dest):
    conn: ConnSpec
    tables: DruidTableNamesSpec
    if_table_exists: IfTableExistsSpec = "append"
    time_column: str | None = None
    segment_granularity: SegmentGranularitySpec = "ALL"
    max_concurrent_subtasks: int = 1
    max_rows_per_segment: int = 5_000_000
    max_rows_in_memory: int = 1_000_000
    http_timeout_sec: int = 300
    dest_cfg: DestCfgSpec | None = None
