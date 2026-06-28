"""Apache Druid connector — public API.

* :class:`DruidDestConn` -- per-Druid-Overlord connection.
* :class:`DruidDest` -- destination: list of datasource names;
  one parquet slot per datasource. Writes go in two stages:
  parquet -> cloud staging (S3 / GCS / Azure), then a native
  batch ``index_parallel`` task to the Overlord with polling
  until completion.

No ``DruidSrc``. URL and credential fields accept ``Var`` /
``Secret`` references.
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
    Var,
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
    Annotated[str, StringConstraints(pattern=_validation.URL_PATTERN)],
    Annotated[Var, _api.VarValueConstraints(pattern=_validation.URL_PATTERN)],
    Annotated[Secret, _api.SecretValueConstraints(pattern=_validation.URL_PATTERN)],
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
    """S3 staging area used by ``DruidDest``. The plugin uploads
    parquet files here, then issues a Druid ingestion spec
    referencing the S3 input source.

    :param bucket: S3 bucket name.
    :param region: AWS region (e.g. ``"us-east-1"``).
    :param credentials: AWS access-key / secret-key pair.
    :param base_path: Optional key prefix under the bucket
        (e.g. ``"staging/druid/"``).
    :param cfg: Optional cfg dict passed to the underlying S3
        client.
    """

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
    """Azure Blob staging area used by ``DruidDest``. The plugin
    uploads parquet to the container, then issues a Druid ingestion
    spec referencing the Azure input source.

    :param container: Azure Blob container name.
    :param credentials: Storage-account key credentials.
    :param base_path: Optional path prefix under the container.
    :param cfg: Optional cfg dict for the Azure Blob client.
    """

    __kind__ = "druidAzBlobLocation"
    container: AzContainerSpec
    credentials: AzureAccountKey
    base_path: BasePathSpec = ""
    cfg: CfgSpec | None = None

    def full_path(self, path: str) -> str:
        return full_path(self.base_path, path)


@_api.dataclass(DruidErrorCode.DRUID_14, kw_only=True)
class GCSLocation(CloudLocation):
    """GCS staging area used by ``DruidDest``. The plugin uploads
    parquet to the bucket, then issues a Druid ingestion spec
    referencing the GCS input source.

    :param bucket: GCS bucket name.
    :param credentials: GCP service-account JSON credentials.
    :param base_path: Optional object-key prefix under the bucket.
    :param cfg: Optional cfg dict for the GCS client.
    """

    __kind__ = "druidGcsLocation"
    bucket: GCSBucketSpec
    credentials: GCPJson
    base_path: BasePathSpec = ""
    cfg: CfgSpec | None = None

    def full_path(self, path: str) -> str:
        return full_path(self.base_path, path)


# --- Connection ------------------------------------------------------


@_api.dataclass(DruidErrorCode.DRUID_1, kw_only=True)
class DruidDestConn(Conn):
    """Druid Overlord connection.

    :param overlord_url: Druid Overlord URL
        (e.g. ``http://druid.example:8081``). ``Var`` / ``Secret``
        accepted.
    :param staging: :class:`S3Location` / :class:`AzBlobLocation`
        / :class:`GCSLocation` -- the cloud staging area for
        parquet uploads.
    :param credentials: :class:`UserPassword` -- Overlord HTTP
        basic-auth credentials.
    :param conn_cfg: Optional driver-side config dict.
    """

    __kind__ = "druidDestConn"

    overlord_url: UrlSpec
    staging: CloudLocation
    credentials: CredentialsSpec
    conn_cfg: CfgSpec | None = None


# --- Destination -----------------------------------------------------


@_api.dataclass(DruidErrorCode.DRUID_2, kw_only=True)
class DruidDest(Dest):
    """Druid destination — one parquet slot per datasource.

    :param tables: Datasource names; one slot per entry.
    :param if_table_exists: ``"append"`` (default) or
        ``"replace"`` (overwrite the datasource).
    :param time_column: Name of the row-level timestamp column
        Druid uses as its primary ``__time`` field at ingest. Druid
        requires a time column on every datasource; if the source
        rows don't carry one, leave unset and the ingestion spec
        falls back to the current ingest time.
    :param segment_granularity: Druid segment grain
        (``SECOND``..``YEAR``, default ``ALL``).
    :param max_concurrent_subtasks: Druid ingestion-task
        concurrency knob.
    :param max_rows_per_segment: Per-segment row cap.
    :param max_rows_in_memory: Per-task memory cap.
    :param http_timeout_sec: HTTP request timeout (s).
    :param dest_cfg: Optional dest cfg dict; keys include
        ``druid.logging_level`` / ``druid.poll_interval_seconds`` /
        ``druid.retry_attempts`` / ``druid.retry_backoff_seconds``.
    """

    tables: DruidTableNamesSpec
    if_table_exists: IfTableExistsSpec = "append"
    time_column: str | None = None
    segment_granularity: SegmentGranularitySpec = "ALL"
    max_concurrent_subtasks: int = 1
    max_rows_per_segment: int = 5_000_000
    max_rows_in_memory: int = 1_000_000
    http_timeout_sec: int = 300
    dest_cfg: DestCfgSpec | None = None
