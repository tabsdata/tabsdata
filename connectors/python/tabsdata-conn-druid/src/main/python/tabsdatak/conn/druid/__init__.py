"""Apache Druid connector — public API.

* :class:`DruidDestConn` -- per-Druid-Overlord connection.
* :class:`DruidDest` -- destination: list of datasource names;
  one parquet slot per datasource. Writes go in two stages:
  parquet -> cloud staging (S3 / GCS / Azure), then a native
  batch ``index_parallel`` task to the Overlord with polling
  until completion.

No ``DruidSrc``. URL and credential fields accept ``Secret``
references.
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
    FullPathSpec,
    GCPJson,
    GCSBucketSpec,
    S3BucketSpec,
    StrOrSecretConstraints,
    UserPassword,
    _convert,
)

# noinspection PyProtectedMember
from tabsdatak.conn.druid import _validation
from tabsdatak.conn.druid.error import DruidErrorCode
from tabsdatak.spi import Conn

# --- Type aliases ----------------------------------------------------

CredentialsSpec: TypeAlias = UserPassword

UrlSpec: TypeAlias = Annotated[
    StrOrSecretSpec, StrOrSecretConstraints(pattern=_validation.URL_PATTERN)
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
    :param base_path: Full path (key prefix) from the bucket root
        (e.g. ``"/staging/druid"``): starts with ``/``, no trailing
        ``/``. The leading ``/`` is trimmed when forming the object key.
        Defaults to ``/``.
    :param cfg: Optional cfg dict passed to the underlying S3
        client.
    """

    __kind__ = "druidS3Location"
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


@_api.dataclass(DruidErrorCode.DRUID_13, kw_only=True)
class AzBlobLocation(CloudLocation):
    """Azure Blob staging area used by ``DruidDest``. The plugin
    uploads parquet to the container, then issues a Druid ingestion
    spec referencing the Azure input source.

    :param container: Azure Blob container name.
    :param credentials: Storage-account key credentials.
    :param base_path: Full path (path prefix) from the container root:
        starts with ``/``, no trailing ``/``. The leading ``/`` is
        trimmed when forming the blob name. Defaults to ``/``.
    :param cfg: Optional cfg dict for the Azure Blob client.
    """

    __kind__ = "druidAzBlobLocation"
    container: AzContainerSpec
    credentials: AzureAccountKey
    base_path: FullPathSpec = "/"
    cfg: CfgSpec | None = None

    def _container(self) -> str:
        return _convert(self.container, str)

    def _base_path(self) -> str:
        return _convert(self.base_path, str)


@_api.dataclass(DruidErrorCode.DRUID_14, kw_only=True)
class GCSLocation(CloudLocation):
    """GCS staging area used by ``DruidDest``. The plugin uploads
    parquet to the bucket, then issues a Druid ingestion spec
    referencing the GCS input source.

    :param bucket: GCS bucket name.
    :param credentials: GCP service-account JSON credentials.
    :param base_path: Full path (object-key prefix) from the bucket
        root: starts with ``/``, no trailing ``/``. The leading ``/`` is
        trimmed when forming the object key. Defaults to ``/``.
    :param cfg: Optional cfg dict for the GCS client.
    """

    __kind__ = "druidGcsLocation"
    bucket: GCSBucketSpec
    credentials: GCPJson
    base_path: FullPathSpec = "/"
    cfg: CfgSpec | None = None

    def _bucket(self) -> str:
        return _convert(self.bucket, str)

    def _base_path(self) -> str:
        return _convert(self.base_path, str)


# --- Connection ------------------------------------------------------


@_api.dataclass(DruidErrorCode.DRUID_1, kw_only=True)
class DruidDestConn(Conn):
    """Druid Overlord connection.

    ``overlord_url`` carries the value as a plain string or a ``Secret``;
    the plugin reads the parsed value through the ``_overlord_url`` getter.

    :param overlord_url: Druid Overlord URL
        (e.g. ``http://druid.example:8081``).
    :param staging: :class:`S3Location` / :class:`AzBlobLocation`
        / :class:`GCSLocation` -- the cloud staging area for
        parquet uploads.
    :param credentials: :class:`UserPassword` -- Overlord HTTP
        basic-auth credentials.
    :param conn_cfg: Optional driver-side config dict.
    """

    overlord_url: UrlSpec
    staging: CloudLocation
    credentials: CredentialsSpec
    conn_cfg: CfgSpec | None = None

    def _overlord_url(self) -> str:
        return _convert(self.overlord_url, str)


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
