#
# Copyright 2026 Tabsdata Inc.
#

"""Apache Druid connector -- public API.

Imported from `tabsdatak.conn.druid`:

* `DruidDestConn` -- the connection: the Overlord URL, a
  `UserPassword` for basic auth, and a cloud `staging` area
  (`S3Location` / `AzBlobLocation`). Registered
  on a collection, not passed to a decorator.
* `DruidDest` -- the `@subscriber` destination: a list of
  datasource names, one output slot per datasource.

There is no Druid source today. Writes go in two stages: parquet is
uploaded to the cloud staging area, then a native batch
`index_parallel` task is submitted to the Overlord and polled to
completion. The connection (with credentials) is resolved from the
function's collection at run time; a flow function only references
`DruidDest`.

Examples:
    .. code-block:: python

        @subscriber(
            destination=DruidDest(tables=["events"], time_column="ts"),
            input_tables=["mycoll/events"],
        )
        def publish(events: TableFrameSpec) -> TableFrameSpec:
            return events
URL and credential fields accept a plain string or a `Secret`.
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
# Druid carries a per-Conn `staging` field whose value is one of the
# concrete CloudLocation subclasses below. The classes mirror the
# legacy conn-common shapes but live here so druid owns its own
# location surface (and routes init failures through DruidErrorCode).


class CloudLocation(_api.DescriptorBase, ABC):
    """Abstract base for a `DruidDest` cloud staging area.

    :categories: destination

    Concrete subclasses (`S3Location` / `AzBlobLocation`) are set as
    the connection's `staging` field; the
    plugin uploads parquet there before submitting the Druid ingestion
    task.
    """

    __kind__ = "druidCloudLocation"


@_api.dataclass(DruidErrorCode.DRUID_12, kw_only=True)
class S3Location(CloudLocation):
    """S3 staging area for `DruidDest`.

    :categories: destination

    The plugin uploads parquet here, then issues a Druid ingestion spec
    referencing the S3 input source.

    Args:
        bucket: A valid S3 bucket name.
        region: A valid AWS region, e.g. `us-east-1`.
        credentials: `AwsAccessSecretKey` (access key id / secret
            access key).
        base_path: Object-key prefix from the bucket root, e.g.
            `/staging/druid`; starts with `/`, has no trailing `/`
            and no empty segments (`//`) (the leading `/` is trimmed
            when forming the key). Defaults to `/`.
        cfg: Optional connector config for the underlying S3 client
            (see `CfgSpec`).
    """

    __kind__ = "druidS3Location"
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


@_api.dataclass(DruidErrorCode.DRUID_13, kw_only=True)
class AzBlobLocation(CloudLocation):
    """Azure Blob staging area for `DruidDest`.

    :categories: destination

    The plugin uploads parquet to the container, then issues a Druid
    ingestion spec referencing the Azure input source.

    Args:
        container: A valid Azure Blob container name.
        credentials: `AzureAccountKey` storage-account credentials.
        base_path: Path prefix from the container root; starts with
            `/`, has no trailing `/` and no empty segments (`//`)
            (the leading `/` is trimmed when forming the blob name).
            Defaults to `/`.
        cfg: Optional connector config for the Azure Blob client (see
            `CfgSpec`).
    """

    __kind__ = "druidAzBlobLocation"
    container: AzContainerSpec
    credentials: AzureAccountKey
    base_path: CloudRelativePathSpec = "/"
    cfg: CfgSpec | None = None

    def _container(self) -> str:
        return _convert(self.container, str)

    def _base_path(self) -> str:
        return _convert(self.base_path, str)


# GCS staging is not offered for Druid.
#
# Druid's native `google` input source accepts no per-task credentials: the
# cluster reads the staged object under its own ambient identity, so the
# credentials would have to be baked into the Druid deployment as
# GOOGLE_APPLICATION_CREDENTIALS rather than passed on the connection. A
# `GCSLocation` a user could fill in would therefore be a lie -- the upload
# would succeed and the ingestion would fail with a 401 -- so there is no
# public surface for it. Kept commented rather than deleted so it is obvious
# what is missing and why, should Druid ever gain per-task GCS credentials.
# @_api.dataclass(DruidErrorCode.DRUID_14, kw_only=True)
# class GCSLocation(CloudLocation):
#     """GCS staging area for `DruidDest`.
#
#     :categories: destination
#
#     The plugin uploads parquet to the bucket, then issues a Druid
#     ingestion spec referencing the GCS input source.
#
#     Args:
#         bucket: A valid GCS bucket name.
#         credentials: `GCPJson` service-account JSON credentials.
#         base_path: Object-key prefix from the bucket root; starts with
#             `/`, has no trailing `/` and no empty segments (`//`)
#             (the leading `/` is trimmed when forming the key).
#             Defaults to `/`.
#         cfg: Optional connector config for the GCS client (see
#             `CfgSpec`).
#
#     Note:
#         `credentials` authenticate the **upload** only. Druid's native
#         `google` input source accepts no per-task credentials, so the
#         cluster reads the staged object under its own ambient identity
#         and must have `GOOGLE_APPLICATION_CREDENTIALS` granting access
#         to `bucket` -- otherwise the ingestion task fails with a 401
#         from `storage.googleapis.com`. `S3Location` and
#         `AzBlobLocation` carry their credentials in the ingestion spec
#         and need no cluster-side setup.
#     """
#
#     __kind__ = "druidGcsLocation"
#     bucket: GCSBucketSpec
#     credentials: GCPJson
#     base_path: CloudRelativePathSpec = "/"
#     cfg: CfgSpec | None = None
#
#     def _bucket(self) -> str:
#         return _convert(self.bucket, str)
#
#     def _base_path(self) -> str:
#         return _convert(self.base_path, str)


# --- Connection ------------------------------------------------------


@_api.dataclass(DruidErrorCode.DRUID_1, kw_only=True)
class DruidDestConn(Conn):
    """Druid Overlord connection used by `DruidDest`.

    :categories: destination

    Registered on a collection; the runtime resolves it and hands it to
    the plugin at run time.

    Args:
        overlord_url: Druid Overlord `http(s)://` URL, e.g.
            `http://druid.example:8081`. Accepts a plain string or a
            `Secret`.
        staging: Cloud staging area for parquet uploads -- an
            `S3Location` or `AzBlobLocation`.
        credentials: `UserPassword` for Overlord HTTP basic auth.
        conn_cfg: Optional connector config dict (see `CfgSpec`).

    Examples:
        A connection that stages parquet in S3 before Druid ingests it:

        .. code-block:: python

            DruidDestConn(
                overlord_url="http://druid.example:8081",
                staging=S3Location(
                    bucket="my-bucket",
                    region="us-east-1",
                    credentials=AwsAccessSecretKey(
                        access_key_id=Secret("AKIA..."),
                        secret_access_key=Secret("..."),
                    ),
                    base_path="/staging/druid",
                ),
                credentials=UserPassword(
                    user=Secret("admin"),
                    password=Secret("..."),
                ),
            )
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
    """Druid destination for `@subscriber` -- one slot per
    datasource.

    :categories: destination

    Args:
        tables: Datasource names, one write slot per entry (each 1-128
            chars, not starting with `.` or `_`, and containing no
            `/`, backslash or NUL).
        if_table_exists: `"append"` (default) or `"replace"`
            (overwrite the datasource).
        time_column: Name of the row-level timestamp column Druid uses
            as its primary `__time` field at ingest. Druid requires a
            time column on every datasource; if the rows carry none,
            leave unset and the ingestion spec falls back to the
            current ingest time.
        segment_granularity: Druid segment grain -- one of `ALL`
            (default), `NONE`, `SECOND`, `MINUTE`,
            `FIFTEEN_MINUTE`, `THIRTY_MINUTE`, `HOUR`,
            `SIX_HOUR`, `DAY`, `WEEK`, `MONTH`, `QUARTER` or
            `YEAR`.
        max_concurrent_subtasks: Druid ingestion-task concurrency
            (default 1).
        max_rows_per_segment: Per-segment row cap (default 5,000,000).
        max_rows_in_memory: Per-task in-memory row cap (default
            1,000,000).
        http_timeout_sec: HTTP request timeout in seconds (default
            300).
        dest_cfg: Optional connector config over the keys
            `druid.logging_level`, `druid.poll_interval_seconds`,
            `druid.retry_attempts` and
            `druid.retry_backoff_seconds`.

    Examples:
        Overwrite the datasource on every run instead of appending:

        .. code-block:: python

            @subscriber(
                destination=DruidDest(tables=["events"], if_table_exists="replace"),
                input_tables=["mycoll/events"],
            )
            def publish(events: TableFrameSpec) -> TableFrameSpec:
                return events
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
