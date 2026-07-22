#
# Copyright 2026 Tabsdata Inc.
#

"""GCS connector -- public API.

Four classes, imported from `tabsdatak.conn.gcs`:

* `GCSSrcConn` / `GCSDestConn` -- the connection: bucket,
  credentials (`GCPJson`), optional `project`, `base_path`,
  `conn_cfg`. Registered on a collection, not passed to a decorator.
* `GCSSrc` -- the `@publisher` source: a list of `paths`
  (glob-style), one parquet slot per path.
* `GCSDest` -- the `@subscriber` destination: a list of `paths`,
  one parquet slot per path.

The connection (with credentials) is separate from the source /
destination and is resolved from the function's collection at run time;
a flow function only references `GCSSrc` / `GCSDest`.

Examples:
    .. code-block:: python

        @publisher(source=GCSSrc(paths=["parquet/*.parquet"]),
                   output_tables=["cities"])
        def ingest(cities: TableFramesSpec) -> TableFrameSpec:
            return cities[0] if cities else None

        @subscriber(destination=GCSDest(paths=["out.csv"]),
                    input_tables=["mycoll/cities"])
        def publish(cities: TableFrameSpec) -> TableFrameSpec:
            return cities
File transport goes through the shared `td_transporter` binary.
"""

from __future__ import annotations

from collections.abc import Mapping
from datetime import datetime
from typing import Annotated, Any, Literal, TypeAlias

# noinspection PyProtectedMember
import tabsdatak._api as _api
from tabsdatak.api import (
    Dest,
    Src,
    StrOrSecretSpec,
)

# noinspection PyProtectedMember
from tabsdatak.conn.common import _validation as _common_validation

# noinspection PyProtectedMember
from tabsdatak.conn.common.file import _validation
from tabsdatak.conn.common.file.typing_ import (
    DestFileFormat,
    DestFormatCfgSpec,
    DestPathsSpec,
    FileInfo,
    SrcFileFormat,
    SrcFormatCfgSpec,
    SrcPathsSpec,
)
from tabsdatak.conn.common.types import (
    CfgSpec,
    FullPathSpec,
    GCPJson,
    GCSBucketSpec,
    StrOrSecretConstraints,
    _convert,
)
from tabsdatak.conn.gcs.error import GcsErrorCode
from tabsdatak.spi import Conn

# --- Type aliases ----------------------------------------------------

# GCS currently supports a single credential shape -- a GCP
# service-account JSON. Adding a new credential type later (e.g.
# Workload Identity) means widening this alias to
# `Union[GCPJson, NewCredentials]` without touching `GCSConn`'s
# signature.
CredentialsSpec: TypeAlias = GCPJson
"""The credential type for GCS connections: `GCPJson` (a GCP
service-account JSON key)."""


# GCP project id: no regex shape; permissive `.+` enables the
# bare-string shortcut at the field site.
ProjectSpec: TypeAlias = Annotated[
    StrOrSecretSpec, StrOrSecretConstraints(pattern=r".+")
]
"""A GCP project id: any non-empty string. Accepts a plain string or a
`Secret`."""


# --- GCSSrc.src_cfg keys --------------------------------------------
SrcCfgKey: TypeAlias = Literal["tabsdata.file.chunkSize"]

SrcCfgSpec: TypeAlias = Annotated[
    Mapping[SrcCfgKey, Any], _common_validation.CONN_CFG_VALIDATOR
]


# --- GCSDest.dest_cfg keys ------------------------------------------
DestCfgKey: TypeAlias = Literal["tabsdata.file.chunkSize"]

DestCfgSpec: TypeAlias = Annotated[
    Mapping[DestCfgKey, Any], _common_validation.CONN_CFG_VALIDATOR
]


# --- Connection ------------------------------------------------------


@_api.dataclass(GcsErrorCode.GCS_1, kw_only=True)
class GCSSrcConn(Conn):
    """GCS connection used by `GCSSrc`.

    :categories: source

    Registered on a collection; the runtime resolves it and hands it to
    the plugin at run time. Each field accepts a plain string or a
    `Secret`.

    Args:
        bucket: A valid GCS bucket name: 3-63 characters, lowercase
            letters, digits, hyphens and underscores, starting and
            ending with a letter or digit.
        credentials: GCP `GCPJson` service-account JSON credentials.
        project: Optional GCP project id; any non-empty string.
        base_path: Object-key prefix from the bucket root; starts with
            `/` and has no trailing `/` (the leading `/` is
            trimmed when forming the key). Defaults to `/`.
        conn_cfg: Optional connector-config dict (see `CfgSpec`).
    """

    bucket: GCSBucketSpec
    credentials: CredentialsSpec
    project: ProjectSpec | None = None
    base_path: FullPathSpec = "/"
    conn_cfg: CfgSpec | None = None

    def _bucket(self) -> str:
        return _convert(self.bucket, str)

    def _project(self) -> str | None:
        return _convert(self.project, str) if self.project is not None else None

    def _base_path(self) -> str:
        return _convert(self.base_path, str)


@_api.dataclass(GcsErrorCode.GCS_1, kw_only=True)
class GCSDestConn(Conn):
    """GCS connection used by `GCSDest`.

    :categories: destination

    Field-identical to `GCSSrcConn` but a distinct class so source and
    destination connections cannot be confused at the wire-format level.
    See `GCSSrcConn` for the field meanings.
    """

    bucket: GCSBucketSpec
    credentials: CredentialsSpec
    project: ProjectSpec | None = None
    base_path: FullPathSpec = "/"
    conn_cfg: CfgSpec | None = None

    def _bucket(self) -> str:
        return _convert(self.bucket, str)

    def _project(self) -> str | None:
        return _convert(self.project, str) if self.project is not None else None

    def _base_path(self) -> str:
        return _convert(self.base_path, str)


# --- Source ----------------------------------------------------------


@_api.dataclass(GcsErrorCode.GCS_2, kw_only=True)
class GCSSrc(Src):
    """GCS source for `@publisher` -- one parquet slot per path.

    :categories: source

    Args:
        paths: Source paths relative to the connection's `base_path`,
            one output slot per entry. A path may use a `*` glob in
            its final segment only; backslashes, NUL and `${...}`
            tokens are not allowed.
        format: File format -- one of `AUTO`, `CSV`, `JSON`,
            `AVRO`, `PARQUET`, `LOG`. `AUTO` (the default)
            infers it from the file extension.
        format_cfg: Optional per-format reader options (source-side
            keys only), keyed by the `SrcFileFormat`.
        initial_last_modified: Optional cutoff for incremental
            ingestion. The first run imports only files modified at or
            after this timestamp; the cutoff then advances each run so
            only newer files are re-imported.
        file_info: Optional per-row provenance column -- `FILENAME`
            adds the file's basename, `FULLPATH` its full path;
            `None` (the default) adds nothing.
        src_cfg: Optional connector config; supports the key
            `tabsdata.file.chunkSize`.

    Examples:
        Read a single object, selecting the CSV format explicitly:

        .. code-block:: python

            @publisher(
                source=GCSSrc(paths=["export.csv"], format=SrcFileFormat.CSV),
                output_tables=["rows"],
            )
            def ingest(rows: TableFramesSpec) -> TableFrameSpec:
                return rows[0] if rows else None

        Incremental read: only objects modified at or after the
        cutoff are imported, and the cutoff advances each run:

        .. code-block:: python

            @publisher(
                source=GCSSrc(
                    paths=["parquet/orders.parquet"],
                    initial_last_modified=datetime(2024, 1, 1, tzinfo=timezone.utc),
                ),
                output_tables=["orders"],
            )
            def ingest_new(orders: TableFramesSpec) -> TableFrameSpec:
                return orders[0] if orders else None
    """

    paths: SrcPathsSpec
    format: SrcFileFormat = SrcFileFormat.AUTO
    format_cfg: SrcFormatCfgSpec | None = None
    initial_last_modified: datetime | None = None
    file_info: FileInfo | None = None
    src_cfg: SrcCfgSpec | None = None

    def validate(self) -> None:
        """Cross-field validation of `format_cfg` (source side).

        Called by the framework; raises `GcsValidateException` on
        failure.
        """
        try:
            _validation.validate_format_cfg(self.format_cfg, side="src")
        except Exception as e:
            raise GcsErrorCode.GCS_3.exception(cause=e, side="src")


# --- Destination -----------------------------------------------------


@_api.dataclass(GcsErrorCode.GCS_4, kw_only=True)
class GCSDest(Dest):
    """GCS destination for `@subscriber` -- one target file per path.

    :categories: destination

    Args:
        paths: Target paths relative to the connection's `base_path`,
            one write slot per entry. Globs are not allowed; the tokens
            `${EXEC_PLAN_ID}`, `${TRX_ID}` and `${EXEC_PLAN_TS}`
            are substituted at write time. Backslashes and NUL are not
            allowed.
        format: Output format -- one of `AUTO`, `CSV`, `JSON`,
            `AVRO`, `PARQUET`. `AUTO` (the default) infers it from
            the file extension.
        format_cfg: Optional per-format writer options (dest-side keys
            only), keyed by the `DestFileFormat`.
        dest_cfg: Optional connector config; supports the key
            `tabsdata.file.chunkSize`.
    """

    paths: DestPathsSpec
    format: DestFileFormat = DestFileFormat.AUTO
    format_cfg: DestFormatCfgSpec | None = None
    dest_cfg: DestCfgSpec | None = None

    def validate(self) -> None:
        """Cross-field validation of `format_cfg` (dest side).

        Called by the framework; raises `GcsValidateException` on
        failure.
        """
        try:
            _validation.validate_format_cfg(self.format_cfg, side="dest")
        except Exception as e:
            raise GcsErrorCode.GCS_5.exception(cause=e, side="dest")
