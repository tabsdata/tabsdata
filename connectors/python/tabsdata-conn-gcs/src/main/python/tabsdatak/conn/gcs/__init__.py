#
# Copyright 2026 Tabsdata Inc.
#

"""GCS connector -- public API.

Four classes, imported from `tabsdatak.conn.gcs`:

* `GCSSrcConn` / `GCSDestConn` -- the connection: bucket,
  credentials (`GCPJson`), optional `project`, `base_path`,
  `conn_cfg`. Registered on a collection, not passed to a decorator.
* `GCSSrc` -- the `@publisher` source: a list of `paths`
  (glob-style), one output slot per path.
* `GCSDest` -- the `@subscriber` destination: a list of `paths`,
  one output slot per path.

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
Sources read their files straight from storage; destinations write them through
the shared `td_transporter` binary.
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
from tabsdatak.conn.common.file import _validation as _file_validation
from tabsdatak.conn.common.file.typing_ import (
    DestFileFormat,
    DestFormatCfgSpec,
    DestPathsSpec,
    SrcFileFormat,
    SrcFormatCfgSpec,
    SrcPathsSpec,
)
from tabsdatak.conn.common.types import (
    CfgSpec,
    CloudRelativePathSpec,
    GCPJson,
    GCSBucketSpec,
    _convert,
)
from tabsdatak.conn.gcs import _validation
from tabsdatak.conn.gcs.error import GCSErrorCode
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


ProjectSpec: TypeAlias = Annotated[StrOrSecretSpec, _validation.PROJECT_VALIDATION]
"""A GCP project id: any non-empty string. Accepts a plain string or a
`Secret`."""


# --- GCSSrc.src_cfg keys --------------------------------------------
SrcCfgKey: TypeAlias = Literal[
    "tabsdata.file.chunk_size",
    "tabsdata.src_metadata.drop",
]

SrcCfgSpec: TypeAlias = Annotated[
    Mapping[SrcCfgKey, Any], _common_validation.CONN_CFG_VALIDATION
]


# --- GCSDest.dest_cfg keys ------------------------------------------
DestCfgKey: TypeAlias = Literal["tabsdata.file.chunk_size"]

DestCfgSpec: TypeAlias = Annotated[
    Mapping[DestCfgKey, Any], _common_validation.CONN_CFG_VALIDATION
]


# --- Connection ------------------------------------------------------


@_api.dataclass(GCSErrorCode.GCS_1, kw_only=True)
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
            `/`, has no trailing `/` and no empty segments (`//`)
            (the leading `/` is trimmed when forming the key).
            Defaults to `/`.
        conn_cfg: Optional connector-config dict (see `CfgSpec`).
    """

    bucket: GCSBucketSpec
    credentials: CredentialsSpec
    project: ProjectSpec | None = None
    base_path: CloudRelativePathSpec = "/"
    conn_cfg: CfgSpec | None = None

    def _bucket(self) -> str:
        return _convert(self.bucket, str)

    def _project(self) -> str | None:
        return _convert(self.project, str) if self.project is not None else None

    def _base_path(self) -> str:
        return _convert(self.base_path, str)


@_api.dataclass(GCSErrorCode.GCS_1, kw_only=True)
class GCSDestConn(Conn):
    """GCS connection used by `GCSDest`.

    :categories: destination

    Field-identical to `GCSSrcConn` but a distinct class; source and
    destination connections are separate types and cannot be used in
    place of one another. See `GCSSrcConn` for the field meanings.
    """

    bucket: GCSBucketSpec
    credentials: CredentialsSpec
    project: ProjectSpec | None = None
    base_path: CloudRelativePathSpec = "/"
    conn_cfg: CfgSpec | None = None

    def _bucket(self) -> str:
        return _convert(self.bucket, str)

    def _project(self) -> str | None:
        return _convert(self.project, str) if self.project is not None else None

    def _base_path(self) -> str:
        return _convert(self.base_path, str)


# --- Source ----------------------------------------------------------


@_api.dataclass(GCSErrorCode.GCS_2, kw_only=True)
class GCSSrc(Src):
    """GCS source for `@publisher` -- one output slot per path.

    :categories: source

    Every table this source publishes automatically carries a
    `@td.file.path` metadata column: the `gs://` object URI the row was read
    from. A path that globs several files reports the specific file
    each row came from, and a worksheet read from a workbook keeps its
    `#<n>` fragment, so the value can be pasted straight back into
    `paths`. Setting the `src_cfg` key `tabsdata.src_metadata.drop` to
    `True` leaves the column off.

    Args:
        paths: Source paths relative to the connection's `base_path`,
            one output slot per entry. Each path is relative (no
            leading `/`), must not end with `/`, and must not contain
            empty segments (`//`). A path may use a `*` glob in its
            final segment only; backslashes, NUL and `${...}` tokens
            are not allowed. An `.xlsx` path may end with `#<n>` to name
            the worksheet -- `1`-based over every sheet the workbook
            declares, hidden ones included. Two paths may name different
            sheets of one workbook, giving a slot each.
        format: File format, by extension:

            * `AUTO` (the default): taken from each file's extension, the
              one before a compression or archive extension if it ends in
              one (`a.json.gz` and `a.json.zip` are `JSON`). Every file a
              source takes must resolve to the same format, and a file
              whose extension names none (`a.b.zip`) fails the run. Any
              other format reads every file as that format, whatever its
              name.
            * `AVRO`: `.avro`.
            * `CSV`: `.csv`.
            * `EXCEL`: `.xlsx`; one worksheet, named by the path's `#<n>`
              fragment.
            * `JSON`: `.json`, `.jsonl`, `.ndjson`; one JSON object per
              line.
            * `LOG`: `.log`; one row per line.
            * `PARQUET`: `.parquet`, `.pq`.
        format_cfg: Optional per-format reader options (source-side
            keys only), keyed by the `SrcFileFormat`. `EXCEL` takes
            `has_header` (default `True`); with `False` the first row is
            data and the columns are named `column_1`, `column_2`, ...
        initial_last_modified: Optional cutoff for incremental
            ingestion. The first run imports only files modified at or
            after this timestamp; the cutoff then advances each run so
            only newer files are re-imported.
        src_cfg: Optional connector config; supported keys are
            `tabsdata.file.chunk_size` and
            `tabsdata.src_metadata.drop`.
        purge: Delete each source file that is older than the incremental
            `initial_last_modified` cursor. Files
            older than the original `initial_last_modified` timestamp set at
            registration time are never deleted -- they
            were never read. Only valid when `initial_last_modified` is set; the default
            is `False`.
        include_compressed: Also take compressed files and archives. A
            path takes a file whose name matches it plus one compression or
            archive extension (`data/*.json` takes `a.json.gz`, not
            `a.json.csv`), and a file whose name ends in one only when this
            is `True`; a path that itself ends in one requires it. The
            extension alone names the codec or the archive, and the name
            before it the format:

            * `.br`: Brotli.
            * `.bz2`: bzip2.
            * `.gz`: gzip.
            * `.lz4`: LZ4 frame.
            * `.tar`: a tar archive holding exactly one file.
            * `.xz`: XZ.
            * `.zip`: a zip archive holding exactly one file.
            * `.zlib`: zlib.
            * `.zst`, `.zstd`: Zstandard.

            A file has at most one of these extensions: an archive inside a
            compressed file (`a.csv.tar.gz`) is not read. Defaults to
            `False`.
        incremental: Whether the data of each run adds to the data of the
            previous runs (`True`, the default) or replaces it (`False`).
            The function handles the data accordingly; this option tells
            Tabsdata what to hand the function when a run finds no new
            data. It applies to each path on its own: in the same run one
            path can take files and another none. A path's files arrive as
            a list, so a run that takes no file for a path hands the
            function, for it:

            * `True`: a list holding one empty `TableFrame` with the schema
              of the last run that had data for that path, or an empty list
              if no run has had data for it yet.
            * `False`: always an empty list; the previous data of that path
              is still valid.

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
    src_cfg: SrcCfgSpec | None = None
    purge: bool = False
    include_compressed: bool = False
    incremental: bool = True

    def validate(self) -> None:
        """Cross-field validation of `format`, `paths` and `format_cfg`
        (source side).

        Called by the framework; raises `GCSValidateException` on
        failure.
        """
        try:
            _file_validation.validate_format_cfg(self.format_cfg, side="src")
            _file_validation.validate_src_format(
                self.format, self.paths, self.format_cfg
            )
            _file_validation.validate_src_purge(self.purge, self.initial_last_modified)
            _file_validation.validate_src_compressed(
                self.include_compressed, self.paths
            )
        except Exception as e:
            raise GCSErrorCode.GCS_3.exception(cause=e, side="src")


# --- Destination -----------------------------------------------------


@_api.dataclass(GCSErrorCode.GCS_4, kw_only=True)
class GCSDest(Dest):
    """GCS destination for `@subscriber` -- one target file per path.

    :categories: destination

    Args:
        paths: Target paths relative to the connection's `base_path`,
            one write slot per entry. Each path is relative (no leading
            `/`), must not end with `/`, and must not contain empty
            segments (`//`). Globs are not allowed; the tokens
            `${EXEC_PLAN_ID}`, `${TRX_ID}` and `${TRIGGERED_ON}`
            are substituted at write time. Backslashes and NUL are not
            allowed.
        format: Output format -- one of `AUTO`, `CSV`, `JSON`,
            `AVRO`, `PARQUET`. `AUTO` (the default) infers it from
            the file extension.
        format_cfg: Optional per-format writer options (dest-side keys
            only), keyed by the `DestFileFormat`.
        dest_cfg: Optional connector config; supports the key
            `tabsdata.file.chunk_size`.

    Note:
        A timestamp carrying a timezone is written to `CSV` and `JSON` as
        ISO-8601 in UTC -- `2026-07-14T09:30:15.000000+0000` in `CSV`,
        `2026-07-14T09:30:15+00:00` in `JSON` -- whatever zone the column
        declared: the instant is preserved and only the label is
        normalised; the `CSV` `datetime_format` option overrides the
        layout. A timestamp without a timezone is written as-is, with no
        offset. `PARQUET` keeps the column's own zone; `AVRO` stores the
        instant, read back in UTC.
    """

    paths: DestPathsSpec
    format: DestFileFormat = DestFileFormat.AUTO
    format_cfg: DestFormatCfgSpec | None = None
    dest_cfg: DestCfgSpec | None = None

    def validate(self) -> None:
        """Cross-field validation of `format_cfg` (dest side).

        Called by the framework; raises `GCSValidateException` on
        failure.
        """
        try:
            _file_validation.validate_format_cfg(self.format_cfg, side="dest")
        except Exception as e:
            raise GCSErrorCode.GCS_5.exception(cause=e, side="dest")
