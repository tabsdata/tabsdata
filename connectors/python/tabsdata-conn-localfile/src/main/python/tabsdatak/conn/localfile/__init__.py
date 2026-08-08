#
# Copyright 2026 Tabsdata Inc.
#

"""LocalFile connector -- public API.

Four classes, imported from `tabsdatak.conn.localfile`:

* `LocalFileSrcConn` / `LocalFileDestConn` -- the connection:
  `base_path` (an absolute local-filesystem path) and `conn_cfg`.
  No credentials. Registered on a collection, not passed to a decorator.
* `LocalFileSrc` -- the `@publisher` source: a list of `paths`
  (glob-style), one output slot per path.
* `LocalFileDest` -- the `@subscriber` destination: a list of
  `paths`, one output slot per path.

The connection is separate from the source / destination and is
resolved from the function's collection at run time; a flow function
only references `LocalFileSrc` / `LocalFileDest`.

Examples:
    .. code-block:: python

        @publisher(source=LocalFileSrc(paths=["parquet/*.parquet"]),
                   output_tables=["cities"])
        def ingest(cities: TableFramesSpec) -> TableFrameSpec:
            return cities[0] if cities else None

        @subscriber(destination=LocalFileDest(paths=["out.csv"]),
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
from tabsdatak.conn.common.types import CfgSpec, LocalAbsolutePathSpec, _convert
from tabsdatak.conn.localfile.error import LocalFileErrorCode
from tabsdatak.spi import Conn

# --- LocalFileSrc.src_cfg keys --------------------------------------
SrcCfgKey: TypeAlias = Literal[
    "tabsdata.file.chunkSize",
    "tabsdata.src_metadata.drop",
]

SrcCfgSpec: TypeAlias = Annotated[
    Mapping[SrcCfgKey, Any], _common_validation.CONN_CFG_VALIDATION
]


# --- LocalFileDest.dest_cfg keys ------------------------------------
DestCfgKey: TypeAlias = Literal["tabsdata.file.chunkSize"]

DestCfgSpec: TypeAlias = Annotated[
    Mapping[DestCfgKey, Any], _common_validation.CONN_CFG_VALIDATION
]


# --- Connection -----------------------------------------------------


@_api.dataclass(LocalFileErrorCode.LOCALFILE_1, kw_only=True)
class LocalFileSrcConn(Conn):
    """LocalFile connection used by `LocalFileSrc`.

    :categories: source

    Registered on a collection; the runtime resolves it and hands it to
    the plugin at run time. Has no credentials.

    Args:
        base_path: Absolute local-filesystem path — POSIX (`/a/b`) or
            Windows drive-absolute (`C:/a/b` or `C:\\a\\b`). No trailing
            separator (other than a bare root) and no empty segments.
            Defaults to `/`. Accepts a plain string or a `Secret`.
        conn_cfg: Optional connector-config dict (see `CfgSpec`).
    """

    base_path: LocalAbsolutePathSpec = "/"
    conn_cfg: CfgSpec | None = None

    def _base_path(self) -> str:
        return _convert(self.base_path, str)


@_api.dataclass(LocalFileErrorCode.LOCALFILE_1, kw_only=True)
class LocalFileDestConn(Conn):
    """LocalFile connection used by `LocalFileDest`.

    :categories: destination

    Field-identical to `LocalFileSrcConn` but a distinct class; source
    and destination connections are separate types and cannot be used in
    place of one another. See `LocalFileSrcConn` for the field meanings.
    """

    base_path: LocalAbsolutePathSpec = "/"
    conn_cfg: CfgSpec | None = None

    def _base_path(self) -> str:
        return _convert(self.base_path, str)


# --- Source ---------------------------------------------------------


@_api.dataclass(LocalFileErrorCode.LOCALFILE_2, kw_only=True)
class LocalFileSrc(Src):
    """LocalFile source for `@publisher` -- one output slot per path.

    :categories: source

    Every table this source publishes automatically carries a
    `@td.file.path` metadata column: the local path the row was read
    from. A path that globs several files reports the specific file
    each row came from. Setting the `src_cfg` key
    `tabsdata.src_metadata.drop` to `True` leaves the column off.

    Args:
        paths: Source paths relative to the connection's `base_path`,
            one output slot per entry. Each path is relative (no
            leading `/`), must not end with `/`, and must not contain
            empty segments (`//`). A path may use a `*` glob in its
            final segment only; backslashes, NUL and `${...}` tokens
            are not allowed.
        format: File format -- one of `AUTO`, `CSV`, `JSON`,
            `AVRO`, `PARQUET`, `LOG`. `AUTO` (the default)
            infers it from the file extension.
        format_cfg: Optional per-format reader options (source-side
            keys only), keyed by the `SrcFileFormat`.
        initial_last_modified: Optional cutoff for incremental
            ingestion. The first run imports only files modified at or
            after this timestamp; the cutoff then advances each run so
            only newer files are re-imported.
        src_cfg: Optional connector config; supported keys are
            `tabsdata.file.chunkSize` and
            `tabsdata.src_metadata.drop`.

    Examples:
        Publish one file, selecting the reader `format` explicitly.
        Each path is an array slot, so its frames arrive as a list:

        .. code-block:: python

            @publisher(
                source=LocalFileSrc(
                    paths=["cities.csv"],
                    format=SrcFileFormat.CSV,
                ),
                output_tables=["cities"],
            )
            def ingest(rows: TableFramesSpec) -> TableFrameSpec:
                return rows[0] if rows else None

        Incremental ingestion: `initial_last_modified` imports only
        files at or after the cutoff, advancing it on each run:

        .. code-block:: python

            @publisher(
                source=LocalFileSrc(
                    paths=["daily/*.parquet"],
                    initial_last_modified=datetime(2024, 1, 1, tzinfo=timezone.utc),
                ),
                output_tables=["new_files"],
            )
            def ingest_new(rows: TableFramesSpec) -> TableFrameSpec:
                return rows[0] if rows else None
    """

    paths: SrcPathsSpec
    format: SrcFileFormat = SrcFileFormat.AUTO
    format_cfg: SrcFormatCfgSpec | None = None
    initial_last_modified: datetime | None = None
    src_cfg: SrcCfgSpec | None = None

    def validate(self) -> None:
        """Cross-field validation of `format_cfg` (source side).

        Called by the framework; raises `LocalFileValidateException`
        on failure.
        """
        try:
            _file_validation.validate_format_cfg(self.format_cfg, side="src")
        except Exception as e:
            raise LocalFileErrorCode.LOCALFILE_3.exception(cause=e)


# --- Destination ----------------------------------------------------


@_api.dataclass(LocalFileErrorCode.LOCALFILE_4, kw_only=True)
class LocalFileDest(Dest):
    """LocalFile destination for `@subscriber` -- one target file per
    path.

    :categories: destination

    Args:
        paths: Target paths relative to the connection's `base_path`,
            one write slot per entry. Each path is relative (no leading
            `/`), must not end with `/`, and must not contain empty
            segments (`//`). Globs are not allowed; the tokens
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

    Note:
        A timestamp carrying a timezone is written to `CSV` and `JSON` as
        ISO-8601 in UTC -- `2026-07-14T09:30:15Z` -- whatever zone the
        column declared: the instant is preserved and only the label is
        normalised. A timestamp without a timezone is written as-is, with
        no offset. `PARQUET` and `AVRO` keep the column's own zone.
    """

    paths: DestPathsSpec
    format: DestFileFormat = DestFileFormat.AUTO
    format_cfg: DestFormatCfgSpec | None = None
    dest_cfg: DestCfgSpec | None = None

    def validate(self) -> None:
        """Cross-field validation of `format_cfg` (dest side).

        Called by the framework; raises `LocalFileValidateException`
        on failure.
        """
        try:
            _file_validation.validate_format_cfg(self.format_cfg, side="dest")
        except Exception as e:
            raise LocalFileErrorCode.LOCALFILE_5.exception(cause=e)
