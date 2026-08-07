#
# Copyright 2026 Tabsdata Inc.
#

"""S3 connector -- public API.

Four classes, imported from `tabsdatak.conn.s3`:

* `S3SrcConn` / `S3DestConn` -- the connection: bucket, region,
  credentials (`AwsAccessSecretKey`), `base_path`, `conn_cfg`.
  Registered on a collection, not passed to a decorator.
* `S3Src` -- the `@publisher` source: a list of `paths`
  (glob-style), one output slot per path.
* `S3Dest` -- the `@subscriber` destination: a list of `paths`,
  one output slot per path.

The connection (with credentials) is separate from the source /
destination and is resolved from the function's collection at run time;
a flow function only references `S3Src` / `S3Dest`.

Examples:
    .. code-block:: python

        @publisher(source=S3Src(paths=["parquet/*.parquet"]),
                   output_tables=["cities"])
        def ingest(cities: TableFramesSpec) -> TableFrameSpec:
            return cities[0] if cities else None

        @subscriber(destination=S3Dest(paths=["out.csv"]),
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
from tabsdatak.conn.common.types import (
    AwsAccessSecretKey,
    AwsRegionSpec,
    CfgSpec,
    CloudRelativePathSpec,
    S3BucketSpec,
    _convert,
)
from tabsdatak.conn.s3.error import S3ErrorCode
from tabsdatak.spi import Conn

# --- Type aliases ----------------------------------------------------

CredentialsSpec: TypeAlias = AwsAccessSecretKey
"""The credential type for S3 connections: `AwsAccessSecretKey`
(access key ID + secret access key)."""


# --- S3Src.src_cfg keys ---------------------------------------------
SrcCfgKey: TypeAlias = Literal["tabsdata.file.chunkSize"]

SrcCfgSpec: TypeAlias = Annotated[
    Mapping[SrcCfgKey, Any], _common_validation.CONN_CFG_VALIDATION
]


# --- S3Dest.dest_cfg keys -------------------------------------------
DestCfgKey: TypeAlias = Literal["tabsdata.file.chunkSize"]

DestCfgSpec: TypeAlias = Annotated[
    Mapping[DestCfgKey, Any], _common_validation.CONN_CFG_VALIDATION
]


# --- Connection ------------------------------------------------------


@_api.dataclass(S3ErrorCode.S3_1, kw_only=True)
class S3SrcConn(Conn):
    """S3 connection used by `S3Src`.

    :categories: source

    Registered on a collection; the runtime resolves it and hands it to
    the plugin at run time. Each field accepts a plain string or a
    `Secret`.

    Args:
        bucket: A valid S3 bucket name.
        region: A valid AWS region, e.g. `us-east-1`.
        credentials: AWS `AwsAccessSecretKey` credentials.
        base_path: Object-key prefix from the bucket root; starts with
            `/`, has no trailing `/` and no empty segments (`//`)
            (the leading `/` is trimmed when forming the key).
            Defaults to `/`.
        conn_cfg: Optional connector-config dict (see `CfgSpec`).
    """

    bucket: S3BucketSpec
    region: AwsRegionSpec
    credentials: CredentialsSpec
    base_path: CloudRelativePathSpec = "/"
    conn_cfg: CfgSpec | None = None

    def _bucket(self) -> str:
        return _convert(self.bucket, str)

    def _region(self) -> str:
        return _convert(self.region, str)

    def _base_path(self) -> str:
        return _convert(self.base_path, str)


@_api.dataclass(S3ErrorCode.S3_1, kw_only=True)
class S3DestConn(Conn):
    """S3 connection used by `S3Dest`.

    :categories: destination

    Field-identical to `S3SrcConn` but a distinct class; source and
    destination connections are separate types and cannot be used in
    place of one another. See `S3SrcConn` for the field meanings.
    """

    bucket: S3BucketSpec
    region: AwsRegionSpec
    credentials: CredentialsSpec
    base_path: CloudRelativePathSpec = "/"
    conn_cfg: CfgSpec | None = None

    def _bucket(self) -> str:
        return _convert(self.bucket, str)

    def _region(self) -> str:
        return _convert(self.region, str)

    def _base_path(self) -> str:
        return _convert(self.base_path, str)


# --- Source ----------------------------------------------------------


@_api.dataclass(S3ErrorCode.S3_2, kw_only=True)
class S3Src(Src):
    """S3 source for `@publisher` -- one output slot per path.

    :categories: source

    Every table this source publishes automatically carries a
    `@td.file.path` metadata column: the `s3://` object URI the row was read
    from. A path that globs several files reports the specific file
    each row came from.

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
        src_cfg: Optional connector config; supports the key
            `tabsdata.file.chunkSize`.
    """

    paths: SrcPathsSpec
    format: SrcFileFormat = SrcFileFormat.AUTO
    format_cfg: SrcFormatCfgSpec | None = None
    initial_last_modified: datetime | None = None
    src_cfg: SrcCfgSpec | None = None

    def validate(self) -> None:
        """Cross-field validation of `format_cfg` (source side).

        Called by the framework; raises `S3ValidateException` on
        failure.
        """
        try:
            _file_validation.validate_format_cfg(self.format_cfg, side="src")
        except Exception as e:
            raise S3ErrorCode.S3_3.exception(cause=e, side="src")


# --- Destination -----------------------------------------------------


@_api.dataclass(S3ErrorCode.S3_4, kw_only=True)
class S3Dest(Dest):
    """S3 destination for `@subscriber` -- one target file per path.

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

    Examples:
        Subscribe rows to S3, choosing the output file format:

        .. code-block:: python

            @subscriber(
                destination=S3Dest(
                    paths=["caps_out.json"],
                    format=DestFileFormat.JSON,
                ),
                input_tables=["caps_input/cap_rows"],
            )
            def write_json(rows: TableFrameSpec) -> TableFrameSpec:
                return rows

        Write CSV, tuning the writer through `format_cfg`:

        .. code-block:: python

            @subscriber(
                destination=S3Dest(
                    paths=["caps_out.csv"],
                    format=DestFileFormat.CSV,
                    format_cfg={
                        DestFileFormat.CSV: {
                            "separator": ";",
                            "include_header": True,
                        }
                    },
                ),
                input_tables=["caps_input/cap_rows"],
            )
            def write_csv(rows: TableFrameSpec) -> TableFrameSpec:
                return rows
    """

    paths: DestPathsSpec
    format: DestFileFormat = DestFileFormat.AUTO
    format_cfg: DestFormatCfgSpec | None = None
    dest_cfg: DestCfgSpec | None = None

    def validate(self) -> None:
        """Cross-field validation of `format_cfg` (dest side).

        Called by the framework; raises `S3ValidateException` on
        failure.
        """
        try:
            _file_validation.validate_format_cfg(self.format_cfg, side="dest")
        except Exception as e:
            raise S3ErrorCode.S3_5.exception(cause=e, side="dest")
