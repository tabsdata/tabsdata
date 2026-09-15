#
# Copyright 2026 Tabsdata Inc.
#

"""Azure Blob connector -- public API.

Four classes, imported from `tabsdatak.conn.azureblob`:

* `AzureBlobSrcConn` / `AzureBlobDestConn` -- the connection:
  container, credentials (`AzureAccountKey`), `base_path`,
  `conn_cfg`. Registered on a collection, not passed to a decorator.
* `AzureBlobSrc` -- the `@publisher` source: a list of `paths`
  (glob-style), one output slot per path.
* `AzureBlobDest` -- the `@subscriber` destination: a list of
  `paths`, one output slot per path.

The connection (with credentials) is separate from the source /
destination and is resolved from the function's collection at run time;
a flow function only references `AzureBlobSrc` / `AzureBlobDest`.

Examples:
    .. code-block:: python

        @publisher(source=AzureBlobSrc(paths=["parquet/*.parquet"]),
                   output_tables=["cities"])
        def ingest(cities: TableFramesSpec) -> TableFrameSpec:
            return cities[0] if cities else None

        @subscriber(destination=AzureBlobDest(paths=["out.csv"]),
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
from tabsdatak.conn.azureblob.error import AzureBlobErrorCode

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
    AzContainerSpec,
    AzureAccountKey,
    CfgSpec,
    CloudRelativePathSpec,
    _convert,
)
from tabsdatak.spi import Conn

# --- Type aliases ----------------------------------------------------

# AzureBlob currently supports a single credential shape -- account
# name + account key. Adding a new credential type later means
# widening this alias to `Union[AzureAccountKey, NewCredentials]`
# without touching `AzureBlobConn`'s signature.
CredentialsSpec: TypeAlias = AzureAccountKey
"""The credential type for Azure Blob connections: `AzureAccountKey`
(storage account name + account key)."""


# --- AzureBlobSrc.src_cfg keys --------------------------------------
SrcCfgKey: TypeAlias = Literal[
    "tabsdata.file.chunk_size",
    "tabsdata.src_metadata.drop",
]

SrcCfgSpec: TypeAlias = Annotated[
    Mapping[SrcCfgKey, Any], _common_validation.CONN_CFG_VALIDATION
]


# --- AzureBlobDest.dest_cfg keys ------------------------------------
DestCfgKey: TypeAlias = Literal["tabsdata.file.chunk_size"]

DestCfgSpec: TypeAlias = Annotated[
    Mapping[DestCfgKey, Any], _common_validation.CONN_CFG_VALIDATION
]


# --- Connection ------------------------------------------------------


@_api.dataclass(AzureBlobErrorCode.AZURE_BLOB_1, kw_only=True)
class AzureBlobSrcConn(Conn):
    """Azure Blob connection used by `AzureBlobSrc`.

    :categories: source

    Registered on a collection; the runtime resolves it and hands it to
    the plugin at run time. Each field accepts a plain string or a
    `Secret`.

    Args:
        container: A valid Azure Blob container name: 3-63 characters,
            lowercase letters and digits with single (non-leading,
            non-trailing) hyphens.
        credentials: Azure `AzureAccountKey` credentials (account name
            + account key).
        base_path: Blob-name prefix from the container root; starts with
            `/`, has no trailing `/` and no empty segments (`//`)
            (the leading `/` is trimmed when forming the blob name).
            Defaults to `/`.
        conn_cfg: Optional connector-config dict (see `CfgSpec`).
    """

    container: AzContainerSpec
    credentials: CredentialsSpec
    base_path: CloudRelativePathSpec = "/"
    conn_cfg: CfgSpec | None = None

    def _container(self) -> str:
        return _convert(self.container, str)

    def _base_path(self) -> str:
        return _convert(self.base_path, str)


@_api.dataclass(AzureBlobErrorCode.AZURE_BLOB_1, kw_only=True)
class AzureBlobDestConn(Conn):
    """Azure Blob connection used by `AzureBlobDest`.

    :categories: destination

    Field-identical to `AzureBlobSrcConn` but a distinct class; source
    and destination connections are separate types and cannot be used in
    place of one another. See `AzureBlobSrcConn` for the field meanings.
    """

    container: AzContainerSpec
    credentials: CredentialsSpec
    base_path: CloudRelativePathSpec = "/"
    conn_cfg: CfgSpec | None = None

    def _container(self) -> str:
        return _convert(self.container, str)

    def _base_path(self) -> str:
        return _convert(self.base_path, str)


# --- Source ----------------------------------------------------------


@_api.dataclass(AzureBlobErrorCode.AZURE_BLOB_2, kw_only=True)
class AzureBlobSrc(Src):
    """Azure Blob source for `@publisher` -- one output slot per path.

    :categories: source

    Every table this source publishes automatically carries a
    `@td.file.path` metadata column: the `az://` object URI the row was read
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
        format: File format -- one of `AUTO`, `CSV`, `JSON`,
            `AVRO`, `PARQUET`, `LOG`, `EXCEL`. `AUTO` (the default)
            infers it from the file extension. `EXCEL` reads one
            worksheet of an `.xlsx` workbook, named by the path's `#<n>`
            fragment.
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
    """
    paths: SrcPathsSpec
    format: SrcFileFormat = SrcFileFormat.AUTO
    format_cfg: SrcFormatCfgSpec | None = None
    initial_last_modified: datetime | None = None
    src_cfg: SrcCfgSpec | None = None
    purge: bool = False

    def validate(self) -> None:
        """Cross-field validation of `format`, `paths` and `format_cfg`
        (source side).

        Called by the framework; raises `AzureBlobValidateException`
        on failure.
        """
        try:
            _file_validation.validate_format_cfg(self.format_cfg, side="src")
            _file_validation.validate_src_format(
                self.format, self.paths, self.format_cfg
            )
            _file_validation.validate_src_purge(
                self.purge, self.initial_last_modified
            )
        except Exception as e:
            raise AzureBlobErrorCode.AZURE_BLOB_3.exception(cause=e)


# --- Destination -----------------------------------------------------


@_api.dataclass(AzureBlobErrorCode.AZURE_BLOB_4, kw_only=True)
class AzureBlobDest(Dest):
    """Azure Blob destination for `@subscriber` -- one target file per
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
            `tabsdata.file.chunk_size`.

    Note:
        A timestamp carrying a timezone is written to `CSV` and `JSON` as
        ISO-8601 in UTC -- `2026-07-14T09:30:15Z` -- whatever zone the
        column declared: the instant is preserved and only the label is
        normalised. A timestamp without a timezone is written as-is, with
        no offset. `PARQUET` and `AVRO` keep the column's own zone.

    Examples:
        Write a table to a single Parquet file, one slot per path:

        .. code-block:: python

            @subscriber(
                destination=AzureBlobDest(
                    paths=["exports/rows.parquet"],
                    format=DestFileFormat.PARQUET,
                ),
                input_tables=["mycoll/rows"],
            )
            def publish(rows: TableFrameSpec) -> TableFrameSpec:
                return rows

        Write CSV and tune the writer through `format_cfg`, keyed by
        the `DestFileFormat`:

        .. code-block:: python

            @subscriber(
                destination=AzureBlobDest(
                    paths=["exports/rows.csv"],
                    format=DestFileFormat.CSV,
                    format_cfg={
                        DestFileFormat.CSV: {
                            "separator": ";",
                            "include_header": True,
                        },
                    },
                ),
                input_tables=["mycoll/rows"],
            )
            def publish(rows: TableFrameSpec) -> TableFrameSpec:
                return rows
    """

    paths: DestPathsSpec
    format: DestFileFormat = DestFileFormat.AUTO
    format_cfg: DestFormatCfgSpec | None = None
    dest_cfg: DestCfgSpec | None = None

    def validate(self) -> None:
        """Cross-field validation of `format_cfg` (dest side).

        Called by the framework; raises `AzureBlobValidateException`
        on failure.
        """
        try:
            _file_validation.validate_format_cfg(self.format_cfg, side="dest")
        except Exception as e:
            raise AzureBlobErrorCode.AZURE_BLOB_5.exception(cause=e)
