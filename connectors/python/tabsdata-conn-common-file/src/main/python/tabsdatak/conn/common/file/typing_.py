#
# Copyright 2026 Tabsdata Inc.
#

"""Shared type shapes for the cloud-file (object-store) connectors --
`azureblob`, `gcs`, `localfile` and `s3` -- so each package
builds its `Src` / `Dest` on the same path and format specs.

* `SrcFileFormat` / `DestFileFormat` -- the formats the file
  connectors read and write: `AUTO`, `CSV`, `JSON`, `AVRO`,
  `PARQUET` (plus `LOG` and `EXCEL` on the source side, both
  read-only). `AUTO` lets the transporter infer the format from the
  file extension.
* `SrcPathsSpec` / `DestPathsSpec` -- the validated `paths` list
  a `Src` / `Dest` takes (one slot per entry).
* `SrcFormatCfgSpec` / `DestFormatCfgSpec` -- the per-format
  `format_cfg` dict, side-typed by the file-format enum.
* `EXEC_PLAN_ID` / `TRX_ID` / `EXEC_PLAN_TS` -- substitution
  tokens allowed in destination paths.

The per-format `format_cfg` dict is side-typed: only the keys valid
for the `Src` side (`has_header`, `separator`, ...) are accepted
on `*Src` instances; the `Dest`-side keys (`include_header`,
`compression`, ...) only on `*Dest` instances. A cross-side key
fails validation.
"""

from __future__ import annotations

from typing import Annotated, Any, Literal, TypeAlias

from tabsdatak.common._fileformat.taxonomy import DestFileFormat, SrcFileFormat

# noinspection PyProtectedMember
from tabsdatak.conn.common.file import _validation

# --- Path / format-cfg type aliases ---------------------------------


SrcPathNameSpec: TypeAlias = Annotated[str, _validation.SRC_PATH_VALIDATION]
"""A single source path, relative to the connection's `base_path`: it
must not start with `/` (it is relative, not absolute), end with `/`,
or contain empty segments (`//`). May use a `*` glob in its final
segment only; backslashes, NUL and `${...}` tokens are not allowed."""
SrcPathsSpec: TypeAlias = list[SrcPathNameSpec]
"""List of source paths, one output slot per entry (each entry follows
`SrcPathNameSpec`)."""

DestPathNameSpec: TypeAlias = Annotated[str, _validation.DEST_PATH_VALIDATION]
"""A single destination path, relative to the connection's `base_path`:
it must not start with `/` (it is relative, not absolute), end with
`/`, or contain empty segments (`//`). Globs are not allowed;
backslashes and NUL are not allowed; the tokens `${EXEC_PLAN_ID}`,
`${TRX_ID}` and `${EXEC_PLAN_TS}` are substituted at write time."""
DestPathsSpec: TypeAlias = list[DestPathNameSpec]
"""List of destination paths, one write slot per entry (each entry
follows `DestPathNameSpec`)."""


# --- CSV format-cfg key specs --------------------------------------
#
# The CSV reader / writer accept different option sets. The literals
# below are the single source of truth for what is allowed; the
# format-cfg validator extracts the allowed key set with
# `typing.get_args`.

CsvCommonKey: TypeAlias = Literal[
    "separator",
    "quote_char",
    "eol_char",
]
"""CSV options valid on both the read and write side (`separator`,
`quote_char`, `eol_char`)."""

CsvSrcKey: TypeAlias = Literal[
    "separator",
    "quote_char",
    "eol_char",
    "encoding",
    "null_values",
    "missing_is_null",
    "truncate_ragged_lines",
    "comment_prefix",
    "try_parse_dates",
    "decimal_comma",
    "has_header",
    "skip_rows",
    "skip_rows_after_header",
    "raise_if_empty",
    "ignore_errors",
]
"""CSV reader options accepted in a source `format_cfg[CSV]` entry."""

CsvDestKey: TypeAlias = Literal[
    "separator",
    "quote_char",
    "eol_char",
    "include_header",
    "datetime_format",
    "date_format",
    "time_format",
    "float_scientific",
    "float_precision",
    "null_value",
    "quote_style",
    "maintain_order",
]
"""CSV writer options accepted in a destination `format_cfg[CSV]`
entry."""

ExcelSrcKey = Literal["has_header"]
"""Excel reader options accepted in a source `format_cfg[EXCEL]` entry.

`has_header` (default `True`) decides whether the worksheet's first
non-empty row names the columns. With it off no row is spent on names,
every row is data, and the columns are called `column_1`, `column_2`, ...
"""

CsvSrcCfg: TypeAlias = dict[CsvSrcKey, Any]
CsvDestCfg: TypeAlias = dict[CsvDestKey, Any]
ExcelSrcCfg: TypeAlias = dict[ExcelSrcKey, Any]


SrcFormatCfgSpec: TypeAlias = dict[SrcFileFormat, CsvSrcCfg | ExcelSrcCfg | dict]
"""Per-format reader options for a source, keyed by `SrcFileFormat`.

Only `CSV` (keys typed by `CsvSrcKey`) and `EXCEL` (`ExcelSrcKey`)
accept non-empty options; every other format must map to `{}` or be
omitted. A destination-only key raises the connector's validate exception
at descriptor construction.
"""

DestFormatCfgSpec: TypeAlias = dict[DestFileFormat, CsvDestCfg | dict]
"""Per-format writer options for a destination, keyed by
`DestFileFormat`.

Only `CSV` accepts non-empty options today (keys typed by
`CsvDestKey`); every other format must map to `{}` or be omitted. A
source-only key raises the connector's validate exception at descriptor
construction.
"""


# --- Destination-path tokens ---------------------------------------
#
# Substitution tokens accepted in `Dest.path` entries. Each
# constant carries the full `${...}` marker so users can drop them
# directly into path strings (`f"out/{EXEC_PLAN_ID}/x.csv"`). The
# `DEST_PATH_TOKENS` set is the single source of truth the
# path validator consults.

EXEC_PLAN_ID: str = "${EXEC_PLAN_ID}"
"""Destination-path token replaced with the execution-plan id at write
time."""
TRX_ID: str = "${TRX_ID}"
"""Destination-path token replaced with the transaction id at write
time."""
EXEC_PLAN_TS: str = "${EXEC_PLAN_TS}"
"""Destination-path token replaced with the execution-plan timestamp at
write time."""

DEST_PATH_TOKENS: frozenset[str] = frozenset({
    EXEC_PLAN_ID,
    TRX_ID,
    EXEC_PLAN_TS,
})
"""The substitution tokens allowed in a destination path:
`${EXEC_PLAN_ID}`, `${TRX_ID}` and `${EXEC_PLAN_TS}`."""
