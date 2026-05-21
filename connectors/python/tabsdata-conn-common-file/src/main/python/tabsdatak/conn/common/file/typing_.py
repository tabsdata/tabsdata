"""Shared types for the cloud-file (object-store) connectors:
``azureblob``, ``gcs``, ``localfile``, ``s3``.

* :class:`SrcFileFormat` / :class:`DestFileFormat` -- enumerations
  of the parquet / csv / json / avro / log formats the file
  connectors can read and write. The ``AUTO`` variant lets the
  transporter infer the format from the file extension.
* :class:`FileInfo` -- per-row provenance enrichment opt-in
  (``BASENAME`` / ``FULLPATH`` / ``NONE``).
* Field aliases (``PathsSpec`` / ``FormatCfgSpec``) used by every
  cloud-file ``Src`` / ``Dest`` dataclass for the per-flow
  ``paths`` list and per-format ``format_cfg`` dict.

The per-format ``format_cfg`` dict is side-typed: only the keys
appropriate for the ``Src`` side (``has_header``, ``separator``,
...) are accepted on ``*Src`` instances; the ``Dest``-side keys
(``include_header``, ``compression``, ...) are accepted only on
``*Dest`` instances. Cross-side keys fail field validation.
"""

from __future__ import annotations

from enum import Enum
from typing import Annotated, Any, Literal, TypeAlias

# noinspection PyProtectedMember
from tabsdatak.conn.common.file import _validation

# --- Format enums ---------------------------------------------------


class SrcFileFormat(Enum):
    AUTO = "AUTO"
    CSV = "CSV"
    JSON = "JSON"
    AVRO = "AVRO"
    PARQUET = "PARQUET"
    LOG = "LOG"


class DestFileFormat(Enum):
    AUTO = "AUTO"
    CSV = "CSV"
    JSON = "JSON"
    AVRO = "AVRO"
    PARQUET = "PARQUET"


class FileInfo(Enum):
    FILENAME = "FILENAME"
    FULLPATH = "FULLPATH"


# --- Path / format-cfg type aliases ---------------------------------


SrcPathNameSpec: TypeAlias = Annotated[str, _validation.SRC_PATH_VALIDATOR]
SrcPathsSpec: TypeAlias = list[SrcPathNameSpec]

DestPathNameSpec: TypeAlias = Annotated[str, _validation.DEST_PATH_VALIDATOR]
DestPathsSpec: TypeAlias = list[DestPathNameSpec]


# --- CSV format-cfg key specs --------------------------------------
#
# The CSV reader / writer accept different option sets. The literals
# below are the single source of truth for what is allowed; the
# format-cfg validator extracts the allowed key set with
# ``typing.get_args``.

CsvCommonKey: TypeAlias = Literal[
    "separator",
    "quote_char",
    "eol_char",
]

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

CsvSrcCfg: TypeAlias = dict[CsvSrcKey, Any]
CsvDestCfg: TypeAlias = dict[CsvDestKey, Any]


# `SrcFormatCfgSpec` -- per-format config dict for **sources**.
#
# Today only ``SrcFileFormat.CSV`` accepts non-empty values; every
# other entry must map to ``{}`` (or be omitted entirely). The CSV
# entry's keys are typed by :data:`CsvSrcKey`; mixing in a
# destination-only key (``include_header``, ``datetime_format``, ...)
# raises ``ConnValidateException(<NAME>_5)`` at descriptor
# construction.
SrcFormatCfgSpec: TypeAlias = dict[SrcFileFormat, CsvSrcCfg | dict]

# `DestFormatCfgSpec` -- per-format config dict for **destinations**.
#
# Today only ``DestFileFormat.CSV`` accepts non-empty values; every
# other entry must map to ``{}`` (or be omitted entirely). The CSV
# entry's keys are typed by :data:`CsvDestKey`; mixing in a
# source-only key (``has_header``, ``encoding``, ...) raises
# ``ConnValidateException(<NAME>_5)`` at descriptor construction.
DestFormatCfgSpec: TypeAlias = dict[DestFileFormat, CsvDestCfg | dict]


# --- Destination-path tokens ---------------------------------------
#
# Substitution tokens accepted in ``Dest.path`` entries. Each
# constant carries the full ``${...}`` marker so users can drop them
# directly into path strings (``f"out/{EXEC_PLAN_ID}/x.csv"``). The
# :data:`DEST_PATH_TOKENS` set is the single source of truth the
# path validator consults.

EXEC_PLAN_ID: str = "${EXEC_PLAN_ID}"
TRX_ID: str = "${TRX_ID}"
EXEC_PLAN_TS: str = "${EXEC_PLAN_TS}"

DEST_PATH_TOKENS: frozenset[str] = frozenset({
    EXEC_PLAN_ID,
    TRX_ID,
    EXEC_PLAN_TS,
})
