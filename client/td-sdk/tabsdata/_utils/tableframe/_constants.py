#
# Copyright 2025 Tabs Data Inc.
#

from __future__ import annotations

from dataclasses import dataclass
from enum import Enum
from typing import Any

import polars as pl

import tabsdata._utils.tableframe._generators as td_generators

PYTEST_CONTEXT_ACTIVE = "PYTEST_VERSION"

TD_SYMLINK_POLARS_LIBS_PYTEST = "TD_SYMLINK_POLARS_LIBS_PYTEST"

DUPLICATE_METHODS = ["collect_schema"]
FUNCTION_METHODS = ["pipe"]
INTERNAL_METHODS = [
    "_comparison_error",
    "_fetch",
    "_filter",
    "_from_pyldf",
    "_scan_python_function",
    "_set_sink_optimizations",
]
MATERIALIZE_METHODS = [
    "collect",
    "collect_async",
    "describe",
    "fetch",
    "max",
    "mean",
    "median",
    "min",
    "null_count",
    "profile",
    "quantile",
    "std",
    "sum",
    "var",
]
RENAME_METHODS = ["with_context"]
UNNECESSARY_METHODS = ["lazy"]
UNRECOMMENDED_METHODS = ["cache"]
UNSUPPORTED_METHODS = [
    "approx_n_unique",
    "bottom_k",
    "clone",
    "count",
    "deserialize",
    "explode",
    "gather_every",
    "group_by_dynamic",
    "interpolate",
    "join_asof",
    "map_batches",
    "match_to_schema",
    "melt",
    "reverse",
    "shift",
    "merge_sorted",
    "remote",
    "remove",
    "rolling",
    "select_seq",
    "set_sorted",
    "serialize",
    "top_k",
    "unnest",
    "unpivot",
    "with_columns_seq",
    "with_row_count",
    "with_row_index",
    "__setstate__",
]
UNSTABLE_METHODS = [
    "_to_metadata",
    "join_where",
    "sink_csv",
    "sink_ipc",
    "sink_ndjson",
    "sink_parquet",
    "sql",
    "update",
]


class Language(Enum):
    PYTHON = "python"
    RUST = "rust"


class Inception(Enum):
    # When the system column is kept as is when storing the table.
    PROPAGATE = "propagate"
    # When the system column is computed when storing the table.
    REGENERATE = "regenerate"


TD_COLUMN_PREFIX = "$td."
TD_COLUMN_PREFIX_REGEXP = "^\\$td\\..*$"


@dataclass(slots=True, eq=True, frozen=True)
class SystemColumn:
    default: Any
    dtype: Any
    language: Language
    generator: Any
    inception: Inception
    aggregation: Any

    def __str__(self) -> str:
        generator_ = getattr(self.generator, "__name__", None) or (
            "None" if self.generator is None else "<callable>"
        )
        aggregation_ = getattr(self.aggregation, "__name__", None) or (
            "None" if self.aggregation is None else "<callable>"
        )
        return (
            "SystemColumn("
            f"default={self.default} - "
            f"dtype={self.dtype} - "
            f"language={self.language} - "
            f"generator={generator_} - "
            f"inception={self.inception} - "
            f"aggregation={aggregation_}"
            ")"
        )


class StandardSystemColumns(Enum):
    TD_IDENTIFIER = "$td.id"
    TD_OFFSET = "$td.offset"


class StandardVolatileSystemColumns(Enum):
    TD_INDEX_COLUMN = "$td._index"
    TD_ITEM_COLUMN = "$td._item"
    TD_MIN_COLUMN = "$td._min"
    TD_MAX_COLUMN = "$td._max"


class StandardSystemColumnsMetadata(Enum):
    # noinspection PyProtectedMember
    TD_IDENTIFIER = SystemColumn(
        dtype=pl.String,
        default=td_generators._id_default,
        language=Language.RUST,
        generator=td_generators.IdGenerator,
        inception=Inception.REGENERATE,
        aggregation=None,
    )


REGEXP_ANCHOR_START = "^"
REGEXP_ANCHOR_END = "$"


# ⚠️ ⚠️ ⚠️
# Do not change the values of the entries in this enum; they are part of the
# public API and are used in various places the data persisted in the storage.
# Changing their names is safe, although highly discouraged if there is no
# strong reason to do so.
class RowOperation(Enum):
    ROW = 0
    UNDEFINED = 1
    GROUP_MIN = 2
    GROUP_MAX = 3
    GROUP_SUM = 4
    GROUP_LEN = 5
    GROUP_COUNT = 6
    GROUP_MEEAN = 7
    GROUP_MEDIAN = 8
    GROUP_UNIQUE = 9
