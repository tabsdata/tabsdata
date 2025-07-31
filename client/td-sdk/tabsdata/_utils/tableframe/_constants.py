#
# Copyright 2025 Tabs Data Inc.
#

from __future__ import annotations

from enum import Enum

import polars as pl

import tabsdata._utils.tableframe._generators as td_generators

PYTEST_CONTEXT_ACTIVE = "PYTEST_VERSION"

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


class Inception(Enum):
    # When the system column is kept as is when storing the table.
    PROPAGATE = "propagate"
    # When the system column is computed when storing the table.
    REGENERATE = "regenerate"


TD_COLUMN_PREFIX = "$td."
TD_COLUMN_PREFIX_REGEXP = "^\\$td\\..*$"

TD_COL_DEFAULT = "default"
TD_COL_DTYPE = "dtype"
TD_COL_GENERATOR = "generator"
TD_COL_INCEPTION = "inception"
TD_COL_AGGREGATION = "aggregation"


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
    TD_IDENTIFIER = {
        TD_COL_DEFAULT: td_generators._id_default,
        TD_COL_DTYPE: pl.String,
        TD_COL_GENERATOR: td_generators.IdGenerator,
        TD_COL_INCEPTION: Inception.REGENERATE,
        TD_COL_AGGREGATION: None,
    }


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
