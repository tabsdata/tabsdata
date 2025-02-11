#
# Copyright 2025 Tabs Data Inc.
#

from __future__ import annotations

from enum import Enum

import polars as pl

import tabsdata.utils.tableframe._generators as td_generators

PYTEST_CONTEXT_ACTIVE = "PYTEST_VERSION"

DUPLICATE_METHODS = ["collect_schema"]
FUNCTION_METHODS = ["pipe"]
INTERNAL_METHODS = [
    "_comparison_error",
    "_fetch",
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
    "melt",
    "reverse",
    "shift",
    "merge_sorted",
    "rename",
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

TD_COLUMN_PREFIX = "$td."

TD_COL_DEFAULT = "default"
TD_COL_DTYPE = "dtype"
TD_COL_GENERATOR = "generator"


class StandardSystemColumns(Enum):
    TD_IDENTIFIER = "$td.id"
    TD_OFFSET = "$td.offset"


class StandardVolatileSystemColumns(Enum):
    TD_ITEM_COLUMN = "$td._item"
    TD_MIN_COLUMN = "$td._min"
    TD_MAX_COLUMN = "$td._max"


class StandardSystemColumnsMetadata(Enum):
    # noinspection PyProtectedMember
    TD_IDENTIFIER = {
        TD_COL_DEFAULT: td_generators._id_default,
        TD_COL_DTYPE: pl.String,
        TD_COL_GENERATOR: td_generators._id,
    }


REGEXP_ANCHOR_START = "^"
REGEXP_ANCHOR_END = "$"
