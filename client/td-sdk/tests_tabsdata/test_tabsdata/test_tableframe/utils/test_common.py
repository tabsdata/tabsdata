#
# Copyright 2025 Tabs Data Inc.
#

import logging

import polars as pl

from tabsdata.tableframe.lazyframe.frame import TableFrame

# noinspection PyProtectedMember
from tabsdata.utils.tableframe._common import add_system_columns, drop_system_columns

# noinspection PyProtectedMember
from tabsdata.utils.tableframe._constants import TD_COL_DTYPE

# noinspection PyProtectedMember
from tabsdata.utils.tableframe._helpers import SYSTEM_COLUMNS_METADATA

# noinspection PyProtectedMember
from tabsdata.utils.tableframe._translator import (
    _unwrap_table_frame,
    _wrap_polars_frame,
)

from .. import pytestmark  # noqa: F401

logger = logging.getLogger(__name__)
logger.setLevel(logging.DEBUG)


def test_add_and_drop_system_columns():
    lf = pl.LazyFrame(
        {
            "letters": ["a", "b", "c"],
            "numbers": [1, 2, 3],
        }
    )

    lf = add_system_columns(lf, 0)

    df = lf.collect()
    system_columns = SYSTEM_COLUMNS_METADATA
    existing_columns = set(lf.collect().columns)
    missing_columns = set(system_columns.keys()) - existing_columns
    assert not missing_columns, f"Missing system columns after add: {missing_columns}"
    for column, metadata in system_columns.items():
        expected_dtype = metadata[TD_COL_DTYPE]
        actual_dtype = df.schema[column]
        assert (
            actual_dtype == expected_dtype
        ), f"Type mismatch for {column}: expected {expected_dtype}, got {actual_dtype}"

    lf = drop_system_columns(lf)

    df = lf.collect()
    remaining_columns = set(system_columns.keys()) & set(df.columns)
    assert not remaining_columns, f"System columns not removed: {remaining_columns}"


def test_wrap_and_unwrap_lazy_frame():
    lf = pl.LazyFrame(
        {
            "letters": ["a", "b", "c"],
            "numbers": [1, 2, 3],
        }
    )

    tf = _wrap_polars_frame(lf)
    lf = _unwrap_table_frame(tf)
    lf.collect()


def test_table_frame_from_none():
    _ = TableFrame(None)


def test_table_frame_from_void():
    _ = TableFrame()


def test_table_frame_from_empty():
    _ = TableFrame.empty()


def test_table_frame_from_dictionary():
    _ = TableFrame(
        {
            "letters": ["a", "b", "c"],
            "numbers": [1, 2, 3],
        }
    )
