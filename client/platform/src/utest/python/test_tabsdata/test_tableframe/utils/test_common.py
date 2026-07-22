#
# Copyright 2025 Tabsdata Inc.
#

import logging
from datetime import datetime, timezone

import polars as pl

# noinspection PyProtectedMember
from tabsdata._utils.tableframe._common import add_system_columns, drop_system_columns

# noinspection PyProtectedMember
from tabsdata._utils.tableframe._helpers import (
    OPTIONAL_SYSTEM_COLUMNS,
    SYSTEM_COLUMNS_METADATA,
)

# noinspection PyProtectedMember
from tabsdata._utils.tableframe._translator import (
    _unwrap_table_frame,
    _wrap_polars_frame,
)
from tabsdata.tableframe.lazyframe.frame import TableFrame
from tabsdata.tableframe.lazyframe.properties import TableFrameProperties

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

    properties = (
        TableFrameProperties.builder()
        .with_execution("e")
        .with_transaction("t")
        .with_version("v")
        .with_timestamp(datetime.now(tz=timezone.utc))
        .build()
    )
    lf = add_system_columns(lf=lf, mode="raw", idx=0, properties=properties)

    df = lf.collect()
    system_columns = SYSTEM_COLUMNS_METADATA
    optional_system_columns = set(OPTIONAL_SYSTEM_COLUMNS)
    existing_columns = set(lf.collect().columns)
    required_system_columns = set(system_columns.keys()) - optional_system_columns
    missing_columns = required_system_columns - existing_columns
    assert not missing_columns, f"Missing system columns after add: {missing_columns}"
    for column, metadata in system_columns.items():
        if column in optional_system_columns:
            continue
        expected_dtype = metadata.dtype
        actual_dtype = df.schema[column]
        assert (
            actual_dtype == expected_dtype
        ), f"Type mismatch for {column}: expected {expected_dtype}, got {actual_dtype}"

    lf = drop_system_columns(lf=lf)

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


def test_optional_system_columns_not_auto_created():
    lf = pl.LazyFrame({"data": [1, 2, 3]})
    properties = (
        TableFrameProperties.builder()
        .with_execution("e")
        .with_transaction("t")
        .with_version("v")
        .with_timestamp(datetime.now(tz=timezone.utc))
        .build()
    )
    lf = add_system_columns(lf=lf, mode="raw", idx=0, properties=properties)
    columns = set(lf.collect().columns)
    system_optional = set(OPTIONAL_SYSTEM_COLUMNS)
    assert not (columns & system_optional), (
        f"Optional columns should not be auto-created: {columns & system_optional}"
    )


def test_optional_system_columns_preserved_when_present():
    lf = pl.LazyFrame({
        "data": [1, 2, 3],
        "$td.opt.tst.srv": ["s1", "s2", "s3"],
        "$td.opt.tst.usr": ["u1", "u2", "u3"],
        "$td.opt.tst.ts": pl.Series(
            [datetime(2026, 1, 1, tzinfo=timezone.utc)] * 3,
            dtype=pl.Datetime(time_unit="us", time_zone="UTC"),
        ),
        "$td.opt.tst.fn": ["f1", "f2", "f3"],
    })
    properties = (
        TableFrameProperties.builder()
        .with_execution("e")
        .with_transaction("t")
        .with_version("v")
        .with_timestamp(datetime.now(tz=timezone.utc))
        .build()
    )
    lf = add_system_columns(lf=lf, mode="tab", idx=0, properties=properties)
    df = lf.collect()
    assert "$td.opt.tst.srv" in df.columns
    assert "$td.opt.tst.usr" in df.columns
    assert "$td.opt.tst.ts" in df.columns
    assert "$td.opt.tst.fn" in df.columns
    assert df["$td.opt.tst.srv"].to_list() == ["s1", "s2", "s3"]
    assert df["$td.opt.tst.usr"].to_list() == ["u1", "u2", "u3"]
    assert df["$td.opt.tst.ts"].to_list() == [datetime(2026, 1, 1, tzinfo=timezone.utc)] * 3
    assert df["$td.opt.tst.fn"].to_list() == ["f1", "f2", "f3"]


def test_table_frame_from_dictionary():
    _ = TableFrame(
        {
            "letters": ["a", "b", "c"],
            "numbers": [1, 2, 3],
        }
    )
