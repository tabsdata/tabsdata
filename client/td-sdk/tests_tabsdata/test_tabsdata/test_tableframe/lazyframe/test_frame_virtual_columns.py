#
# Copyright 2025 Tabs Data Inc.
#

import logging
import unittest
from datetime import datetime, timezone

import polars as pl

import tabsdata as td
from tabsdata.extensions._tableframe.extension import SystemColumns
from tabsdata.tableframe.lazyframe.properties import (
    TableFrameProperties,
    TableFramePropertiesBuilder,
)

# noinspection PyUnresolvedReferences
from .. import pytestmark  # noqa: F401
from ..common import pretty_polars

logger = logging.getLogger(__name__)
logger.setLevel(logging.DEBUG)


class TestTableFrameVirtualColumns(unittest.TestCase):

    def setUp(self):
        pretty_polars()

        self.execution_first = "exec-11111"
        self.transaction_first = "txn-11111"
        self.version_first = "ver-11111"
        self.timestamp_first = datetime(2019, 8, 28, 6, 28, 30, tzinfo=timezone.utc)
        self.properties_first = (
            TableFrameProperties.builder()
            .with_execution(self.execution_first)
            .with_transaction(self.transaction_first)
            .with_version(self.version_first)
            .with_timestamp(self.timestamp_first)
            .build()
        )

        self.execution_second = "exec-22222"
        self.transaction_second = "txn-22222"
        self.version_second = "ver-22222"
        self.timestamp_second = datetime(2022, 4, 22, 3, 14, 15, tzinfo=timezone.utc)
        self.properties_second = (
            TableFrameProperties.builder()
            .with_execution(self.execution_second)
            .with_transaction(self.transaction_second)
            .with_version(self.version_second)
            .with_timestamp(self.timestamp_second)
            .build()
        )

    def test_create_tableframe_with_properties_has_virtual_columns(self):
        data = pl.LazyFrame(
            {
                "a": [1, 2, 3],
                "b": ["x", "y", "z"],
            }
        )
        tf = td.TableFrame.__build__(
            df=data,
            mode="raw",
            idx=0,
            properties=self.properties_first,
        )
        columns = tf.columns("all")

        assert SystemColumns.TD_VER_EXECUTION.value in columns
        assert SystemColumns.TD_VER_TRANSACTION.value in columns
        assert SystemColumns.TD_VER_VERSION.value in columns
        assert SystemColumns.TD_VER_TIMESTAMP.value in columns

    def test_virtual_columns_have_correct_values(self):
        data = pl.LazyFrame(
            {
                "a": [1, 2, 3],
                "b": ["x", "y", "z"],
            }
        )
        tf = td.TableFrame.__build__(
            df=data,
            mode="raw",
            idx=0,
            properties=self.properties_first,
        )
        dataframe = tf._lf.collect()
        execution_column = dataframe[SystemColumns.TD_VER_EXECUTION.value].to_list()
        transaction_column = dataframe[SystemColumns.TD_VER_TRANSACTION.value].to_list()
        version_column = dataframe[SystemColumns.TD_VER_VERSION.value].to_list()
        timestamp_column = dataframe[SystemColumns.TD_VER_TIMESTAMP.value].to_list()

        assert all(val == self.execution_first for val in execution_column)
        assert all(val == self.transaction_first for val in transaction_column)
        assert all(val == self.version_first for val in version_column)
        assert all(val == self.timestamp_first for val in timestamp_column)

    def test_empty_properties_creates_default_values(self):
        data = pl.LazyFrame(
            {
                "a": [1, 2],
                "b": ["x", "y"],
            }
        )
        tf = td.TableFrame.__build__(
            df=data,
            mode="raw",
            idx=0,
            properties=TableFramePropertiesBuilder.empty(),
        )
        columns = tf.columns("all")

        assert SystemColumns.TD_VER_EXECUTION.value in columns
        assert SystemColumns.TD_VER_TRANSACTION.value in columns
        assert SystemColumns.TD_VER_VERSION.value in columns
        assert SystemColumns.TD_VER_TIMESTAMP.value in columns

    def test_properties_with_timestamp_as_milliseconds(self):
        execution_zeroth = "exec-0"
        transaction_zeroth = "txn-0"
        version_zeroth = "ver-0"
        datetime_zeroth = datetime(2025, 3, 25, 2, 7, 1, tzinfo=timezone.utc)
        timestamp_zeroth = datetime_zeroth.timestamp()
        properties = (
            TableFrameProperties.builder()
            .with_execution(execution_zeroth)
            .with_transaction(transaction_zeroth)
            .with_version(version_zeroth)
            .with_timestamp(int(timestamp_zeroth))
            .build()
        )
        data = pl.LazyFrame(
            {
                "a": [1],
            }
        )
        tf = td.TableFrame.__build__(
            df=data,
            mode="raw",
            idx=0,
            properties=properties,
        )
        dataframe = tf._lf.collect()
        timestamp_column_value = dataframe[
            SystemColumns.TD_VER_TIMESTAMP.value
        ].to_list()
        timestamp_column_expected = datetime.fromtimestamp(
            timestamp_zeroth / 1000, tz=timezone.utc
        )

        assert timestamp_column_value[0] == timestamp_column_expected

    def test_filter_preserves_virtual_columns(self):
        data = pl.LazyFrame(
            {
                "a": [1, 2, 3, 4, 5],
                "b": ["x", "y", "z", "w", "v"],
            }
        )
        tf = td.TableFrame.__build__(
            df=data,
            mode="raw",
            idx=0,
            properties=self.properties_first,
        )
        filtered_tf = tf.filter(td.col("a") > 2)
        dataframe = filtered_tf._lf.collect()
        execution_column = dataframe[SystemColumns.TD_VER_EXECUTION.value].to_list()
        transaction_column = dataframe[SystemColumns.TD_VER_TRANSACTION.value].to_list()
        version_column = dataframe[SystemColumns.TD_VER_VERSION.value].to_list()

        assert len(dataframe) == 3
        assert all(val == self.execution_first for val in execution_column)
        assert all(val == self.transaction_first for val in transaction_column)
        assert all(val == self.version_first for val in version_column)

    def test_select_preserves_virtual_columns(self):
        data = pl.LazyFrame(
            {
                "a": [1, 2, 3],
                "b": ["x", "y", "z"],
                "c": [10, 20, 30],
            }
        )
        tf = td.TableFrame.__build__(
            df=data,
            mode="raw",
            idx=0,
            properties=self.properties_first,
        )
        selected_tf = tf.select("a", "b")
        columns = selected_tf.columns("all")

        assert SystemColumns.TD_VER_EXECUTION.value in columns
        assert SystemColumns.TD_VER_TRANSACTION.value in columns
        assert SystemColumns.TD_VER_VERSION.value in columns
        assert SystemColumns.TD_VER_TIMESTAMP.value in columns

        dataframe = selected_tf._lf.collect()
        execution_column = dataframe[SystemColumns.TD_VER_EXECUTION.value].to_list()

        assert all(val == self.execution_first for val in execution_column)

    def test_with_columns_preserves_virtual_columns(self):
        data = pl.LazyFrame(
            {
                "a": [1, 2, 3],
                "b": [10, 20, 30],
            }
        )
        tf = td.TableFrame.__build__(
            df=data,
            mode="raw",
            idx=0,
            properties=self.properties_first,
        )
        modified_tf = tf.with_columns((td.col("a") * 2).alias("a_doubled"))
        dataframe = modified_tf._lf.collect()
        execution_column = dataframe[SystemColumns.TD_VER_EXECUTION.value].to_list()
        transaction_column = dataframe[SystemColumns.TD_VER_TRANSACTION.value].to_list()

        assert all(val == self.execution_first for val in execution_column)
        assert all(val == self.transaction_first for val in transaction_column)

    def test_rename_preserves_virtual_columns(self):
        data = pl.LazyFrame(
            {
                "a": [1, 2, 3],
                "b": ["x", "y", "z"],
            }
        )
        tf = td.TableFrame.__build__(
            df=data,
            mode="raw",
            idx=0,
            properties=self.properties_first,
        )
        renamed_tf = tf.rename({"a": "alpha", "b": "beta"})
        columns = renamed_tf.columns("all")

        assert SystemColumns.TD_VER_EXECUTION.value in columns
        assert SystemColumns.TD_VER_VERSION.value in columns

        dataframe = renamed_tf._lf.collect()
        version_column = dataframe[SystemColumns.TD_VER_VERSION.value].to_list()

        assert all(val == self.version_first for val in version_column)

    def test_sort_preserves_virtual_columns(self):
        data = pl.LazyFrame(
            {
                "a": [3, 1, 2],
                "b": ["z", "x", "y"],
            }
        )
        tf = td.TableFrame.__build__(
            df=data,
            mode="raw",
            idx=0,
            properties=self.properties_first,
        )
        sorted_tf = tf.sort("a")
        dataframe = sorted_tf._lf.collect()
        execution_column = dataframe[SystemColumns.TD_VER_EXECUTION.value].to_list()
        timestamp_column = dataframe[SystemColumns.TD_VER_TIMESTAMP.value].to_list()

        assert all(val == self.execution_first for val in execution_column)
        assert all(val == self.timestamp_first for val in timestamp_column)

    def test_unique_preserves_virtual_columns(self):
        data = pl.LazyFrame(
            {
                "a": [1, 2, 2, 3, 3, 3],
                "b": ["x", "y", "y", "z", "z", "z"],
            }
        )
        tf = td.TableFrame.__build__(
            df=data,
            mode="raw",
            idx=0,
            properties=self.properties_first,
        )
        unique_tf = tf.unique(subset=["a"])
        dataframe = unique_tf._lf.collect()
        transaction_column = dataframe[SystemColumns.TD_VER_TRANSACTION.value].to_list()

        assert all(val == self.transaction_first for val in transaction_column)

    def test_limit_preserves_virtual_columns(self):
        data = pl.LazyFrame(
            {
                "a": [1, 2, 3, 4, 5],
                "b": ["a", "b", "c", "d", "e"],
            }
        )
        tf = td.TableFrame.__build__(
            df=data,
            mode="raw",
            idx=0,
            properties=self.properties_first,
        )

        limited_tf = tf.limit(3)
        dataframe = limited_tf._lf.collect()
        assert len(dataframe) == 3

        version_column = dataframe[SystemColumns.TD_VER_VERSION.value].to_list()

        assert all(val == self.version_first for val in version_column)

    def test_drop_preserves_virtual_columns(self):
        data = pl.LazyFrame(
            {
                "a": [1, 2, 3],
                "b": ["x", "y", "z"],
                "c": [10, 20, 30],
            }
        )
        tf = td.TableFrame.__build__(
            df=data,
            mode="raw",
            idx=0,
            properties=self.properties_first,
        )
        dropped_tf = tf.drop("c")
        columns = dropped_tf.columns("all")

        assert SystemColumns.TD_VER_EXECUTION.value in columns
        assert SystemColumns.TD_VER_TRANSACTION.value in columns

        dataframe = dropped_tf._lf.collect()
        execution_column = dataframe[SystemColumns.TD_VER_EXECUTION.value].to_list()

        assert all(val == self.execution_first for val in execution_column)

    def test_drop_nulls_preserves_virtual_columns(self):
        data = pl.LazyFrame(
            {
                "a": [1, None, 3],
                "b": ["x", "y", None],
            }
        )
        tf = td.TableFrame.__build__(
            df=data,
            mode="raw",
            idx=0,
            properties=self.properties_first,
        )
        cleaned_tf = tf.drop_nulls()
        dataframe = cleaned_tf._lf.collect()

        assert len(dataframe) == 1

        execution_column = dataframe[SystemColumns.TD_VER_EXECUTION.value].to_list()

        assert execution_column[0] == self.execution_first

    def test_group_by_preserves_virtual_columns_context(self):
        data = pl.LazyFrame(
            {
                "category": ["A", "B", "A", "B", "A"],
                "value": [10, 20, 30, 40, 50],
            }
        )
        tf = td.TableFrame.__build__(
            df=data,
            mode="raw",
            idx=0,
            properties=self.properties_first,
        )
        grouped = tf.group_by("category").agg(td.col("value").sum())
        columns = grouped.columns("all")

        assert SystemColumns.TD_VER_EXECUTION.value in columns
        assert SystemColumns.TD_VER_TRANSACTION.value in columns

    def test_concat_maintains_individual_row_properties(self):
        data1 = pl.LazyFrame(
            {
                "a": [1, 2],
                "b": ["x", "y"],
            }
        )
        tf1 = td.TableFrame.__build__(
            df=data1,
            mode="raw",
            idx=0,
            properties=self.properties_first,
        )
        data2 = pl.LazyFrame(
            {
                "a": [3, 4],
                "b": ["z", "w"],
            }
        )
        tf2 = td.TableFrame.__build__(
            df=data2,
            mode="raw",
            idx=0,
            properties=self.properties_second,
        )

        concatenated = td.concat([tf1, tf2])
        dataframe = concatenated._lf.collect()

        assert len(dataframe) == 4

        execution_column = dataframe[SystemColumns.TD_VER_EXECUTION.value].to_list()
        transaction_column = dataframe[SystemColumns.TD_VER_TRANSACTION.value].to_list()
        version_column = dataframe[SystemColumns.TD_VER_VERSION.value].to_list()
        timestamp_column = dataframe[SystemColumns.TD_VER_TIMESTAMP.value].to_list()

        assert execution_column[0] == self.execution_first
        assert execution_column[1] == self.execution_first
        assert transaction_column[0] == self.transaction_first
        assert transaction_column[1] == self.transaction_first
        assert version_column[0] == self.version_first
        assert version_column[1] == self.version_first
        assert timestamp_column[0] == self.timestamp_first
        assert timestamp_column[1] == self.timestamp_first

        assert execution_column[2] == self.execution_second
        assert execution_column[3] == self.execution_second
        assert transaction_column[2] == self.transaction_second
        assert transaction_column[3] == self.transaction_second
        assert version_column[2] == self.version_second
        assert version_column[3] == self.version_second
        assert timestamp_column[2] == self.timestamp_second
        assert timestamp_column[3] == self.timestamp_second

    def test_concat_multiple_frames_maintains_properties(self):
        q = 16
        frames = []
        expected_executions = []
        for i in range(q):
            data = pl.LazyFrame(
                {
                    "a": [i * 10, i * 10 + 1],
                }
            )
            props = (
                TableFrameProperties.builder()
                .with_execution(f"exec-{i}")
                .with_transaction(f"txn-{i}")
                .with_version(f"v{i}.0.0")
                .with_timestamp(datetime(2025, 1, i + 1, tzinfo=timezone.utc))
                .build()
            )
            tf = td.TableFrame.__build__(
                df=data,
                mode="raw",
                idx=0,
                properties=props,
            )
            frames.append(tf)
            expected_executions.extend([f"exec-{i}", f"exec-{i}"])
        concatenated = td.concat(frames)
        dataframe = concatenated._lf.collect()

        assert len(dataframe) == q * 2

        execution_column = dataframe[SystemColumns.TD_VER_EXECUTION.value].to_list()

        assert execution_column == expected_executions

    def test_concat_vertical_maintains_properties(self):
        data1 = pl.LazyFrame(
            {
                "a": [1, 2],
                "b": ["x", "y"],
            }
        )
        tf1 = td.TableFrame.__build__(
            df=data1,
            mode="raw",
            idx=0,
            properties=self.properties_first,
        )
        data2 = pl.LazyFrame(
            {
                "a": [3, 4],
                "b": ["z", "w"],
            }
        )
        tf2 = td.TableFrame.__build__(
            df=data2,
            mode="raw",
            idx=0,
            properties=self.properties_second,
        )
        concatenated = td.concat([tf1, tf2], how="vertical")
        dataframe = concatenated._lf.collect()
        version_column = dataframe[SystemColumns.TD_VER_VERSION.value].to_list()

        assert version_column[:2] == [self.version_first, self.version_first]
        assert version_column[2:] == [self.version_second, self.version_second]

    def test_concat_diagonal_maintains_properties(self):
        data1 = pl.LazyFrame(
            {
                "a": [1, 2],
                "b": ["x", "y"],
            }
        )
        tf1 = td.TableFrame.__build__(
            df=data1,
            mode="raw",
            idx=0,
            properties=self.properties_first,
        )
        data2 = pl.LazyFrame(
            {
                "a": [3, 4],
                "c": [100, 200],
            }
        )
        tf2 = td.TableFrame.__build__(
            df=data2,
            mode="raw",
            idx=0,
            properties=self.properties_second,
        )
        concatenated = td.concat([tf1, tf2], how="diagonal")
        dataframe = concatenated._lf.collect()
        transaction_column = dataframe[SystemColumns.TD_VER_TRANSACTION.value].to_list()

        assert transaction_column[:2] == [
            self.transaction_first,
            self.transaction_first,
        ]
        assert transaction_column[2:] == [
            self.transaction_second,
            self.transaction_second,
        ]

    def test_join_inner_preserves_left_properties(self):
        data_left = pl.LazyFrame(
            {
                "key": [1, 2, 3],
                "value_l": ["a", "b", "c"],
            }
        )
        tf_left = td.TableFrame.__build__(
            df=data_left,
            mode="raw",
            idx=0,
            properties=self.properties_first,
        )
        data_right = pl.LazyFrame(
            {
                "key": [2, 3, 4],
                "value_r": ["x", "y", "z"],
            }
        )
        tf_right = td.TableFrame.__build__(
            df=data_right,
            mode="raw",
            idx=0,
            properties=self.properties_second,
        )
        joined = tf_left.join(tf_right, on="key", how="inner")
        dataframe = joined._lf.collect()

        assert len(dataframe) == 2

        execution_column = dataframe[SystemColumns.TD_VER_EXECUTION.value].to_list()
        version_column = dataframe[SystemColumns.TD_VER_VERSION.value].to_list()

        assert all(val == self.execution_first for val in execution_column)
        assert all(val == self.version_first for val in version_column)

    def test_join_left_preserves_left_properties(self):
        data_left = pl.LazyFrame(
            {
                "key": [1, 2, 3],
                "value_l": ["a", "b", "c"],
            }
        )
        tf_left = td.TableFrame.__build__(
            df=data_left,
            mode="raw",
            idx=0,
            properties=self.properties_first,
        )
        data_right = pl.LazyFrame(
            {
                "key": [2, 4],
                "value_r": ["x", "z"],
            }
        )
        tf_right = td.TableFrame.__build__(
            df=data_right,
            mode="raw",
            idx=0,
            properties=self.properties_second,
        )
        joined = tf_left.join(tf_right, on="key", how="left")
        dataframe = joined._lf.collect()

        assert len(dataframe) == 3

        transaction_column = dataframe[SystemColumns.TD_VER_TRANSACTION.value].to_list()
        timestamp_column = dataframe[SystemColumns.TD_VER_TIMESTAMP.value].to_list()

        assert all(val == self.transaction_first for val in transaction_column)
        assert all(val == self.timestamp_first for val in timestamp_column)

    def test_join_full_preserves_respective_properties(self):
        data_left = pl.LazyFrame(
            {
                "key": [1, 2],
                "value_l": ["a", "b"],
            }
        )
        tf_left = td.TableFrame.__build__(
            df=data_left,
            mode="raw",
            idx=0,
            properties=self.properties_first,
        )
        data_right = pl.LazyFrame(
            {
                "key": [2, 3],
                "value_r": ["x", "y"],
            }
        )
        tf_right = td.TableFrame.__build__(
            df=data_right,
            mode="raw",
            idx=0,
            properties=self.properties_second,
        )
        joined = tf_left.join(tf_right, on="key", how="full")
        dataframe = joined._lf.collect()

        assert len(dataframe) == 3

        columns = joined.columns("all")

        assert SystemColumns.TD_VER_EXECUTION.value in columns
        assert SystemColumns.TD_VER_TRANSACTION.value in columns
