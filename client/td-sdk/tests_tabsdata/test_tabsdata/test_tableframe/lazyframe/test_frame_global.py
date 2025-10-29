#
# Copyright 2025 Tabs Data Inc.
#

import logging
import unittest

import polars as pl

import tabsdata as td

# noinspection PyProtectedMember
from tabsdata._utils.tableframe._translator import _wrap_polars_frame
from tabsdata.tableframe.lazyframe.properties import TableFramePropertiesBuilder

# noinspection PyUnresolvedReferences
from .. import pytestmark  # noqa: F401
from ..common import load_complex_dataframe, load_simple_dataframe, pretty_polars

logger = logging.getLogger(__name__)
logger.setLevel(logging.DEBUG)


class TestTableFrame(unittest.TestCase):

    def setUp(self):
        pretty_polars()

        self.data_frame_l, self.lazy_frame_l, self.table_frame_l = (
            load_complex_dataframe(token="l")
        )
        self.data_frame_r, self.lazy_frame_r, self.table_frame_r = (
            load_complex_dataframe(token="r")
        )

    def test_concat(self):
        _, _, tf_top = load_simple_dataframe(token="top-")
        _, _, tf_bottom = load_simple_dataframe(token="bottom-")
        tf = td.concat([tf_top, tf_bottom])
        assert tf is not None
        assert len(tf.to_polars_lf().collect().rows()) == 6

    def test_concat_single_frame(self):
        _, _, tf = load_simple_dataframe()
        result = td.concat([tf])
        assert result is not None
        assert len(result.to_polars_lf().collect().rows()) == 3

    def test_concat_multiple_frames(self):
        frames = [load_simple_dataframe()[2] for _ in range(5)]
        result = td.concat(frames)
        assert result is not None
        assert len(result.to_polars_lf().collect().rows()) == 15

    def test_concat_vertical_matching_schemas(self):
        tf1 = td.TableFrame.__build__(
            df=pl.LazyFrame({"a": [1, 2], "b": ["x", "y"]}),
            mode="raw",
            idx=0,
            properties=TableFramePropertiesBuilder.empty(),
        )
        tf2 = td.TableFrame.__build__(
            df=pl.LazyFrame({"a": [3, 4], "b": ["z", "w"]}),
            mode="raw",
            idx=0,
            properties=TableFramePropertiesBuilder.empty(),
        )
        result = td.concat([tf1, tf2], how="vertical")
        collected = result.to_polars_lf().collect()
        assert len(collected.rows()) == 4
        assert collected.columns == ["a", "b"]

    def test_concat_vertical_mismatched_schemas(self):
        tf1 = td.TableFrame.__build__(
            df=pl.LazyFrame({"a": [1, 2], "b": ["x", "y"]}),
            mode="raw",
            idx=0,
            properties=TableFramePropertiesBuilder.empty(),
        )
        tf2 = td.TableFrame.__build__(
            df=pl.LazyFrame({"a": [3, 4], "c": [10, 20]}),
            mode="raw",
            idx=0,
            properties=TableFramePropertiesBuilder.empty(),
        )
        with self.assertRaises(Exception):
            td.concat([tf1, tf2], how="vertical")

    def test_concat_diagonal_mismatched_schemas(self):
        tf1 = td.TableFrame.__build__(
            df=pl.LazyFrame({"a": [1, 2], "b": ["x", "y"]}),
            mode="raw",
            idx=0,
            properties=TableFramePropertiesBuilder.empty(),
        )
        tf2 = td.TableFrame.__build__(
            df=pl.LazyFrame({"a": [3, 4], "c": [10, 20]}),
            mode="raw",
            idx=0,
            properties=TableFramePropertiesBuilder.empty(),
        )
        result = td.concat([tf1, tf2], how="diagonal")
        collected = result.to_polars_lf().collect()
        assert len(collected.rows()) == 4
        assert set(collected.columns) == {"a", "b", "c"}
        assert collected["b"][2] is None
        assert collected["c"][0] is None

    def test_concat_vertical_relaxed_type_coercion(self):
        tf1 = td.TableFrame.__build__(
            df=pl.LazyFrame({"a": pl.Series([1, 2], dtype=pl.Int32)}),
            mode="raw",
            idx=0,
            properties=TableFramePropertiesBuilder.empty(),
        )
        tf2 = td.TableFrame.__build__(
            df=pl.LazyFrame({"a": pl.Series([3, 4], dtype=pl.Int64)}),
            mode="raw",
            idx=0,
            properties=TableFramePropertiesBuilder.empty(),
        )
        result = td.concat([tf1, tf2], how="vertical_relaxed")
        collected = result.to_polars_lf().collect()
        assert len(collected.rows()) == 4
        assert collected["a"].dtype == pl.Int64

    def test_concat_diagonal_relaxed_type_coercion(self):
        tf1 = td.TableFrame.__build__(
            df=pl.LazyFrame({"a": pl.Series([1, 2], dtype=pl.Int32), "b": [10, 20]}),
            mode="raw",
            idx=0,
            properties=TableFramePropertiesBuilder.empty(),
        )
        tf2 = td.TableFrame.__build__(
            df=pl.LazyFrame({"a": pl.Series([3, 4], dtype=pl.Int64), "c": ["x", "y"]}),
            mode="raw",
            idx=0,
            properties=TableFramePropertiesBuilder.empty(),
        )
        result = td.concat([tf1, tf2], how="diagonal_relaxed")
        collected = result.to_polars_lf().collect()
        assert len(collected.rows()) == 4
        assert collected["a"].dtype == pl.Int64
        assert set(collected.columns) == {"a", "b", "c"}

    def test_concat_invalid_method(self):
        _, _, tf = load_simple_dataframe()
        with self.assertRaises(ValueError) as context:
            # noinspection PyTypeChecker
            td.concat([tf], how="invalid_method")
        assert "Invalid concatenation method" in str(context.exception)

    def test_concat_relaxed_string_int_coerces_to_string(self):
        tf1 = td.TableFrame.__build__(
            df=pl.LazyFrame({"a": ["x", "y"]}),
            mode="raw",
            idx=0,
            properties=TableFramePropertiesBuilder.empty(),
        )
        tf2 = td.TableFrame.__build__(
            df=pl.LazyFrame({"a": [1, 2]}),
            mode="raw",
            idx=0,
            properties=TableFramePropertiesBuilder.empty(),
        )
        result = td.concat([tf1, tf2], how="vertical_relaxed")
        collected = result.to_polars_lf().collect()
        assert len(collected.rows()) == 4
        assert collected["a"].dtype == pl.String
        assert collected["a"][2] == "1"
        assert collected["a"][3] == "2"

    def test_concat_relaxed_numeric_types_int_float(self):
        tf1 = td.TableFrame.__build__(
            df=pl.LazyFrame({"a": pl.Series([1, 2], dtype=pl.Int32)}),
            mode="raw",
            idx=0,
            properties=TableFramePropertiesBuilder.empty(),
        )
        tf2 = td.TableFrame.__build__(
            df=pl.LazyFrame({"a": pl.Series([3.5, 4.5], dtype=pl.Float64)}),
            mode="raw",
            idx=0,
            properties=TableFramePropertiesBuilder.empty(),
        )
        result = td.concat([tf1, tf2], how="vertical_relaxed")
        collected = result.to_polars_lf().collect()
        assert len(collected.rows()) == 4
        assert collected["a"].dtype == pl.Float64

    def test_concat_relaxed_numeric_types_different_widths(self):
        tf1 = td.TableFrame.__build__(
            df=pl.LazyFrame({"a": pl.Series([1, 2], dtype=pl.Int8)}),
            mode="raw",
            idx=0,
            properties=TableFramePropertiesBuilder.empty(),
        )
        tf2 = td.TableFrame.__build__(
            df=pl.LazyFrame({"a": pl.Series([3, 4], dtype=pl.Int32)}),
            mode="raw",
            idx=0,
            properties=TableFramePropertiesBuilder.empty(),
        )
        tf3 = td.TableFrame.__build__(
            df=pl.LazyFrame({"a": pl.Series([5, 6], dtype=pl.Int64)}),
            mode="raw",
            idx=0,
            properties=TableFramePropertiesBuilder.empty(),
        )
        result = td.concat([tf1, tf2, tf3], how="vertical_relaxed")
        collected = result.to_polars_lf().collect()
        assert len(collected.rows()) == 6
        assert collected["a"].dtype == pl.Int64

    def test_concat_relaxed_float_types_different_widths(self):
        tf1 = td.TableFrame.__build__(
            df=pl.LazyFrame({"a": pl.Series([1.0, 2.0], dtype=pl.Float32)}),
            mode="raw",
            idx=0,
            properties=TableFramePropertiesBuilder.empty(),
        )
        tf2 = td.TableFrame.__build__(
            df=pl.LazyFrame({"a": pl.Series([3.0, 4.0], dtype=pl.Float64)}),
            mode="raw",
            idx=0,
            properties=TableFramePropertiesBuilder.empty(),
        )
        result = td.concat([tf1, tf2], how="vertical_relaxed")
        collected = result.to_polars_lf().collect()
        assert len(collected.rows()) == 4
        assert collected["a"].dtype == pl.Float64

    def test_concat_relaxed_bool_int_coerces_to_int(self):
        tf1 = td.TableFrame.__build__(
            df=pl.LazyFrame({"a": [True, False]}),
            mode="raw",
            idx=0,
            properties=TableFramePropertiesBuilder.empty(),
        )
        tf2 = td.TableFrame.__build__(
            df=pl.LazyFrame({"a": [1, 2]}),
            mode="raw",
            idx=0,
            properties=TableFramePropertiesBuilder.empty(),
        )
        result = td.concat([tf1, tf2], how="vertical_relaxed")
        collected = result.to_polars_lf().collect()
        assert len(collected.rows()) == 4
        assert collected["a"].dtype == pl.Int64
        assert collected["a"][0] == 1
        assert collected["a"][1] == 0

    def test_concat_relaxed_string_categorical(self):
        tf1 = td.TableFrame.__build__(
            df=pl.LazyFrame({"a": pl.Series(["x", "y"], dtype=pl.String)}),
            mode="raw",
            idx=0,
            properties=TableFramePropertiesBuilder.empty(),
        )
        tf2 = td.TableFrame.__build__(
            df=pl.LazyFrame({"a": pl.Series(["z", "w"], dtype=pl.Categorical)}),
            mode="raw",
            idx=0,
            properties=TableFramePropertiesBuilder.empty(),
        )
        result = td.concat([tf1, tf2], how="vertical_relaxed")
        collected = result.to_polars_lf().collect()
        assert len(collected.rows()) == 4

    def test_concat_diagonal_relaxed_mixed_compatible_types(self):
        tf1 = td.TableFrame.__build__(
            df=pl.LazyFrame(
                {
                    "a": pl.Series([1, 2], dtype=pl.Int32),
                    "b": pl.Series([10.0, 20.0], dtype=pl.Float32),
                }
            ),
            mode="raw",
            idx=0,
            properties=TableFramePropertiesBuilder.empty(),
        )
        tf2 = td.TableFrame.__build__(
            df=pl.LazyFrame(
                {
                    "a": pl.Series([3, 4], dtype=pl.Int64),
                    "c": ["x", "y"],
                }
            ),
            mode="raw",
            idx=0,
            properties=TableFramePropertiesBuilder.empty(),
        )
        tf3 = td.TableFrame.__build__(
            df=pl.LazyFrame(
                {
                    "b": pl.Series([30.0, 40.0], dtype=pl.Float64),
                    "c": ["z", "w"],
                }
            ),
            mode="raw",
            idx=0,
            properties=TableFramePropertiesBuilder.empty(),
        )
        result = td.concat([tf1, tf2, tf3], how="diagonal_relaxed")
        collected = result.to_polars_lf().collect()
        assert len(collected.rows()) == 6
        assert collected["a"].dtype == pl.Int64
        assert collected["b"].dtype == pl.Float64
        assert set(collected.columns) == {"a", "b", "c"}

    def test_concat_relaxed_date_int_coerces_to_int(self):
        tf1 = td.TableFrame.__build__(
            df=pl.LazyFrame(
                {"a": pl.Series(["2024-01-01", "2024-01-02"]).str.to_date()}
            ),
            mode="raw",
            idx=0,
            properties=TableFramePropertiesBuilder.empty(),
        )
        tf2 = td.TableFrame.__build__(
            df=pl.LazyFrame({"a": [1, 2]}),
            mode="raw",
            idx=0,
            properties=TableFramePropertiesBuilder.empty(),
        )
        result = td.concat([tf1, tf2], how="vertical_relaxed")
        collected = result.to_polars_lf().collect()
        assert len(collected.rows()) == 4
        assert collected["a"].dtype == pl.Int64

    def test_concat_vertical_requires_exact_column_match(self):
        tf1 = td.TableFrame.__build__(
            df=pl.LazyFrame({"a": [1, 2], "b": [10, 20], "c": [100, 200]}),
            mode="raw",
            idx=0,
            properties=TableFramePropertiesBuilder.empty(),
        )
        tf2 = td.TableFrame.__build__(
            df=pl.LazyFrame({"a": [3, 4], "b": [30, 40], "d": [300, 400]}),
            mode="raw",
            idx=0,
            properties=TableFramePropertiesBuilder.empty(),
        )
        with self.assertRaises(Exception):
            td.concat([tf1, tf2], how="vertical")

    def test_concat_vertical_requires_exact_columns_multiple_frames(self):
        tf1 = td.TableFrame.__build__(
            df=pl.LazyFrame({"a": [1], "b": [10], "c": [100], "d": [1000]}),
            mode="raw",
            idx=0,
            properties=TableFramePropertiesBuilder.empty(),
        )
        tf2 = td.TableFrame.__build__(
            df=pl.LazyFrame({"a": [2], "b": [20], "c": [200], "e": [2000]}),
            mode="raw",
            idx=0,
            properties=TableFramePropertiesBuilder.empty(),
        )
        tf3 = td.TableFrame.__build__(
            df=pl.LazyFrame({"a": [3], "b": [30], "f": [300]}),
            mode="raw",
            idx=0,
            properties=TableFramePropertiesBuilder.empty(),
        )
        with self.assertRaises(Exception):
            td.concat([tf1, tf2, tf3], how="vertical")

    def test_concat_vertical_no_common_columns(self):
        tf1 = td.TableFrame.__build__(
            df=pl.LazyFrame({"a": [1, 2]}),
            mode="raw",
            idx=0,
            properties=TableFramePropertiesBuilder.empty(),
        )
        tf2 = td.TableFrame.__build__(
            df=pl.LazyFrame({"b": [3, 4]}),
            mode="raw",
            idx=0,
            properties=TableFramePropertiesBuilder.empty(),
        )
        with self.assertRaises(Exception):
            td.concat([tf1, tf2], how="vertical")

    def test_concat_diagonal_fills_nulls_two_frames(self):
        tf1 = td.TableFrame.__build__(
            df=pl.LazyFrame({"a": [1, 2], "b": [10, 20]}),
            mode="raw",
            idx=0,
            properties=TableFramePropertiesBuilder.empty(),
        )
        tf2 = td.TableFrame.__build__(
            df=pl.LazyFrame({"a": [3, 4], "c": [30, 40]}),
            mode="raw",
            idx=0,
            properties=TableFramePropertiesBuilder.empty(),
        )
        result = td.concat([tf1, tf2], how="diagonal")
        collected = result.to_polars_lf().collect()
        assert len(collected.rows()) == 4
        assert set(collected.columns) == {"a", "b", "c"}
        assert collected["c"][0] is None
        assert collected["c"][1] is None
        assert collected["b"][2] is None
        assert collected["b"][3] is None

    def test_concat_diagonal_fills_nulls_multiple_frames(self):
        tf1 = td.TableFrame.__build__(
            df=pl.LazyFrame({"a": [1], "b": [10]}),
            mode="raw",
            idx=0,
            properties=TableFramePropertiesBuilder.empty(),
        )
        tf2 = td.TableFrame.__build__(
            df=pl.LazyFrame({"b": [20], "c": [200]}),
            mode="raw",
            idx=0,
            properties=TableFramePropertiesBuilder.empty(),
        )
        tf3 = td.TableFrame.__build__(
            df=pl.LazyFrame({"c": [300], "d": [3000]}),
            mode="raw",
            idx=0,
            properties=TableFramePropertiesBuilder.empty(),
        )
        tf4 = td.TableFrame.__build__(
            df=pl.LazyFrame({"a": [4], "d": [4000]}),
            mode="raw",
            idx=0,
            properties=TableFramePropertiesBuilder.empty(),
        )
        result = td.concat([tf1, tf2, tf3, tf4], how="diagonal")
        collected = result.to_polars_lf().collect()
        assert len(collected.rows()) == 4
        assert set(collected.columns) == {"a", "b", "c", "d"}
        assert collected["a"][0] == 1
        assert collected["b"][0] == 10
        assert collected["c"][0] is None
        assert collected["d"][0] is None
        assert collected["a"][1] is None
        assert collected["b"][1] == 20
        assert collected["c"][1] == 200
        assert collected["d"][1] is None
        assert collected["a"][2] is None
        assert collected["b"][2] is None
        assert collected["c"][2] == 300
        assert collected["d"][2] == 3000
        assert collected["a"][3] == 4
        assert collected["b"][3] is None
        assert collected["c"][3] is None
        assert collected["d"][3] == 4000

    def test_concat_diagonal_all_different_columns(self):
        tf1 = td.TableFrame.__build__(
            df=pl.LazyFrame({"a": [1]}),
            mode="raw",
            idx=0,
            properties=TableFramePropertiesBuilder.empty(),
        )
        tf2 = td.TableFrame.__build__(
            df=pl.LazyFrame({"b": [2]}),
            mode="raw",
            idx=0,
            properties=TableFramePropertiesBuilder.empty(),
        )
        tf3 = td.TableFrame.__build__(
            df=pl.LazyFrame({"c": [3]}),
            mode="raw",
            idx=0,
            properties=TableFramePropertiesBuilder.empty(),
        )
        result = td.concat([tf1, tf2, tf3], how="diagonal")
        collected = result.to_polars_lf().collect()
        assert len(collected.rows()) == 3
        assert set(collected.columns) == {"a", "b", "c"}
        assert (
            collected["a"][0] == 1
            and collected["b"][0] is None
            and collected["c"][0] is None
        )
        assert (
            collected["a"][1] is None
            and collected["b"][1] == 2
            and collected["c"][1] is None
        )
        assert (
            collected["a"][2] is None
            and collected["b"][2] is None
            and collected["c"][2] == 3
        )

    def test_concat_vertical_relaxed_requires_exact_columns(self):
        tf1 = td.TableFrame.__build__(
            df=pl.LazyFrame(
                {"a": pl.Series([1, 2], dtype=pl.Int32), "b": [10, 20], "c": [100, 200]}
            ),
            mode="raw",
            idx=0,
            properties=TableFramePropertiesBuilder.empty(),
        )
        tf2 = td.TableFrame.__build__(
            df=pl.LazyFrame(
                {"a": pl.Series([3, 4], dtype=pl.Int64), "b": [30, 40], "d": [300, 400]}
            ),
            mode="raw",
            idx=0,
            properties=TableFramePropertiesBuilder.empty(),
        )
        with self.assertRaises(Exception):
            td.concat([tf1, tf2], how="vertical_relaxed")

    def test_lit(self):
        lf = pl.LazyFrame(
            {
                "letters": ["a", "b", "c"],
                "numbers": [1, 2, 3],
            }
        )
        tf = _wrap_polars_frame(lf)
        tf = tf.with_columns([pl.lit("こんにちは世界").alias("token")])
        rows = tf._lf.collect()
        column = rows["token"]
        assert all(
            value == "こんにちは世界" for value in column
        ), "Token column values do not match expected value!"
