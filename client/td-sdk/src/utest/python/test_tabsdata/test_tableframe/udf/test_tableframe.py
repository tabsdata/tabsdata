#
# Copyright 2025 Tabs Data Inc.
#

from itertools import product
from typing import cast

import pytest
from polars.exceptions import DuplicateError

import tabsdata as td
import tabsdata.tableframe.typing as td_typing
from tabsdata.tableframe.udf.function import (
    SIGNATURE_LIST,
    SIGNATURE_UNPACKED,
    UDFList,
    UDFUnpacked,
)
from tests_tabsdata.test_tabsdata.test_tableframe.common import (
    load_normalized_complex_dataframe,
    load_simple_dataframe,
    pretty_polars,
)

pretty_polars()


class SquareUDFList(UDFList):
    def on_batch(self, series: list[td_typing.Series]) -> list[td_typing.Series]:
        squared = series[0] * series[0]
        return [squared]


class SquareUDFUnpacked(UDFUnpacked):
    def on_batch(self, series_0: td_typing.Series) -> list[td_typing.Series]:
        squared = series_0 * series_0
        return [squared]


class SquareElementUDFList(UDFList):
    def on_element(self, values: list) -> list:
        return [values[0] * values[0]]


class SquareElementUDFUnpacked(UDFUnpacked):
    def on_element(self, value_0) -> list:
        return [value_0 * value_0]


class TestTableFrameUDFSingleColumn:
    @pytest.mark.parametrize("udf_class", [SquareUDFList, SquareUDFUnpacked])
    def test_single_column_on_batch_simple(self, udf_class):
        _, _, tf = load_simple_dataframe()
        original_df = tf._lf.collect()
        original_rows, original_cols = original_df.shape

        result = tf.udf(td.col("intColumn"), udf_class([("squared", td.Int64)]))

        assert isinstance(result, td.TableFrame)
        collected = result._lf.collect()
        assert collected.shape == (original_rows, original_cols + 1)
        assert "squared" in collected.columns
        assert collected["squared"].to_list() == [1, 4, 9]
        assert "intColumn" in collected.columns
        assert "stringColumn" in collected.columns

    @pytest.mark.parametrize(
        "udf_class", [SquareElementUDFList, SquareElementUDFUnpacked]
    )
    def test_single_column_on_element_simple(self, udf_class):
        _, _, tf = load_simple_dataframe()
        original_df = tf._lf.collect()
        original_rows, original_cols = original_df.shape

        result = tf.udf(td.col("intColumn"), udf_class([("squared", td.Int64)]))

        assert isinstance(result, td.TableFrame)
        collected = result._lf.collect()
        assert collected.shape == (original_rows, original_cols + 1)
        assert "squared" in collected.columns
        assert collected["squared"].to_list() == [1, 4, 9]
        assert "intColumn" in collected.columns
        assert "stringColumn" in collected.columns

    @pytest.mark.parametrize("udf_class", [SquareUDFList, SquareUDFUnpacked])
    def test_single_column_on_batch_complex(self, udf_class):
        class DoubleValueUDFList(UDFList):
            def on_batch(
                self, series: list[td_typing.Series]
            ) -> list[td_typing.Series]:
                doubled = cast(td_typing.Series, cast(object, series[0] * 2))
                return [doubled]

        class DoubleValueUDFUnpacked(UDFUnpacked):
            def on_batch(self, series_0: td_typing.Series) -> list[td_typing.Series]:
                doubled = cast(td_typing.Series, cast(object, series_0 * 2))
                return [doubled]

        udf_map = {
            SquareUDFList: DoubleValueUDFList,
            SquareUDFUnpacked: DoubleValueUDFUnpacked,
        }
        actual_udf_class = udf_map[udf_class]

        _, _, tf = load_normalized_complex_dataframe()
        original_df = tf._lf.collect()
        original_rows, original_cols = original_df.shape
        original_columns = set(original_df.columns)

        result = tf.udf(
            td.col("bill_length_mm"), actual_udf_class([("doubledValue", td.Float64)])
        )

        assert isinstance(result, td.TableFrame)
        collected = result._lf.collect()
        assert collected.shape == (original_rows, original_cols + 1)
        assert "doubledValue" in collected.columns
        for col in original_columns:
            assert col in collected.columns
        mask = collected["bill_length_mm"].is_not_null()
        assert cast(
            td_typing.Series,
            cast(
                object,
                collected.filter(mask)["doubledValue"]
                == cast(
                    td_typing.Series,
                    cast(object, collected.filter(mask)["bill_length_mm"] * 2),
                ),
            ),
        ).all()

    @pytest.mark.parametrize(
        "udf_class", [SquareElementUDFList, SquareElementUDFUnpacked]
    )
    def test_single_column_on_element_complex(self, udf_class):
        class BillRatioUDFList(UDFList):
            def on_element(self, values: list) -> list:
                if values[0] is None:
                    return [None]
                return [values[0] * 2]

        class BillRatioUDFUnpacked(UDFUnpacked):
            def on_element(self, value_0) -> list:
                if value_0 is None:
                    return [None]
                return [value_0 * 2]

        udf_map = {
            SquareElementUDFList: BillRatioUDFList,
            SquareElementUDFUnpacked: BillRatioUDFUnpacked,
        }
        actual_udf_class = udf_map[udf_class]

        _, _, tf = load_normalized_complex_dataframe()
        original_df = tf._lf.collect()
        original_rows, original_cols = original_df.shape
        original_columns = set(original_df.columns)

        result = tf.udf(
            td.col("bill_length_mm"), actual_udf_class([("doubledValue", td.Float64)])
        )

        assert isinstance(result, td.TableFrame)
        collected = result._lf.collect()
        assert collected.shape == (original_rows, original_cols + 1)
        assert "doubledValue" in collected.columns
        for col in original_columns:
            assert col in collected.columns
        mask = collected["bill_length_mm"].is_not_null()
        assert cast(
            td_typing.Series,
            cast(
                object,
                collected.filter(mask)["doubledValue"]
                == cast(
                    td_typing.Series,
                    cast(object, collected.filter(mask)["bill_length_mm"] * 2),
                ),
            ),
        ).all()


class TestTableFrameUDFMultipleColumns:
    @pytest.mark.parametrize("signature", [SIGNATURE_LIST, SIGNATURE_UNPACKED])
    def test_multiple_columns_on_batch_sum(self, signature):
        class SumColumnsUDFList(UDFList):
            def on_batch(
                self, series: list[td_typing.Series]
            ) -> list[td_typing.Series]:
                f_result = series[0]
                for s in series[1:]:
                    f_result = f_result + s
                return [f_result]

        class SumColumnsUDFUnpacked(UDFUnpacked):
            def on_batch(self, *series: td_typing.Series) -> list[td_typing.Series]:
                f_result = series[0]
                for s in series[1:]:
                    f_result = f_result + s
                return [f_result]

        udf_class = (
            SumColumnsUDFList if signature == SIGNATURE_LIST else SumColumnsUDFUnpacked
        )

        data = {"a": [1, 2, 3], "b": [10, 20, 30], "c": [100, 200, 300]}
        tf = td.TableFrame(data)
        original_df = tf._lf.collect()
        original_rows, original_cols = original_df.shape
        original_columns = set(original_df.columns)

        result = tf.udf(td.col("a", "b", "c"), udf_class([("sum", td.Int64)]))

        assert isinstance(result, td.TableFrame)
        collected = result._lf.collect()
        assert collected.shape == (original_rows, original_cols + 1)
        assert "sum" in collected.columns
        assert collected["sum"].to_list() == [111, 222, 333]
        for col in original_columns:
            assert col in collected.columns

    @pytest.mark.parametrize("signature", [SIGNATURE_LIST, SIGNATURE_UNPACKED])
    def test_multiple_columns_on_element_sum(self, signature):
        class SumElementUDFList(UDFList):
            def on_element(self, values: list) -> list:
                return [sum(values)]

        class SumElementUDFUnpacked(UDFUnpacked):
            def on_element(self, *values) -> list:
                return [sum(values)]

        udf_class = (
            SumElementUDFList if signature == SIGNATURE_LIST else SumElementUDFUnpacked
        )

        data = {"a": [1, 2, 3], "b": [10, 20, 30], "c": [100, 200, 300]}
        tf = td.TableFrame(data)
        original_df = tf._lf.collect()
        original_rows, original_cols = original_df.shape
        original_columns = set(original_df.columns)

        result = tf.udf(td.col("a", "b", "c"), udf_class([("sum", td.Int64)]))

        assert isinstance(result, td.TableFrame)
        collected = result._lf.collect()
        assert collected.shape == (original_rows, original_cols + 1)
        assert "sum" in collected.columns
        assert collected["sum"].to_list() == [111, 222, 333]
        for col in original_columns:
            assert col in collected.columns

    @pytest.mark.parametrize("signature", [SIGNATURE_LIST, SIGNATURE_UNPACKED])
    def test_multiple_columns_on_batch_complex_dataset(self, signature):
        class BillAreaUDFList(UDFList):
            def on_batch(
                self, series: list[td_typing.Series]
            ) -> list[td_typing.Series]:
                bill_area = series[0] * series[1]
                return [bill_area]

        class BillAreaUDFUnpacked(UDFUnpacked):
            def on_batch(
                self, series_0: td_typing.Series, series_1: td_typing.Series
            ) -> list[td_typing.Series]:
                bill_area = series_0 * series_1
                return [bill_area]

        udf_class = (
            BillAreaUDFList if signature == SIGNATURE_LIST else BillAreaUDFUnpacked
        )

        _, _, tf = load_normalized_complex_dataframe()
        original_df = tf._lf.collect()
        original_rows, original_cols = original_df.shape
        original_columns = set(original_df.columns)

        result = tf.udf(
            td.col("bill_length_mm", "bill_depth_mm"),
            udf_class([("bill_area_mm2", td.Float64)]),
        )

        assert isinstance(result, td.TableFrame)
        collected = result._lf.collect()
        assert collected.shape == (original_rows, original_cols + 1)
        assert "bill_area_mm2" in collected.columns
        for col in original_columns:
            assert col in collected.columns

    @pytest.mark.parametrize("signature", [SIGNATURE_LIST, SIGNATURE_UNPACKED])
    def test_multiple_columns_on_element_complex_dataset(self, signature):
        class BillRatioUDFList(UDFList):
            def on_element(self, values: list) -> list:
                if values[0] is None or values[1] is None or values[1] == 0:
                    return [None]
                return [values[0] / values[1]]

        class BillRatioUDFUnpacked(UDFUnpacked):
            def on_element(self, value_0, value_1) -> list:
                if value_0 is None or value_1 is None or value_1 == 0:
                    return [None]
                return [value_0 / value_1]

        udf_class = (
            BillRatioUDFList if signature == SIGNATURE_LIST else BillRatioUDFUnpacked
        )

        _, _, tf = load_normalized_complex_dataframe()
        original_df = tf._lf.collect()
        original_rows, original_cols = original_df.shape
        original_columns = set(original_df.columns)

        result = tf.udf(
            td.col("bill_length_mm", "bill_depth_mm"),
            udf_class([("bill_ratio", td.Float64)]),
        )

        assert isinstance(result, td.TableFrame)
        collected = result._lf.collect()
        assert collected.shape == (original_rows, original_cols + 1)
        assert "bill_ratio" in collected.columns
        for col in original_columns:
            assert col in collected.columns


class TestTableFrameUDFMultipleOutputs:
    @pytest.mark.parametrize("signature", [SIGNATURE_LIST, SIGNATURE_UNPACKED])
    def test_multiple_outputs_on_batch(self, signature):
        class StatsUDFList(UDFList):
            def on_batch(
                self, series: list[td_typing.Series]
            ) -> list[td_typing.Series]:
                doubled = cast(td_typing.Series, cast(object, series[0] * 2))
                tripled = cast(td_typing.Series, cast(object, series[0] * 3))
                return [doubled, tripled]

        class StatsUDFUnpacked(UDFUnpacked):
            def on_batch(self, series_0: td_typing.Series) -> list[td_typing.Series]:
                doubled = cast(td_typing.Series, cast(object, series_0 * 2))
                tripled = cast(td_typing.Series, cast(object, series_0 * 3))
                return [doubled, tripled]

        udf_class = StatsUDFList if signature == SIGNATURE_LIST else StatsUDFUnpacked

        _, _, tf = load_simple_dataframe()
        original_df = tf._lf.collect()
        original_rows, original_cols = original_df.shape
        original_columns = set(original_df.columns)

        result = tf.udf(
            td.col("intColumn"),
            udf_class([("doubled", td.Int64), ("tripled", td.Int64)]),
        )

        assert isinstance(result, td.TableFrame)
        collected = result._lf.collect()
        assert collected.shape == (original_rows, original_cols + 2)
        assert "doubled" in collected.columns
        assert "tripled" in collected.columns
        assert collected["doubled"].to_list() == [2, 4, 6]
        assert collected["tripled"].to_list() == [3, 6, 9]
        for col in original_columns:
            assert col in collected.columns

    @pytest.mark.parametrize("signature", [SIGNATURE_LIST, SIGNATURE_UNPACKED])
    def test_multiple_outputs_on_element(self, signature):
        class MultiStatsUDFList(UDFList):
            def on_element(self, values: list) -> list:
                value = values[0]
                return [value * 2, value * 3, value * 4]

        class MultiStatsUDFUnpacked(UDFUnpacked):
            def on_element(self, value) -> list:
                return [value * 2, value * 3, value * 4]

        udf_class = (
            MultiStatsUDFList if signature == SIGNATURE_LIST else MultiStatsUDFUnpacked
        )

        _, _, tf = load_simple_dataframe()
        original_df = tf._lf.collect()
        original_rows, original_cols = original_df.shape
        original_columns = set(original_df.columns)

        result = tf.udf(
            td.col("intColumn"),
            udf_class(
                [
                    ("doubled", td.Int64),
                    ("tripled", td.Int64),
                    ("quadrupled", td.Int64),
                ]
            ),
        )

        assert isinstance(result, td.TableFrame)
        collected = result._lf.collect()
        assert collected.shape == (original_rows, original_cols + 3)
        assert "doubled" in collected.columns
        assert "tripled" in collected.columns
        assert "quadrupled" in collected.columns
        assert collected["doubled"].to_list() == [2, 4, 6]
        assert collected["tripled"].to_list() == [3, 6, 9]
        assert collected["quadrupled"].to_list() == [4, 8, 12]
        for col in original_columns:
            assert col in collected.columns

    @pytest.mark.parametrize("signature", [SIGNATURE_LIST, SIGNATURE_UNPACKED])
    def test_multiple_outputs_from_multiple_inputs_on_batch(self, signature):
        class SumAndProductUDFList(UDFList):
            def on_batch(
                self, series: list[td_typing.Series]
            ) -> list[td_typing.Series]:
                sum_result = series[0] + series[1]
                product_result = series[0] * series[1]
                return [sum_result, product_result]

        class SumAndProductUDFUnpacked(UDFUnpacked):
            def on_batch(
                self, series_0: td_typing.Series, series_1: td_typing.Series
            ) -> list[td_typing.Series]:
                sum_result = series_0 + series_1
                product_result = series_0 * series_1
                return [sum_result, product_result]

        udf_class = (
            SumAndProductUDFList
            if signature == SIGNATURE_LIST
            else SumAndProductUDFUnpacked
        )

        data = {"a": [1, 2, 3], "b": [4, 5, 6]}
        tf = td.TableFrame(data)
        original_df = tf._lf.collect()
        original_rows, original_cols = original_df.shape
        original_columns = set(original_df.columns)

        result = tf.udf(
            td.col("a", "b"),
            udf_class([("sum", td.Int64), ("product", td.Int64)]),
        )

        assert isinstance(result, td.TableFrame)
        collected = result._lf.collect()
        assert collected.shape == (original_rows, original_cols + 2)
        assert "sum" in collected.columns
        assert "product" in collected.columns
        assert collected["sum"].to_list() == [5, 7, 9]
        assert collected["product"].to_list() == [4, 10, 18]
        for col in original_columns:
            assert col in collected.columns

    @pytest.mark.parametrize("signature", [SIGNATURE_LIST, SIGNATURE_UNPACKED])
    def test_multiple_outputs_from_multiple_inputs_on_element(self, signature):
        class SumAndProductUDFList(UDFList):
            def on_element(self, values: list) -> list:
                return [values[0] + values[1], values[0] * values[1]]

        class SumAndProductUDFUnpacked(UDFUnpacked):
            def on_element(self, value_0, value_1) -> list:
                return [value_0 + value_1, value_0 * value_1]

        udf_class = (
            SumAndProductUDFList
            if signature == SIGNATURE_LIST
            else SumAndProductUDFUnpacked
        )

        data = {"a": [1, 2, 3], "b": [4, 5, 6]}
        tf = td.TableFrame(data)
        original_df = tf._lf.collect()
        original_rows, original_cols = original_df.shape
        original_columns = set(original_df.columns)

        result = tf.udf(
            td.col("a", "b"),
            udf_class([("sum", td.Int64), ("product", td.Int64)]),
        )

        assert isinstance(result, td.TableFrame)
        collected = result._lf.collect()
        assert collected.shape == (original_rows, original_cols + 2)
        assert "sum" in collected.columns
        assert "product" in collected.columns
        assert collected["sum"].to_list() == [5, 7, 9]
        assert collected["product"].to_list() == [4, 10, 18]
        for col in original_columns:
            assert col in collected.columns

    @pytest.mark.parametrize("signature", [SIGNATURE_LIST, SIGNATURE_UNPACKED])
    def test_output_dtype_spec(self, signature):
        class StatsUDFList(UDFList):
            def on_batch(
                self, series: list[td_typing.Series]
            ) -> list[td_typing.Series]:
                doubled = cast(td_typing.Series, cast(object, series[0] * 2))
                tripled = cast(td_typing.Series, cast(object, series[0] * 3))
                quadrupled = cast(td_typing.Series, cast(object, series[0] * 4))
                return [doubled, tripled, quadrupled]

        class StatsUDFUnpacked(UDFUnpacked):
            def on_batch(self, series_0: td_typing.Series) -> list[td_typing.Series]:
                doubled = cast(td_typing.Series, cast(object, series_0 * 2))
                tripled = cast(td_typing.Series, cast(object, series_0 * 3))
                quadrupled = cast(td_typing.Series, cast(object, series_0 * 4))
                return [doubled, tripled, quadrupled]

        udf_class = StatsUDFList if signature == SIGNATURE_LIST else StatsUDFUnpacked

        _, _, tf = load_simple_dataframe()
        original_df = tf._lf.collect()
        original_rows, original_cols = original_df.shape
        original_columns = set(original_df.columns)

        result = tf.udf(
            td.col("intColumn"),
            udf_class(
                [
                    ("doubled", td.Int64),
                    ("tripled", td.Float64),
                    ("quadrupled", td.String),
                ]
            ),
        )

        assert isinstance(result, td.TableFrame)
        collected = result._lf.collect()
        assert collected.shape == (original_rows, original_cols + 3)
        assert "doubled" in collected.columns
        assert "tripled" in collected.columns
        assert "quadrupled" in collected.columns
        assert collected["doubled"].to_list() == [2, 4, 6]
        assert collected["tripled"].to_list() == [3.0, 6.0, 9.0]
        assert collected["quadrupled"].to_list() == ["4", "8", "12"]
        for col in original_columns:
            assert col in collected.columns


class TestTableFrameUDFStateful:
    @pytest.mark.parametrize("signature", [SIGNATURE_LIST, SIGNATURE_UNPACKED])
    def test_stateful_udf_with_fixed_attribute_on_batch(self, signature):
        class MultiplierUDFList(UDFList):
            def __init__(self, multiplier: float, output_columns):
                super().__init__(output_columns)
                self.multiplier = multiplier

            def on_batch(
                self, series: list[td_typing.Series]
            ) -> list[td_typing.Series]:
                f_result = cast(
                    td_typing.Series, cast(object, series[0] * self.multiplier)
                )
                return [f_result]

        class MultiplierUDFUnpacked(UDFUnpacked):
            def __init__(self, multiplier: float, output_columns):
                super().__init__(output_columns)
                self.multiplier = multiplier

            def on_batch(self, series_0: td_typing.Series) -> list[td_typing.Series]:
                f_result = cast(
                    td_typing.Series, cast(object, series_0 * self.multiplier)
                )
                return [f_result]

        udf_class = (
            MultiplierUDFList if signature == SIGNATURE_LIST else MultiplierUDFUnpacked
        )

        _, _, tf = load_simple_dataframe()
        original_df = tf._lf.collect()
        original_rows, original_cols = original_df.shape
        original_columns = set(original_df.columns)

        udf_instance = udf_class(5.0, [("multiplied", td.Float64)])
        result = tf.udf(td.col("intColumn"), udf_instance)

        assert isinstance(result, td.TableFrame)
        collected = result._lf.collect()
        assert collected.shape == (original_rows, original_cols + 1)
        assert "multiplied" in collected.columns
        assert collected["multiplied"].to_list() == [5.0, 10.0, 15.0]
        for col in original_columns:
            assert col in collected.columns

    @pytest.mark.parametrize("signature", [SIGNATURE_LIST, SIGNATURE_UNPACKED])
    def test_stateful_udf_with_fixed_attribute_on_element(self, signature):
        class AddConstantUDFList(UDFList):
            def __init__(self, constant: int, output_columns):
                super().__init__(output_columns)
                self.constant = constant

            def on_element(self, values: list) -> list:
                return [values[0] + self.constant]

        class AddConstantUDFUnpacked(UDFUnpacked):
            def __init__(self, constant: int, output_columns):
                super().__init__(output_columns)
                self.constant = constant

            def on_element(self, value) -> list:
                return [value + self.constant]

        udf_class = (
            AddConstantUDFList
            if signature == SIGNATURE_LIST
            else AddConstantUDFUnpacked
        )

        _, _, tf = load_simple_dataframe()
        original_df = tf._lf.collect()
        original_rows, original_cols = original_df.shape
        original_columns = set(original_df.columns)

        udf_instance = udf_class(100, [("with_constant", td.Int64)])
        result = tf.udf(td.col("intColumn"), udf_instance)

        assert isinstance(result, td.TableFrame)
        collected = result._lf.collect()
        assert collected.shape == (original_rows, original_cols + 1)
        assert "with_constant" in collected.columns
        assert collected["with_constant"].to_list() == [101, 102, 103]
        for col in original_columns:
            assert col in collected.columns

    @pytest.mark.parametrize("signature", [SIGNATURE_LIST, SIGNATURE_UNPACKED])
    def test_stateful_udf_with_counter_on_batch(self, signature):
        class BatchCounterUDFList(UDFList):
            def __init__(self, output_columns):
                super().__init__(output_columns)
                self.batch_count = 0
                self.total_rows_processed = 0

            def on_batch(
                self, series: list[td_typing.Series]
            ) -> list[td_typing.Series]:
                self.batch_count += 1
                batch_size = len(series[0])
                self.total_rows_processed += batch_size

                f_result = cast(
                    td_typing.Series,
                    cast(object, series[0] + (self.batch_count * 1000)),
                )
                return [f_result]

        class BatchCounterUDFUnpacked(UDFUnpacked):
            def __init__(self, output_columns):
                super().__init__(output_columns)
                self.batch_count = 0
                self.total_rows_processed = 0

            def on_batch(self, series_0: td_typing.Series) -> list[td_typing.Series]:
                self.batch_count += 1
                batch_size = len(series_0)
                self.total_rows_processed += batch_size

                f_result = cast(
                    td_typing.Series,
                    cast(object, series_0 + (self.batch_count * 1000)),
                )
                return [f_result]

        udf_class = (
            BatchCounterUDFList
            if signature == SIGNATURE_LIST
            else BatchCounterUDFUnpacked
        )

        _, _, tf = load_simple_dataframe()
        original_df = tf._lf.collect()
        original_rows, original_cols = original_df.shape
        original_columns = set(original_df.columns)

        udf = udf_class([("with_batch_number", td.Int64)])
        result = tf.udf(td.col("intColumn"), udf)

        assert isinstance(result, td.TableFrame)
        collected = result._lf.collect()
        assert collected.shape == (original_rows, original_cols + 1)
        assert "with_batch_number" in collected.columns
        for col in original_columns:
            assert col in collected.columns
        assert udf.batch_count > 0
        assert udf.total_rows_processed == 3

    @pytest.mark.parametrize("signature", [SIGNATURE_LIST, SIGNATURE_UNPACKED])
    def test_stateful_udf_with_accumulator_on_element(self, signature):
        class AccumulatorUDFList(UDFList):
            def __init__(self, output_columns):
                super().__init__(output_columns)
                self.running_sum = 0

            def on_element(self, values: list) -> list:
                self.running_sum += values[0]
                return [self.running_sum]

        class AccumulatorUDFUnpacked(UDFUnpacked):
            def __init__(self, output_columns):
                super().__init__(output_columns)
                self.running_sum = 0

            def on_element(self, value) -> list:
                self.running_sum += value
                return [self.running_sum]

        udf_class = (
            AccumulatorUDFList
            if signature == SIGNATURE_LIST
            else AccumulatorUDFUnpacked
        )

        data = {"a": [1, 2, 3, 4, 5]}
        tf = td.TableFrame(data)
        original_df = tf._lf.collect()
        original_rows, original_cols = original_df.shape
        original_columns = set(original_df.columns)

        udf = udf_class([("cumulative_sum", td.Int64)])
        result = tf.udf(td.col("a"), udf)

        assert isinstance(result, td.TableFrame)
        collected = result._lf.collect()
        assert collected.shape == (original_rows, original_cols + 1)
        assert "cumulative_sum" in collected.columns
        for col in original_columns:
            assert col in collected.columns
        assert udf.running_sum > 0


class TestTableFrameUDFLargeDatasets:
    @pytest.mark.parametrize("signature", [SIGNATURE_LIST, SIGNATURE_UNPACKED])
    def test_large_dataset_multiple_batches_on_batch(self, signature):
        class BatchTrackingUDFList(UDFList):
            def __init__(self, output_columns):
                super().__init__(output_columns)
                self.batch_sizes = []

            def on_batch(
                self, series: list[td_typing.Series]
            ) -> list[td_typing.Series]:
                self.batch_sizes.append(len(series[0]))
                f_result = cast(td_typing.Series, cast(object, series[0] * 2))
                return [f_result]

        class BatchTrackingUDFUnpacked(UDFUnpacked):
            def __init__(self, output_columns):
                super().__init__(output_columns)
                self.batch_sizes = []

            def on_batch(self, series_0: td_typing.Series) -> list[td_typing.Series]:
                self.batch_sizes.append(len(series_0))
                f_result = cast(td_typing.Series, cast(object, series_0 * 2))
                return [f_result]

        udf_class = (
            BatchTrackingUDFList
            if signature == SIGNATURE_LIST
            else BatchTrackingUDFUnpacked
        )

        n_rows = 1_000_000
        data = {"values": list(range(n_rows))}
        tf = td.TableFrame(data)
        original_df = tf._lf.collect()
        original_rows, original_cols = original_df.shape
        original_columns = set(original_df.columns)

        udf = udf_class([("doubled", td.Int64)])
        result = tf.udf(td.col("values"), udf)

        assert isinstance(result, td.TableFrame)
        collected = result._lf.collect()
        assert collected.shape == (original_rows, original_cols + 1)
        assert "doubled" in collected.columns
        for col in original_columns:
            assert col in collected.columns
        total_processed = sum(udf.batch_sizes)
        assert total_processed == n_rows

    @pytest.mark.parametrize("signature", [SIGNATURE_LIST, SIGNATURE_UNPACKED])
    def test_large_dataset_multiple_batches_on_element(self, signature):
        class ElementCounterUDFList(UDFList):
            def __init__(self, output_columns):
                super().__init__(output_columns)
                self.element_count = 0

            def on_element(self, values: list) -> list:
                self.element_count += 1
                return [values[0] + 1]

        class ElementCounterUDFUnpacked(UDFUnpacked):
            def __init__(self, output_columns):
                super().__init__(output_columns)
                self.element_count = 0

            def on_element(self, value) -> list:
                self.element_count += 1
                return [value + 1]

        udf_class = (
            ElementCounterUDFList
            if signature == SIGNATURE_LIST
            else ElementCounterUDFUnpacked
        )

        n_rows = 1_000_000
        data = {"values": list(range(n_rows))}
        tf = td.TableFrame(data)
        original_df = tf._lf.collect()
        original_rows, original_cols = original_df.shape
        original_columns = set(original_df.columns)

        udf = udf_class([("incremented", td.Int64)])
        result = tf.udf(td.col("values"), udf)

        assert isinstance(result, td.TableFrame)
        collected = result._lf.collect()
        assert collected.shape == (original_rows, original_cols + 1)
        assert "incremented" in collected.columns
        for col in original_columns:
            assert col in collected.columns
        assert udf.element_count == n_rows

    @pytest.mark.parametrize("signature", [SIGNATURE_LIST, SIGNATURE_UNPACKED])
    def test_complex_dataset_with_nulls_on_batch(self, signature):
        class NullHandlingUDFList(UDFList):
            def on_batch(
                self, series: list[td_typing.Series]
            ) -> list[td_typing.Series]:
                filled = series[0].fill_null(-1)
                f_result = cast(td_typing.Series, cast(object, filled * 2))
                return [f_result]

        class NullHandlingUDFUnpacked(UDFUnpacked):
            def on_batch(self, series_0: td_typing.Series) -> list[td_typing.Series]:
                filled = series_0.fill_null(-1)
                f_result = cast(td_typing.Series, cast(object, filled * 2))
                return [f_result]

        udf_class = (
            NullHandlingUDFList
            if signature == SIGNATURE_LIST
            else NullHandlingUDFUnpacked
        )

        _, _, tf = load_normalized_complex_dataframe()
        original_df = tf._lf.collect()
        original_rows, original_cols = original_df.shape
        original_columns = set(original_df.columns)

        result = tf.udf(td.col("body_mass_g"), udf_class([("handled", td.Float64)]))

        assert isinstance(result, td.TableFrame)
        collected = result._lf.collect()
        assert collected.shape == (original_rows, original_cols + 1)
        assert "handled" in collected.columns
        for col in original_columns:
            assert col in collected.columns

    @pytest.mark.parametrize("signature", [SIGNATURE_LIST, SIGNATURE_UNPACKED])
    def test_complex_dataset_with_nulls_on_element(self, signature):
        class NullHandlingElementUDFList(UDFList):
            def on_element(self, values: list) -> list:
                val = values[0] if values[0] is not None else -1
                return [val * 2]

        class NullHandlingElementUDFUnpacked(UDFUnpacked):
            def on_element(self, value) -> list:
                val = value if value is not None else -1
                return [val * 2]

        udf_class = (
            NullHandlingElementUDFList
            if signature == SIGNATURE_LIST
            else NullHandlingElementUDFUnpacked
        )

        _, _, tf = load_normalized_complex_dataframe()
        original_df = tf._lf.collect()
        original_rows, original_cols = original_df.shape
        original_columns = set(original_df.columns)

        result = tf.udf(td.col("body_mass_g"), udf_class([("handled", td.Float64)]))

        assert isinstance(result, td.TableFrame)
        collected = result._lf.collect()
        assert collected.shape == (original_rows, original_cols + 1)
        assert "handled" in collected.columns
        for col in original_columns:
            assert col in collected.columns


class TestTableFrameUDFErrors:
    # noinspection PyTypeChecker
    def test_udf_with_invalid_udf_instance(self):
        _, _, tf = load_simple_dataframe()

        expected_exceptions: tuple[type[BaseException], ...] = (
            AttributeError,
            TypeError,
        )
        with pytest.raises(expected_exceptions):
            tf.udf(td.col("intColumn"), lambda x: x * 2)

    @pytest.mark.parametrize("signature", [SIGNATURE_LIST, SIGNATURE_UNPACKED])
    def test_udf_with_invalid_expression(self, signature):
        class SimpleUDFList(UDFList):
            def on_batch(
                self, series: list[td_typing.Series]
            ) -> list[td_typing.Series]:
                return series

        class SimpleUDFUnpacked(UDFUnpacked):
            def on_batch(self, *series: td_typing.Series) -> list[td_typing.Series]:
                return list(series)

        udf_class = SimpleUDFList if signature == SIGNATURE_LIST else SimpleUDFUnpacked

        _, _, tf = load_simple_dataframe()

        with pytest.raises(Exception):
            result = tf.udf(
                td.col("nonexistent_column"), udf_class([("result", td.Int64)])
            )
            result.collect()

    @pytest.mark.parametrize("signature", [SIGNATURE_LIST, SIGNATURE_UNPACKED])
    def test_udf_returning_wrong_type_on_batch(self, signature):
        class WrongReturnTypeUDFList(UDFList):
            def on_batch(
                self, series: list[td_typing.Series]
            ) -> list[td_typing.Series]:
                # noinspection PyTypeChecker
                return {"wrong": series[0]}

        class WrongReturnTypeUDFUnpacked(UDFUnpacked):
            def on_batch(self, series_0: td_typing.Series) -> list[td_typing.Series]:
                # noinspection PyTypeChecker
                return {"wrong": series_0}

        udf_class = (
            WrongReturnTypeUDFList
            if signature == SIGNATURE_LIST
            else WrongReturnTypeUDFUnpacked
        )

        _, _, tf = load_simple_dataframe()

        expected_exceptions: tuple[type[BaseException], ...] = (
            AttributeError,
            TypeError,
            ValueError,
        )
        with pytest.raises(expected_exceptions):
            result = tf.udf(td.col("intColumn"), udf_class([("result", td.Int64)]))
            result.collect()

    @pytest.mark.parametrize("signature", [SIGNATURE_LIST, SIGNATURE_UNPACKED])
    def test_udf_returning_empty_list_on_batch(self, signature):
        class EmptyReturnUDFList(UDFList):
            def on_batch(
                self, series: list[td_typing.Series]
            ) -> list[td_typing.Series]:
                return []

        class EmptyReturnUDFUnpacked(UDFUnpacked):
            def on_batch(self, *series: td_typing.Series) -> list[td_typing.Series]:
                return []

        udf_class = (
            EmptyReturnUDFList
            if signature == SIGNATURE_LIST
            else EmptyReturnUDFUnpacked
        )

        _, _, tf = load_simple_dataframe()

        result = tf.udf(td.col("intColumn"), udf_class([("result", td.Int64)]))
        with pytest.raises(ValueError, match="produced 0 output columns"):
            result._lf.collect()

    @pytest.mark.parametrize("signature", [SIGNATURE_LIST, SIGNATURE_UNPACKED])
    def test_udf_with_mismatched_lengths_on_batch(self, signature):
        class MismatchedLengthUDFList(UDFList):
            def on_batch(
                self, series: list[td_typing.Series]
            ) -> list[td_typing.Series]:
                return [td_typing.Series([1, 2])]

        class MismatchedLengthUDFUnpacked(UDFUnpacked):
            def on_batch(self, series_0: td_typing.Series) -> list[td_typing.Series]:
                return [td_typing.Series([1, 2])]

        udf_class = (
            MismatchedLengthUDFList
            if signature == SIGNATURE_LIST
            else MismatchedLengthUDFUnpacked
        )

        data = {"a": [1, 2, 3, 4, 5]}
        tf = td.TableFrame(data)

        with pytest.raises(Exception):
            result = tf.udf(td.col("a"), udf_class([("result", td.Int64)]))
            result.collect()

    @pytest.mark.parametrize("signature", [SIGNATURE_LIST, SIGNATURE_UNPACKED])
    def test_udf_raising_exception_on_batch(self, signature):
        class ExceptionUDFList(UDFList):
            def on_batch(
                self, series: list[td_typing.Series]
            ) -> list[td_typing.Series]:
                raise ValueError("Intentional error in on_batch")

        class ExceptionUDFUnpacked(UDFUnpacked):
            def on_batch(self, series_0: td_typing.Series) -> list[td_typing.Series]:
                raise ValueError("Intentional error in on_batch")

        udf_class = (
            ExceptionUDFList if signature == SIGNATURE_LIST else ExceptionUDFUnpacked
        )

        _, _, tf = load_simple_dataframe()

        with pytest.raises(ValueError, match="Intentional error in on_batch"):
            result = tf.udf(td.col("intColumn"), udf_class([("result", td.Int64)]))
            result._lf.collect()

    @pytest.mark.parametrize("signature", [SIGNATURE_LIST, SIGNATURE_UNPACKED])
    def test_udf_raising_exception_on_element(self, signature):
        class ExceptionElementUDFList(UDFList):
            def on_element(self, values: list) -> list:
                raise RuntimeError("Intentional error in on_element")

        class ExceptionElementUDFUnpacked(UDFUnpacked):
            def on_element(self, *values) -> list:
                raise RuntimeError("Intentional error in on_element")

        udf_class = (
            ExceptionElementUDFList
            if signature == SIGNATURE_LIST
            else ExceptionElementUDFUnpacked
        )

        _, _, tf = load_simple_dataframe()

        with pytest.raises(RuntimeError, match="Intentional error"):
            result = tf.udf(td.col("intColumn"), udf_class([("result", td.Int64)]))
            result._lf.collect()


class TestTableFrameUDFChaining:
    @pytest.mark.parametrize("signature", [SIGNATURE_LIST, SIGNATURE_UNPACKED])
    def test_chaining_multiple_udfs_on_batch(self, signature):
        class DoubleUDFList(UDFList):
            def on_batch(
                self, series: list[td_typing.Series]
            ) -> list[td_typing.Series]:
                by_two = cast(td_typing.Series, cast(object, series[0] * 2))
                return [by_two]

        class AddTenUDFList(UDFList):
            def on_batch(
                self, series: list[td_typing.Series]
            ) -> list[td_typing.Series]:
                plus_ten = cast(td_typing.Series, cast(object, series[0] + 10))
                return [plus_ten]

        class DoubleUDFUnpacked(UDFUnpacked):
            def on_batch(self, series_0: td_typing.Series) -> list[td_typing.Series]:
                by_two = cast(td_typing.Series, cast(object, series_0 * 2))
                return [by_two]

        class AddTenUDFUnpacked(UDFUnpacked):
            def on_batch(self, series_0: td_typing.Series) -> list[td_typing.Series]:
                plus_ten = cast(td_typing.Series, cast(object, series_0 + 10))
                return [plus_ten]

        if signature == SIGNATURE_LIST:
            udf_class_1 = DoubleUDFList
            udf_class_2 = AddTenUDFList
        else:
            udf_class_1 = DoubleUDFUnpacked
            udf_class_2 = AddTenUDFUnpacked

        data = {"a": [1, 2, 3]}
        tf = td.TableFrame(data)
        original_df = tf._lf.collect()
        original_rows, original_cols = original_df.shape
        original_columns = set(original_df.columns)

        result = tf.udf(td.col("a"), udf_class_1([("by_two", td.Int64)])).udf(
            td.col("a"), udf_class_2([("plus_ten", td.Int64)])
        )

        assert isinstance(result, td.TableFrame)
        collected = result._lf.collect()
        assert collected.shape == (original_rows, original_cols + 2)
        assert "by_two" in collected.columns
        assert "plus_ten" in collected.columns
        for col in original_columns:
            assert col in collected.columns

    @pytest.mark.parametrize("signature", [SIGNATURE_LIST, SIGNATURE_UNPACKED])
    def test_chaining_multiple_udfs_on_element(self, signature):
        class InternalSquareUDFList(UDFList):
            def on_element(self, values: list) -> list:
                return [values[0] ** 2]

        class SqrtUDFList(UDFList):
            def on_element(self, values: list) -> list:
                return [values[0] ** 0.5]

        class InternalSquareUDFUnpacked(UDFUnpacked):
            def on_element(self, value) -> list:
                return [value**2]

        class SqrtUDFUnpacked(UDFUnpacked):
            def on_element(self, value) -> list:
                return [value**0.5]

        if signature == SIGNATURE_LIST:
            udf_class_1 = InternalSquareUDFList
            udf_class_2 = SqrtUDFList
        else:
            udf_class_1 = InternalSquareUDFUnpacked
            udf_class_2 = SqrtUDFUnpacked

        data = {"a": [4.0, 9.0, 16.0]}
        tf = td.TableFrame(data)
        original_df = tf._lf.collect()
        original_rows, original_cols = original_df.shape
        original_columns = set(original_df.columns)

        result = tf.udf(td.col("a"), udf_class_1([("squared", td.Float64)])).udf(
            td.col("a"), udf_class_2([("sqrt", td.Float64)])
        )

        assert isinstance(result, td.TableFrame)
        collected = result._lf.collect()
        assert collected.shape == (original_rows, original_cols + 2)
        assert "squared" in collected.columns
        assert "sqrt" in collected.columns
        for col in original_columns:
            assert col in collected.columns


class TestTableFrameUDFOutputNames:
    @pytest.mark.parametrize("signature", [SIGNATURE_LIST, SIGNATURE_UNPACKED])
    def test_on_element_with_output_names_single_column(self, signature):
        class InternalSquareUDFList(UDFList):
            def on_element(self, values: list) -> list:
                return [values[0] ** 2]

        class InternalSquareUDFUnpacked(UDFUnpacked):
            def on_element(self, value) -> list:
                return [value**2]

        udf_class = (
            InternalSquareUDFList
            if signature == SIGNATURE_LIST
            else InternalSquareUDFUnpacked
        )

        _, _, tf = load_simple_dataframe()
        original_df = tf._lf.collect()
        original_rows, original_cols = original_df.shape
        original_columns = set(original_df.columns)

        result = tf.udf(td.col("intColumn"), udf_class([("squared", td.Int64)]))

        assert isinstance(result, td.TableFrame)
        collected = result._lf.collect()
        assert collected.shape == (original_rows, original_cols + 1)
        assert "squared" in collected.columns
        assert collected["squared"].to_list() == [1, 4, 9]
        for col in original_columns:
            assert col in collected.columns

    @pytest.mark.parametrize("signature", [SIGNATURE_LIST, SIGNATURE_UNPACKED])
    def test_on_element_with_output_names_multiple_columns(self, signature):
        class MultiStatsUDFList(UDFList):
            def on_element(self, values: list) -> list:
                val = values[0]
                return [val * 2, val * 3]

        class MultiStatsUDFUnpacked(UDFUnpacked):
            def on_element(self, value) -> list:
                return [value * 2, value * 3]

        udf_class = (
            MultiStatsUDFList if signature == SIGNATURE_LIST else MultiStatsUDFUnpacked
        )

        _, _, tf = load_simple_dataframe()
        original_df = tf._lf.collect()
        original_rows, original_cols = original_df.shape
        original_columns = set(original_df.columns)

        result = tf.udf(
            td.col("intColumn"),
            udf_class([("doubled", td.Int64), ("tripled", td.Int64)]),
        )

        assert isinstance(result, td.TableFrame)
        collected = result._lf.collect()
        assert collected.shape == (original_rows, original_cols + 2)
        assert "doubled" in collected.columns
        assert "tripled" in collected.columns
        assert collected["doubled"].to_list() == [2, 4, 6]
        assert collected["tripled"].to_list() == [3, 6, 9]
        for col in original_columns:
            assert col in collected.columns

    @pytest.mark.parametrize("signature", [SIGNATURE_LIST, SIGNATURE_UNPACKED])
    def test_on_batch_with_output_names_override(self, signature):
        class RenameUDFList(UDFList):
            def on_batch(
                self, series: list[td_typing.Series]
            ) -> list[td_typing.Series]:
                doubled = cast(td_typing.Series, cast(object, series[0] * 2))
                return [doubled]

        class RenameUDFUnpacked(UDFUnpacked):
            def on_batch(self, series_0: td_typing.Series) -> list[td_typing.Series]:
                doubled = cast(td_typing.Series, cast(object, series_0 * 2))
                return [doubled]

        udf_class = RenameUDFList if signature == SIGNATURE_LIST else RenameUDFUnpacked

        _, _, tf = load_simple_dataframe()
        original_df = tf._lf.collect()
        original_rows, original_cols = original_df.shape
        original_columns = set(original_df.columns)

        udf = udf_class([("doubled", td.Int64)])
        udf.with_columns([("doubled_new", None)])
        result = tf.udf(td.col("intColumn"), udf)

        assert isinstance(result, td.TableFrame)
        collected = result._lf.collect()
        assert collected.shape == (original_rows, original_cols + 1)
        assert "doubled" not in collected.columns
        assert "doubled_new" in collected.columns
        assert collected["doubled_new"].to_list() == [2, 4, 6]
        for col in original_columns:
            assert col in collected.columns

    @pytest.mark.parametrize("signature", [SIGNATURE_LIST, SIGNATURE_UNPACKED])
    def test_on_batch_output_name_conflict(self, signature):
        class ConflictUDFList(UDFList):
            def on_batch(
                self, series: list[td_typing.Series]
            ) -> list[td_typing.Series]:
                return [series[0]]

        class ConflictUDFUnpacked(UDFUnpacked):
            def on_batch(self, series_0: td_typing.Series) -> list[td_typing.Series]:
                return [series_0]

        udf_class = (
            ConflictUDFList if signature == SIGNATURE_LIST else ConflictUDFUnpacked
        )

        _, _, tf = load_simple_dataframe()

        with pytest.raises(DuplicateError):
            result = tf.udf(td.col("intColumn"), udf_class([("intColumn", td.Int64)]))
            collected = result._lf.collect()
            assert collected["intColumn"].to_list() == [1, 2, 3]

    @pytest.mark.parametrize("signature", [SIGNATURE_LIST, SIGNATURE_UNPACKED])
    def test_on_element_missing_super_init_raises_at_runtime(self, signature):
        class MissingInitUDFList(UDFList):
            # noinspection PyMissingConstructor
            def __init__(self):
                pass

            def on_element(self, values: list) -> list:
                return [values[0] * 2]

        class MissingInitUDFUnpacked(UDFUnpacked):
            # noinspection PyMissingConstructor
            def __init__(self):
                pass

            def on_element(self, value) -> list:
                return [value * 2]

        udf_class = (
            MissingInitUDFList
            if signature == SIGNATURE_LIST
            else MissingInitUDFUnpacked
        )

        _, _, tf = load_simple_dataframe()

        with pytest.raises(RuntimeError, match="did not call super"):
            result = tf.udf(td.col("intColumn"), udf_class())
            result.collect()

    @pytest.mark.parametrize("signature", [SIGNATURE_LIST, SIGNATURE_UNPACKED])
    def test_output_names_count_mismatch_too_many_on_batch(self, signature):
        class TooManyNamesUDFList(UDFList):
            def on_batch(
                self, series: list[td_typing.Series]
            ) -> list[td_typing.Series]:
                doubled = cast(td_typing.Series, cast(object, series[0] * 2))
                return [doubled]

        class TooManyNamesUDFUnpacked(UDFUnpacked):
            def on_batch(self, series_0: td_typing.Series) -> list[td_typing.Series]:
                doubled = cast(td_typing.Series, cast(object, series_0 * 2))
                return [doubled]

        udf_class = (
            TooManyNamesUDFList
            if signature == SIGNATURE_LIST
            else TooManyNamesUDFUnpacked
        )

        _, _, tf = load_simple_dataframe()

        with pytest.raises(ValueError, match="produced 1 output columns"):
            result = tf.udf(
                td.col("intColumn"),
                udf_class(
                    [("first", td.Int64), ("second", td.Int64), ("third", td.Int64)]
                ),
            )
            result._lf.collect()

    @pytest.mark.parametrize("signature", [SIGNATURE_LIST, SIGNATURE_UNPACKED])
    def test_output_names_count_mismatch_too_few_on_batch(self, signature):
        class TooFewNamesUDFList(UDFList):
            def on_batch(
                self, series: list[td_typing.Series]
            ) -> list[td_typing.Series]:
                doubled = cast(td_typing.Series, cast(object, series[0] * 2))
                tripled = cast(td_typing.Series, cast(object, series[0] * 3))
                return [doubled, tripled]

        class TooFewNamesUDFUnpacked(UDFUnpacked):
            def on_batch(self, series_0: td_typing.Series) -> list[td_typing.Series]:
                doubled = cast(td_typing.Series, cast(object, series_0 * 2))
                tripled = cast(td_typing.Series, cast(object, series_0 * 3))
                return [doubled, tripled]

        udf_class = (
            TooFewNamesUDFList
            if signature == SIGNATURE_LIST
            else TooFewNamesUDFUnpacked
        )

        _, _, tf = load_simple_dataframe()

        with pytest.raises(ValueError, match="produced 2 output columns"):
            result = tf.udf(td.col("intColumn"), udf_class([("doubled", td.Int64)]))
            result._lf.collect()

    @pytest.mark.parametrize("signature", [SIGNATURE_LIST, SIGNATURE_UNPACKED])
    def test_output_names_count_mismatch_too_few_on_element(self, signature):
        class TooFewNamesElementUDFList(UDFList):
            def on_element(self, values: list) -> list:
                return [values[0] * 2, values[0] * 3]

        class TooFewNamesElementUDFUnpacked(UDFUnpacked):
            def on_element(self, value) -> list:
                return [value * 2, value * 3]

        udf_class = (
            TooFewNamesElementUDFList
            if signature == SIGNATURE_LIST
            else TooFewNamesElementUDFUnpacked
        )

        _, _, tf = load_simple_dataframe()

        with pytest.raises(ValueError, match="produced 2 output columns"):
            result = tf.udf(td.col("intColumn"), udf_class([("doubled", td.Int64)]))
            result._lf.collect()

    @pytest.mark.parametrize("signature", [SIGNATURE_LIST, SIGNATURE_UNPACKED])
    def test_duplicate_output_names_in_schema(self, signature):
        class DuplicateNamesUDFList(UDFList):
            def on_batch(
                self, series: list[td_typing.Series]
            ) -> list[td_typing.Series]:
                doubled = cast(td_typing.Series, cast(object, series[0] * 2))
                tripled = cast(td_typing.Series, cast(object, series[0] * 3))
                return [doubled, tripled]

        class DuplicateNamesUDFUnpacked(UDFUnpacked):
            def on_batch(self, series_0: td_typing.Series) -> list[td_typing.Series]:
                doubled = cast(td_typing.Series, cast(object, series_0 * 2))
                tripled = cast(td_typing.Series, cast(object, series_0 * 3))
                return [doubled, tripled]

        udf_class = (
            DuplicateNamesUDFList
            if signature == SIGNATURE_LIST
            else DuplicateNamesUDFUnpacked
        )

        _, _, tf = load_simple_dataframe()

        with pytest.raises(ValueError, match="duplicate column names"):
            result = tf.udf(
                td.col("intColumn"),
                udf_class([("result", td.Int64), ("result", td.Int64)]),
            )
            result.collect()

    @pytest.mark.parametrize("signature", [SIGNATURE_LIST, SIGNATURE_UNPACKED])
    def test_none_output_names_for_on_batch_with_alias(self, signature):
        class EmptyNamesWithAliasUDFList(UDFList):
            def on_batch(
                self, series: list[td_typing.Series]
            ) -> list[td_typing.Series]:
                return [series[0]]

        class EmptyNamesWithAliasUDFUnpacked(UDFUnpacked):
            def on_batch(self, series_0: td_typing.Series) -> list[td_typing.Series]:
                return [series_0]

        udf_class = (
            EmptyNamesWithAliasUDFList
            if signature == SIGNATURE_LIST
            else EmptyNamesWithAliasUDFUnpacked
        )

        _, _, tf = load_simple_dataframe()
        original_df = tf._lf.collect()
        original_rows, original_cols = original_df.shape
        original_columns = set(original_df.columns)
        result = tf.udf(
            td.col("intColumn"),
            udf_class(("proper_name", td.Int64)).with_columns((None, None)),
        )
        assert isinstance(result, td.TableFrame)
        collected = result._lf.collect()
        assert collected.shape == (original_rows, original_cols + 1)
        assert "proper_name" in collected.columns
        for col in original_columns:
            assert col in collected.columns

    @pytest.mark.parametrize("signature", [SIGNATURE_LIST, SIGNATURE_UNPACKED])
    def test_output_names_with_special_characters(self, signature):
        class SpecialNamesUDFList(UDFList):
            def on_element(self, values: list) -> list:
                return [values[0] * 2]

        class SpecialNamesUDFUnpacked(UDFUnpacked):
            def on_element(self, value) -> list:
                return [value * 2]

        udf_class = (
            SpecialNamesUDFList
            if signature == SIGNATURE_LIST
            else SpecialNamesUDFUnpacked
        )

        _, _, tf = load_simple_dataframe()
        original_df = tf._lf.collect()
        original_rows, original_cols = original_df.shape
        original_columns = set(original_df.columns)

        result = tf.udf(
            td.col("intColumn"), udf_class([("σpecïalᚣ🧪列👀💩", td.Int64)])
        )

        assert isinstance(result, td.TableFrame)
        collected = result._lf.collect()
        assert collected.shape == (original_rows, original_cols + 1)
        assert "σpecïalᚣ🧪列👀💩" in collected.columns
        assert collected["σpecïalᚣ🧪列👀💩"].to_list() == [2, 4, 6]
        for col in original_columns:
            assert col in collected.columns

    @pytest.mark.parametrize("signature", [SIGNATURE_LIST, SIGNATURE_UNPACKED])
    def test_output_names_multiple_inputs_multiple_outputs(self, signature):
        class MultiInMultiOutUDFList(UDFList):
            def on_element(self, values: list) -> list:
                return [
                    values[0] + values[1],
                    values[0] * values[1],
                    values[0] - values[1],
                ]

        class MultiInMultiOutUDFUnpacked(UDFUnpacked):
            def on_element(self, value_0, value_1) -> list:
                return [
                    value_0 + value_1,
                    value_0 * value_1,
                    value_0 - value_1,
                ]

        udf_class = (
            MultiInMultiOutUDFList
            if signature == SIGNATURE_LIST
            else MultiInMultiOutUDFUnpacked
        )

        data = {"a": [1, 2, 3], "b": [4, 5, 6]}
        tf = td.TableFrame(data)
        original_df = tf._lf.collect()
        original_rows, original_cols = original_df.shape
        original_columns = set(original_df.columns)

        result = tf.udf(
            td.col("a", "b"),
            udf_class(
                [
                    ("sum", td.Int64),
                    ("product", td.Int64),
                    ("difference", td.Int64),
                ]
            ),
        )

        assert isinstance(result, td.TableFrame)
        collected = result._lf.collect()
        assert collected.shape == (original_rows, original_cols + 3)
        assert "sum" in collected.columns
        assert "product" in collected.columns
        assert "difference" in collected.columns
        assert collected["sum"].to_list() == [5, 7, 9]
        assert collected["product"].to_list() == [4, 10, 18]
        assert collected["difference"].to_list() == [-3, -3, -3]
        for col in original_columns:
            assert col in collected.columns


class TestTableFrameUDFPipelines:
    @pytest.mark.parametrize("signature", [SIGNATURE_LIST, SIGNATURE_UNPACKED])
    def test_concat_all_on_element_and_on_batch_combinations(self, signature):
        class BatchScalerUDFList(UDFList):
            def __init__(self, the_factor: int, output_columns):
                super().__init__(output_columns)
                self._factor = the_factor

            def on_batch(
                self, series: list[td_typing.Series]
            ) -> list[td_typing.Series]:
                scaled = cast(
                    td_typing.Series,
                    cast(object, series[0] * self._factor),
                )
                return [scaled]

        class ElementScalerUDFList(UDFList):
            def __init__(self, the_factor: int, output_columns):
                super().__init__(output_columns)
                self._factor = the_factor

            def on_element(self, values: list) -> list:
                return [values[0] * self._factor]

        class BatchScalerUDFUnpacked(UDFUnpacked):
            def __init__(self, the_factor: int, output_columns):
                super().__init__(output_columns)
                self._factor = the_factor

            def on_batch(self, series_0: td_typing.Series) -> list[td_typing.Series]:
                scaled = cast(
                    td_typing.Series,
                    cast(object, series_0 * self._factor),
                )
                return [scaled]

        class ElementScalerUDFUnpacked(UDFUnpacked):
            def __init__(self, the_factor: int, output_columns):
                super().__init__(output_columns)
                self._factor = the_factor

            def on_element(self, value) -> list:
                return [value * self._factor]

        if signature == SIGNATURE_LIST:
            batch_scaler_udf = BatchScalerUDFList
            element_scaler_udf = ElementScalerUDFList
        else:
            batch_scaler_udf = BatchScalerUDFUnpacked
            element_scaler_udf = ElementScalerUDFUnpacked

        _, _, tf = load_simple_dataframe()
        original_df = tf._lf.collect()
        original_rows, original_cols = original_df.shape
        original_columns = list(original_df.columns)

        base_values = original_df["intColumn"].to_list()

        combinations = list(product(("batch", "element"), repeat=4))
        for combination in combinations:
            pipeline = tf
            input_column = "intColumn"
            expected_columns: list[tuple[str, list[int | float]]] = []
            for iteration, mode in enumerate(combination):
                factor = (iteration + 1) * (2 if mode == "element" else 1)
                output_column = f"column_{iteration}"
                if mode == "batch":
                    udf_instance = batch_scaler_udf(factor, [(output_column, td.Int64)])
                else:
                    udf_instance = element_scaler_udf(
                        factor, [(output_column, td.Int64)]
                    )
                pipeline = pipeline.udf(td.col(input_column), udf_instance)
                if iteration == 0:
                    expected_values = [value * factor for value in base_values]
                else:
                    expected_values = [
                        value * factor for value in expected_columns[-1][1]
                    ]
                expected_columns.append((output_column, expected_values))
                input_column = output_column

            assert isinstance(pipeline, td.TableFrame)
            collected = pipeline._lf.collect()
            assert collected.shape == (original_rows, original_cols + len(combination))
            for column in original_columns:
                assert column in collected.columns
            for column_name, expected_values in expected_columns:
                assert column_name in collected.columns
                assert collected[column_name].to_list() == pytest.approx(
                    expected_values
                )

    @pytest.mark.parametrize("signature", [SIGNATURE_LIST, SIGNATURE_UNPACKED])
    def test_udf_alternates_with_polars_transformations(self, signature):
        class BatchAddOneUDFList(UDFList):
            def on_batch(
                self, series: list[td_typing.Series]
            ) -> list[td_typing.Series]:
                incremented = cast(
                    td_typing.Series,
                    cast(object, series[0] + 1),
                )
                return [incremented]

        class ElementSquareUDFList(UDFList):
            def on_element(self, values: list) -> list:
                return [values[0] * values[0]]

        class BatchAbsoluteUDFList(UDFList):
            def on_batch(
                self, series: list[td_typing.Series]
            ) -> list[td_typing.Series]:
                absolute = cast(
                    td_typing.Series,
                    cast(object, series[0].abs()),
                )
                return [absolute]

        class BatchAddOneUDFUnpacked(UDFUnpacked):
            def on_batch(self, series_0: td_typing.Series) -> list[td_typing.Series]:
                incremented = cast(
                    td_typing.Series,
                    cast(object, series_0 + 1),
                )
                return [incremented]

        class ElementSquareUDFUnpacked(UDFUnpacked):
            def on_element(self, value) -> list:
                return [value * value]

        class BatchAbsoluteUDFUnpacked(UDFUnpacked):
            def on_batch(self, series_0: td_typing.Series) -> list[td_typing.Series]:
                absolute = cast(
                    td_typing.Series,
                    cast(object, series_0.abs()),
                )
                return [absolute]

        if signature == SIGNATURE_LIST:
            batch_add_one_udf = BatchAddOneUDFList
            element_square_udf = ElementSquareUDFList
            batch_absolute_udf = BatchAbsoluteUDFList
        else:
            batch_add_one_udf = BatchAddOneUDFUnpacked
            element_square_udf = ElementSquareUDFUnpacked
            batch_absolute_udf = BatchAbsoluteUDFUnpacked

        _, _, tf = load_simple_dataframe()

        pipeline = tf.udf(
            td.col("intColumn"), batch_add_one_udf([("batch_added", td.Int64)])
        )
        pipeline = pipeline.with_columns(
            td.col("batch_added").mul(2).alias("batch_doubled"),
        )
        pipeline = pipeline.udf(
            td.col("batch_doubled"), element_square_udf([("element_squared", td.Int64)])
        )
        pipeline = pipeline.with_columns(
            (td.col("element_squared") - td.col("batch_doubled")).alias("difference"),
        )
        pipeline = pipeline.udf(
            td.col("difference"), batch_absolute_udf([("difference_abs", td.Int64)])
        )

        collected = pipeline._lf.collect()
        assert collected["batch_added"].to_list() == [2, 3, 4]
        assert collected["batch_doubled"].to_list() == [4, 6, 8]
        assert collected["element_squared"].to_list() == [16, 36, 64]
        assert collected["difference"].to_list() == [12, 30, 56]
        assert collected["difference_abs"].to_list() == [12, 30, 56]
