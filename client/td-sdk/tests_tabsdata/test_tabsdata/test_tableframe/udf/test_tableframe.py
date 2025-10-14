#
# Copyright 2025 Tabs Data Inc.
#

from itertools import product
from typing import cast

import pytest
from polars.exceptions import DuplicateError

import tabsdata as td
import tabsdata.tableframe.typing as td_typing
from tabsdata.tableframe.udf.function import UDF
from tests_tabsdata.test_tabsdata.test_tableframe.common import (
    load_normalized_complex_dataframe,
    load_simple_dataframe,
    pretty_polars,
)

pretty_polars()


class TestTableFrameUDFSingleColumn:
    def test_single_column_on_batch_simple(self):
        class SquareUDF(UDF):
            def on_batch(
                self, series: list[td_typing.Series]
            ) -> list[td_typing.Series]:
                squared = series[0] * series[0]
                return [squared]

        _, _, tf = load_simple_dataframe()
        original_df = tf._lf.collect()
        original_rows, original_cols = original_df.shape

        result = tf.udf(td.col("intColumn"), SquareUDF([("squared", td.Int64)]))

        assert isinstance(result, td.TableFrame)
        collected = result._lf.collect()
        assert collected.shape == (original_rows, original_cols + 1)
        assert "squared" in collected.columns
        assert collected["squared"].to_list() == [1, 4, 9]
        assert "intColumn" in collected.columns
        assert "stringColumn" in collected.columns

    def test_single_column_on_element_simple(self):
        class SquareUDF(UDF):
            def on_element(self, values: list) -> list:
                return [values[0] * values[0]]

        _, _, tf = load_simple_dataframe()
        original_df = tf._lf.collect()
        original_rows, original_cols = original_df.shape

        result = tf.udf(td.col("intColumn"), SquareUDF([("squared", td.Int64)]))

        assert isinstance(result, td.TableFrame)
        collected = result._lf.collect()
        assert collected.shape == (original_rows, original_cols + 1)
        assert "squared" in collected.columns
        assert collected["squared"].to_list() == [1, 4, 9]
        assert "intColumn" in collected.columns
        assert "stringColumn" in collected.columns

    def test_single_column_on_batch_complex(self):
        class DoubleValueUDF(UDF):
            def on_batch(
                self, series: list[td_typing.Series]
            ) -> list[td_typing.Series]:
                doubled = cast(td_typing.Series, cast(object, series[0] * 2))
                return [doubled]

        _, _, tf = load_normalized_complex_dataframe()
        original_df = tf._lf.collect()
        original_rows, original_cols = original_df.shape
        original_columns = set(original_df.columns)

        result = tf.udf(
            td.col("bill_length_mm"), DoubleValueUDF([("doubledValue", td.Float64)])
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

    def test_single_column_on_element_complex(self):
        class BillRatioUDF(UDF):
            def on_element(self, values: list) -> list:
                if values[0] is None:
                    return [None]
                return [values[0] * 2]

        _, _, tf = load_normalized_complex_dataframe()
        original_df = tf._lf.collect()
        original_rows, original_cols = original_df.shape
        original_columns = set(original_df.columns)

        result = tf.udf(
            td.col("bill_length_mm"), BillRatioUDF([("doubledValue", td.Float64)])
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
    def test_multiple_columns_on_batch_sum(self):
        class SumColumnsUDF(UDF):
            def on_batch(
                self, series: list[td_typing.Series]
            ) -> list[td_typing.Series]:
                f_result = series[0]
                for s in series[1:]:
                    f_result = f_result + s
                return [f_result]

        data = {"a": [1, 2, 3], "b": [10, 20, 30], "c": [100, 200, 300]}
        tf = td.TableFrame(data)
        original_df = tf._lf.collect()
        original_rows, original_cols = original_df.shape
        original_columns = set(original_df.columns)

        result = tf.udf(td.col("a", "b", "c"), SumColumnsUDF([("sum", td.Int64)]))

        assert isinstance(result, td.TableFrame)
        collected = result._lf.collect()
        assert collected.shape == (original_rows, original_cols + 1)
        assert "sum" in collected.columns
        assert collected["sum"].to_list() == [111, 222, 333]
        for col in original_columns:
            assert col in collected.columns

    def test_multiple_columns_on_element_sum(self):
        class SumElementUDF(UDF):
            def on_element(self, values: list) -> list:
                return [sum(values)]

        data = {"a": [1, 2, 3], "b": [10, 20, 30], "c": [100, 200, 300]}
        tf = td.TableFrame(data)
        original_df = tf._lf.collect()
        original_rows, original_cols = original_df.shape
        original_columns = set(original_df.columns)

        result = tf.udf(td.col("a", "b", "c"), SumElementUDF([("sum", td.Int64)]))

        assert isinstance(result, td.TableFrame)
        collected = result._lf.collect()
        assert collected.shape == (original_rows, original_cols + 1)
        assert "sum" in collected.columns
        assert collected["sum"].to_list() == [111, 222, 333]
        for col in original_columns:
            assert col in collected.columns

    def test_multiple_columns_on_batch_complex_dataset(self):
        class BillAreaUDF(UDF):
            def on_batch(
                self, series: list[td_typing.Series]
            ) -> list[td_typing.Series]:
                bill_area = series[0] * series[1]
                return [bill_area]

        _, _, tf = load_normalized_complex_dataframe()
        original_df = tf._lf.collect()
        original_rows, original_cols = original_df.shape
        original_columns = set(original_df.columns)

        result = tf.udf(
            td.col("bill_length_mm", "bill_depth_mm"),
            BillAreaUDF([("bill_area_mm2", td.Float64)]),
        )

        assert isinstance(result, td.TableFrame)
        collected = result._lf.collect()
        assert collected.shape == (original_rows, original_cols + 1)
        assert "bill_area_mm2" in collected.columns
        for col in original_columns:
            assert col in collected.columns

    def test_multiple_columns_on_element_complex_dataset(self):
        class BillRatioUDF(UDF):
            def on_element(self, values: list) -> list:
                if values[0] is None or values[1] is None or values[1] == 0:
                    return [None]
                return [values[0] / values[1]]

        _, _, tf = load_normalized_complex_dataframe()
        original_df = tf._lf.collect()
        original_rows, original_cols = original_df.shape
        original_columns = set(original_df.columns)

        result = tf.udf(
            td.col("bill_length_mm", "bill_depth_mm"),
            BillRatioUDF([("bill_ratio", td.Float64)]),
        )

        assert isinstance(result, td.TableFrame)
        collected = result._lf.collect()
        assert collected.shape == (original_rows, original_cols + 1)
        assert "bill_ratio" in collected.columns
        for col in original_columns:
            assert col in collected.columns


class TestTableFrameUDFMultipleOutputs:
    def test_multiple_outputs_on_batch(self):
        class StatsUDF(UDF):
            def on_batch(
                self, series: list[td_typing.Series]
            ) -> list[td_typing.Series]:
                doubled = cast(td_typing.Series, cast(object, series[0] * 2))
                tripled = cast(td_typing.Series, cast(object, series[0] * 3))
                return [doubled, tripled]

        _, _, tf = load_simple_dataframe()
        original_df = tf._lf.collect()
        original_rows, original_cols = original_df.shape
        original_columns = set(original_df.columns)

        result = tf.udf(
            td.col("intColumn"),
            StatsUDF([("doubled", td.Int64), ("tripled", td.Int64)]),
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

    def test_multiple_outputs_on_element(self):
        class MultiStatsUDF(UDF):
            def on_element(self, values: list) -> list:
                value = values[0]
                return [value * 2, value * 3, value * 4]

        _, _, tf = load_simple_dataframe()
        original_df = tf._lf.collect()
        original_rows, original_cols = original_df.shape
        original_columns = set(original_df.columns)

        result = tf.udf(
            td.col("intColumn"),
            MultiStatsUDF(
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

    def test_multiple_outputs_from_multiple_inputs_on_batch(self):
        class SumAndProductUDF(UDF):
            def on_batch(
                self, series: list[td_typing.Series]
            ) -> list[td_typing.Series]:
                sum_result = series[0] + series[1]
                product_result = series[0] * series[1]
                return [sum_result, product_result]

        data = {"a": [1, 2, 3], "b": [4, 5, 6]}
        tf = td.TableFrame(data)
        original_df = tf._lf.collect()
        original_rows, original_cols = original_df.shape
        original_columns = set(original_df.columns)

        result = tf.udf(
            td.col("a", "b"),
            SumAndProductUDF([("sum", td.Int64), ("product", td.Int64)]),
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

    def test_multiple_outputs_from_multiple_inputs_on_element(self):
        class SumAndProductUDF(UDF):
            def on_element(self, values: list) -> list:
                return [values[0] + values[1], values[0] * values[1]]

        data = {"a": [1, 2, 3], "b": [4, 5, 6]}
        tf = td.TableFrame(data)
        original_df = tf._lf.collect()
        original_rows, original_cols = original_df.shape
        original_columns = set(original_df.columns)

        result = tf.udf(
            td.col("a", "b"),
            SumAndProductUDF([("sum", td.Int64), ("product", td.Int64)]),
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

    def test_output_dtype_spec(self):
        class StatsUDF(UDF):
            def on_batch(
                self, series: list[td_typing.Series]
            ) -> list[td_typing.Series]:
                doubled = cast(td_typing.Series, cast(object, series[0] * 2))
                tripled = cast(td_typing.Series, cast(object, series[0] * 3))
                quadrupled = cast(td_typing.Series, cast(object, series[0] * 4))
                return [doubled, tripled, quadrupled]

        _, _, tf = load_simple_dataframe()
        original_df = tf._lf.collect()
        original_rows, original_cols = original_df.shape
        original_columns = set(original_df.columns)

        result = tf.udf(
            td.col("intColumn"),
            StatsUDF(
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
    def test_stateful_udf_with_fixed_attribute_on_batch(self):
        class MultiplierUDF(UDF):
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

        _, _, tf = load_simple_dataframe()
        original_df = tf._lf.collect()
        original_rows, original_cols = original_df.shape
        original_columns = set(original_df.columns)

        udf_instance = MultiplierUDF(5.0, [("multiplied", td.Float64)])
        result = tf.udf(td.col("intColumn"), udf_instance)

        assert isinstance(result, td.TableFrame)
        collected = result._lf.collect()
        assert collected.shape == (original_rows, original_cols + 1)
        assert "multiplied" in collected.columns
        assert collected["multiplied"].to_list() == [5.0, 10.0, 15.0]
        for col in original_columns:
            assert col in collected.columns

    def test_stateful_udf_with_fixed_attribute_on_element(self):
        class AddConstantUDF(UDF):
            def __init__(self, constant: int, output_columns):
                super().__init__(output_columns)
                self.constant = constant

            def on_element(self, values: list) -> list:
                return [values[0] + self.constant]

        _, _, tf = load_simple_dataframe()
        original_df = tf._lf.collect()
        original_rows, original_cols = original_df.shape
        original_columns = set(original_df.columns)

        udf_instance = AddConstantUDF(100, [("with_constant", td.Int64)])
        result = tf.udf(td.col("intColumn"), udf_instance)

        assert isinstance(result, td.TableFrame)
        collected = result._lf.collect()
        assert collected.shape == (original_rows, original_cols + 1)
        assert "with_constant" in collected.columns
        assert collected["with_constant"].to_list() == [101, 102, 103]
        for col in original_columns:
            assert col in collected.columns

    def test_stateful_udf_with_counter_on_batch(self):
        class BatchCounterUDF(UDF):
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

        _, _, tf = load_simple_dataframe()
        original_df = tf._lf.collect()
        original_rows, original_cols = original_df.shape
        original_columns = set(original_df.columns)

        udf = BatchCounterUDF([("with_batch_number", td.Int64)])
        result = tf.udf(td.col("intColumn"), udf)

        assert isinstance(result, td.TableFrame)
        collected = result._lf.collect()
        assert collected.shape == (original_rows, original_cols + 1)
        assert "with_batch_number" in collected.columns
        for col in original_columns:
            assert col in collected.columns
        assert udf.batch_count > 0
        assert udf.total_rows_processed == 3

    def test_stateful_udf_with_accumulator_on_element(self):
        class AccumulatorUDF(UDF):
            def __init__(self, output_columns):
                super().__init__(output_columns)
                self.running_sum = 0

            def on_element(self, values: list) -> list:
                self.running_sum += values[0]
                return [self.running_sum]

        data = {"a": [1, 2, 3, 4, 5]}
        tf = td.TableFrame(data)
        original_df = tf._lf.collect()
        original_rows, original_cols = original_df.shape
        original_columns = set(original_df.columns)

        udf = AccumulatorUDF([("cumulative_sum", td.Int64)])
        result = tf.udf(td.col("a"), udf)

        assert isinstance(result, td.TableFrame)
        collected = result._lf.collect()
        assert collected.shape == (original_rows, original_cols + 1)
        assert "cumulative_sum" in collected.columns
        for col in original_columns:
            assert col in collected.columns
        assert udf.running_sum > 0


class TestTableFrameUDFLargeDatasets:
    def test_large_dataset_multiple_batches_on_batch(self):
        class BatchTrackingUDF(UDF):
            def __init__(self, output_columns):
                super().__init__(output_columns)
                self.batch_sizes = []

            def on_batch(
                self, series: list[td_typing.Series]
            ) -> list[td_typing.Series]:
                self.batch_sizes.append(len(series[0]))
                f_result = cast(td_typing.Series, cast(object, series[0] * 2))
                return [f_result]

        n_rows = 1_000_000
        data = {"values": list(range(n_rows))}
        tf = td.TableFrame(data)
        original_df = tf._lf.collect()
        original_rows, original_cols = original_df.shape
        original_columns = set(original_df.columns)

        udf = BatchTrackingUDF([("doubled", td.Int64)])
        result = tf.udf(td.col("values"), udf)

        assert isinstance(result, td.TableFrame)
        collected = result._lf.collect()
        assert collected.shape == (original_rows, original_cols + 1)
        assert "doubled" in collected.columns
        for col in original_columns:
            assert col in collected.columns
        total_processed = sum(udf.batch_sizes)
        assert total_processed == n_rows

    def test_large_dataset_multiple_batches_on_element(self):
        class ElementCounterUDF(UDF):
            def __init__(self, output_columns):
                super().__init__(output_columns)
                self.element_count = 0

            def on_element(self, values: list) -> list:
                self.element_count += 1
                return [values[0] + 1]

        n_rows = 1_000_000
        data = {"values": list(range(n_rows))}
        tf = td.TableFrame(data)
        original_df = tf._lf.collect()
        original_rows, original_cols = original_df.shape
        original_columns = set(original_df.columns)

        udf = ElementCounterUDF([("incremented", td.Int64)])
        result = tf.udf(td.col("values"), udf)

        assert isinstance(result, td.TableFrame)
        collected = result._lf.collect()
        assert collected.shape == (original_rows, original_cols + 1)
        assert "incremented" in collected.columns
        for col in original_columns:
            assert col in collected.columns
        assert udf.element_count == n_rows

    def test_complex_dataset_with_nulls_on_batch(self):
        class NullHandlingUDF(UDF):
            def on_batch(
                self, series: list[td_typing.Series]
            ) -> list[td_typing.Series]:
                filled = series[0].fill_null(-1)
                f_result = cast(td_typing.Series, cast(object, filled * 2))
                return [f_result]

        _, _, tf = load_normalized_complex_dataframe()
        original_df = tf._lf.collect()
        original_rows, original_cols = original_df.shape
        original_columns = set(original_df.columns)

        result = tf.udf(
            td.col("body_mass_g"), NullHandlingUDF([("handled", td.Float64)])
        )

        assert isinstance(result, td.TableFrame)
        collected = result._lf.collect()
        assert collected.shape == (original_rows, original_cols + 1)
        assert "handled" in collected.columns
        for col in original_columns:
            assert col in collected.columns

    def test_complex_dataset_with_nulls_on_element(self):
        class NullHandlingElementUDF(UDF):
            def on_element(self, values: list) -> list:
                val = values[0] if values[0] is not None else -1
                return [val * 2]

        _, _, tf = load_normalized_complex_dataframe()
        original_df = tf._lf.collect()
        original_rows, original_cols = original_df.shape
        original_columns = set(original_df.columns)

        result = tf.udf(
            td.col("body_mass_g"), NullHandlingElementUDF([("handled", td.Float64)])
        )

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

    def test_udf_with_invalid_expression(self):
        class SimpleUDF(UDF):
            def on_batch(
                self, series: list[td_typing.Series]
            ) -> list[td_typing.Series]:
                return series

        _, _, tf = load_simple_dataframe()

        with pytest.raises(Exception):
            result = tf.udf(
                td.col("nonexistent_column"), SimpleUDF([("result", td.Int64)])
            )
            result.collect()

    def test_udf_returning_wrong_type_on_batch(self):
        class WrongReturnTypeUDF(UDF):
            def on_batch(
                self, series: list[td_typing.Series]
            ) -> list[td_typing.Series]:
                # noinspection PyTypeChecker
                return {"wrong": series[0]}

        _, _, tf = load_simple_dataframe()

        expected_exceptions: tuple[type[BaseException], ...] = (
            AttributeError,
            TypeError,
            ValueError,
        )
        with pytest.raises(expected_exceptions):
            result = tf.udf(
                td.col("intColumn"), WrongReturnTypeUDF([("result", td.Int64)])
            )
            result.collect()

    def test_udf_returning_empty_list_on_batch(self):
        class EmptyReturnUDF(UDF):
            def on_batch(
                self, series: list[td_typing.Series]
            ) -> list[td_typing.Series]:
                return []

        _, _, tf = load_simple_dataframe()

        result = tf.udf(td.col("intColumn"), EmptyReturnUDF([("result", td.Int64)]))
        with pytest.raises(ValueError, match="produced 0 output columns"):
            result._lf.collect()

    def test_udf_with_mismatched_lengths_on_batch(self):
        class MismatchedLengthUDF(UDF):
            def on_batch(
                self, series: list[td_typing.Series]
            ) -> list[td_typing.Series]:
                return [td_typing.Series([1, 2])]

        data = {"a": [1, 2, 3, 4, 5]}
        tf = td.TableFrame(data)

        with pytest.raises(Exception):
            result = tf.udf(td.col("a"), MismatchedLengthUDF([("result", td.Int64)]))
            result.collect()

    def test_udf_raising_exception_on_batch(self):
        class ExceptionUDF(UDF):
            def on_batch(
                self, series: list[td_typing.Series]
            ) -> list[td_typing.Series]:
                raise ValueError("Intentional error in on_batch")

        _, _, tf = load_simple_dataframe()

        with pytest.raises(ValueError, match="Intentional error in on_batch"):
            result = tf.udf(td.col("intColumn"), ExceptionUDF([("result", td.Int64)]))
            result._lf.collect()

    def test_udf_raising_exception_on_element(self):
        class ExceptionElementUDF(UDF):
            def on_element(self, values: list) -> list:
                raise RuntimeError("Intentional error in on_element")

        _, _, tf = load_simple_dataframe()

        with pytest.raises(RuntimeError, match="Intentional error"):
            result = tf.udf(
                td.col("intColumn"), ExceptionElementUDF([("result", td.Int64)])
            )
            result._lf.collect()


class TestTableFrameUDFChaining:
    def test_chaining_multiple_udfs_on_batch(self):
        class DoubleUDF(UDF):
            def on_batch(
                self, series: list[td_typing.Series]
            ) -> list[td_typing.Series]:
                by_two = cast(td_typing.Series, cast(object, series[0] * 2))
                return [by_two]

        class AddTenUDF(UDF):
            def on_batch(
                self, series: list[td_typing.Series]
            ) -> list[td_typing.Series]:
                plus_ten = cast(td_typing.Series, cast(object, series[0] + 10))
                return [plus_ten]

        data = {"a": [1, 2, 3]}
        tf = td.TableFrame(data)
        original_df = tf._lf.collect()
        original_rows, original_cols = original_df.shape
        original_columns = set(original_df.columns)

        result = tf.udf(td.col("a"), DoubleUDF([("by_two", td.Int64)])).udf(
            td.col("a"), AddTenUDF([("plus_ten", td.Int64)])
        )

        assert isinstance(result, td.TableFrame)
        collected = result._lf.collect()
        assert collected.shape == (original_rows, original_cols + 2)
        assert "by_two" in collected.columns
        assert "plus_ten" in collected.columns
        for col in original_columns:
            assert col in collected.columns

    def test_chaining_multiple_udfs_on_element(self):
        class SquareUDF(UDF):
            def on_element(self, values: list) -> list:
                return [values[0] ** 2]

        class SqrtUDF(UDF):
            def on_element(self, values: list) -> list:
                return [values[0] ** 0.5]

        data = {"a": [4.0, 9.0, 16.0]}
        tf = td.TableFrame(data)
        original_df = tf._lf.collect()
        original_rows, original_cols = original_df.shape
        original_columns = set(original_df.columns)

        result = tf.udf(td.col("a"), SquareUDF([("squared", td.Float64)])).udf(
            td.col("a"), SqrtUDF([("sqrt", td.Float64)])
        )

        assert isinstance(result, td.TableFrame)
        collected = result._lf.collect()
        assert collected.shape == (original_rows, original_cols + 2)
        assert "squared" in collected.columns
        assert "sqrt" in collected.columns
        for col in original_columns:
            assert col in collected.columns


class TestTableFrameUDFOutputNames:
    def test_on_element_with_output_names_single_column(self):
        class SquareUDF(UDF):
            def on_element(self, values: list) -> list:
                return [values[0] ** 2]

        _, _, tf = load_simple_dataframe()
        original_df = tf._lf.collect()
        original_rows, original_cols = original_df.shape
        original_columns = set(original_df.columns)

        result = tf.udf(td.col("intColumn"), SquareUDF([("squared", td.Int64)]))

        assert isinstance(result, td.TableFrame)
        collected = result._lf.collect()
        assert collected.shape == (original_rows, original_cols + 1)
        assert "squared" in collected.columns
        assert collected["squared"].to_list() == [1, 4, 9]
        for col in original_columns:
            assert col in collected.columns

    def test_on_element_with_output_names_multiple_columns(self):
        class MultiStatsUDF(UDF):
            def on_element(self, values: list) -> list:
                val = values[0]
                return [val * 2, val * 3]

        _, _, tf = load_simple_dataframe()
        original_df = tf._lf.collect()
        original_rows, original_cols = original_df.shape
        original_columns = set(original_df.columns)

        result = tf.udf(
            td.col("intColumn"),
            MultiStatsUDF([("doubled", td.Int64), ("tripled", td.Int64)]),
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

    def test_on_batch_with_output_names_override(self):
        class RenameUDF(UDF):
            def on_batch(
                self, series: list[td_typing.Series]
            ) -> list[td_typing.Series]:
                doubled = cast(td_typing.Series, cast(object, series[0] * 2))
                return [doubled]

        _, _, tf = load_simple_dataframe()
        original_df = tf._lf.collect()
        original_rows, original_cols = original_df.shape
        original_columns = set(original_df.columns)

        udf = RenameUDF([("doubled", td.Int64)])
        udf.output_columns([("doubled_new", None)])  # Override name
        result = tf.udf(td.col("intColumn"), udf)

        assert isinstance(result, td.TableFrame)
        collected = result._lf.collect()
        assert collected.shape == (original_rows, original_cols + 1)
        assert "doubled" not in collected.columns
        assert "doubled_new" in collected.columns
        assert collected["doubled_new"].to_list() == [2, 4, 6]
        for col in original_columns:
            assert col in collected.columns

    def test_on_batch_output_name_conflict(self):
        class ConflictUDF(UDF):
            def on_batch(
                self, series: list[td_typing.Series]
            ) -> list[td_typing.Series]:
                return [series[0]]

        _, _, tf = load_simple_dataframe()

        with pytest.raises(DuplicateError):
            result = tf.udf(td.col("intColumn"), ConflictUDF([("intColumn", td.Int64)]))
            collected = result._lf.collect()
            assert collected["intColumn"].to_list() == [1, 2, 3]

    def test_on_element_missing_super_init_raises_at_runtime(self):
        # noinspection PyMissingConstructor
        class MissingInitUDF(UDF):
            def __init__(self):
                pass

            def on_element(self, values: list) -> list:
                return [values[0] * 2]

        _, _, tf = load_simple_dataframe()

        with pytest.raises(RuntimeError, match="did not call super"):
            result = tf.udf(td.col("intColumn"), MissingInitUDF())
            result.collect()

    def test_output_names_count_mismatch_too_many_on_batch(self):
        class TooManyNamesUDF(UDF):
            def on_batch(
                self, series: list[td_typing.Series]
            ) -> list[td_typing.Series]:
                doubled = cast(td_typing.Series, cast(object, series[0] * 2))
                return [doubled]

        _, _, tf = load_simple_dataframe()

        with pytest.raises(ValueError, match="produced 1 output columns"):
            result = tf.udf(
                td.col("intColumn"),
                TooManyNamesUDF(
                    [("first", td.Int64), ("second", td.Int64), ("third", td.Int64)]
                ),
            )
            result._lf.collect()

    def test_output_names_count_mismatch_too_few_on_batch(self):
        class TooFewNamesUDF(UDF):
            def on_batch(
                self, series: list[td_typing.Series]
            ) -> list[td_typing.Series]:
                doubled = cast(td_typing.Series, cast(object, series[0] * 2))
                tripled = cast(td_typing.Series, cast(object, series[0] * 3))
                return [doubled, tripled]

        _, _, tf = load_simple_dataframe()

        with pytest.raises(ValueError, match="produced 2 output columns"):
            result = tf.udf(
                td.col("intColumn"), TooFewNamesUDF([("doubled", td.Int64)])
            )
            result._lf.collect()

    def test_output_names_count_mismatch_too_few_on_element(self):
        class TooFewNamesElementUDF(UDF):
            def on_element(self, values: list) -> list:
                return [values[0] * 2, values[0] * 3]

        _, _, tf = load_simple_dataframe()

        with pytest.raises(ValueError, match="produced 2 output columns"):
            result = tf.udf(
                td.col("intColumn"), TooFewNamesElementUDF([("doubled", td.Int64)])
            )
            result._lf.collect()

    def test_duplicate_output_names_in_schema(self):
        class DuplicateNamesUDF(UDF):
            def on_batch(
                self, series: list[td_typing.Series]
            ) -> list[td_typing.Series]:
                doubled = cast(td_typing.Series, cast(object, series[0] * 2))
                tripled = cast(td_typing.Series, cast(object, series[0] * 3))
                return [doubled, tripled]

        _, _, tf = load_simple_dataframe()

        with pytest.raises(ValueError, match="duplicate column names"):
            result = tf.udf(
                td.col("intColumn"),
                DuplicateNamesUDF([("result", td.Int64), ("result", td.Int64)]),
            )
            result.collect()

    def test_none_output_names_for_on_batch_with_alias(self):
        class EmptyNamesWithAliasUDF(UDF):
            def on_batch(
                self, series: list[td_typing.Series]
            ) -> list[td_typing.Series]:
                return [series[0]]

        _, _, tf = load_simple_dataframe()
        original_df = tf._lf.collect()
        original_rows, original_cols = original_df.shape
        original_columns = set(original_df.columns)
        result = tf.udf(
            td.col("intColumn"),
            EmptyNamesWithAliasUDF(("proper_name", td.Int64)).output_columns(
                (None, None)
            ),
        )
        assert isinstance(result, td.TableFrame)
        collected = result._lf.collect()
        assert collected.shape == (original_rows, original_cols + 1)
        assert "proper_name" in collected.columns
        for col in original_columns:
            assert col in collected.columns

    def test_output_names_with_special_characters(self):
        class SpecialNamesUDF(UDF):
            def on_element(self, values: list) -> list:
                return [values[0] * 2]

        _, _, tf = load_simple_dataframe()
        original_df = tf._lf.collect()
        original_rows, original_cols = original_df.shape
        original_columns = set(original_df.columns)

        result = tf.udf(
            td.col("intColumn"), SpecialNamesUDF([("σpecïalᚣ🧪列👀💩", td.Int64)])
        )

        assert isinstance(result, td.TableFrame)
        collected = result._lf.collect()
        assert collected.shape == (original_rows, original_cols + 1)
        assert "σpecïalᚣ🧪列👀💩" in collected.columns
        assert collected["σpecïalᚣ🧪列👀💩"].to_list() == [2, 4, 6]
        for col in original_columns:
            assert col in collected.columns

    def test_output_names_multiple_inputs_multiple_outputs(self):
        class MultiInMultiOutUDF(UDF):
            def on_element(self, values: list) -> list:
                return [
                    values[0] + values[1],
                    values[0] * values[1],
                    values[0] - values[1],
                ]

        data = {"a": [1, 2, 3], "b": [4, 5, 6]}
        tf = td.TableFrame(data)
        original_df = tf._lf.collect()
        original_rows, original_cols = original_df.shape
        original_columns = set(original_df.columns)

        result = tf.udf(
            td.col("a", "b"),
            MultiInMultiOutUDF(
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
    def test_concat_all_on_element_and_on_batch_combinations(self):
        class BatchScalerUDF(UDF):
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

        class ElementScalerUDF(UDF):
            def __init__(self, the_factor: int, output_columns):
                super().__init__(output_columns)
                self._factor = the_factor

            def on_element(self, values: list) -> list:
                return [values[0] * self._factor]

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
                    udf_instance = BatchScalerUDF(factor, [(output_column, td.Int64)])
                else:
                    udf_instance = ElementScalerUDF(factor, [(output_column, td.Int64)])
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

    def test_udf_alternates_with_polars_transformations(self):
        class BatchAddOneUDF(UDF):
            def on_batch(
                self, series: list[td_typing.Series]
            ) -> list[td_typing.Series]:
                incremented = cast(
                    td_typing.Series,
                    cast(object, series[0] + 1),
                )
                return [incremented]

        class ElementSquareUDF(UDF):
            def on_element(self, values: list) -> list:
                return [values[0] * values[0]]

        class BatchAbsoluteUDF(UDF):
            def on_batch(
                self, series: list[td_typing.Series]
            ) -> list[td_typing.Series]:
                absolute = cast(
                    td_typing.Series,
                    cast(object, series[0].abs()),
                )
                return [absolute]

        _, _, tf = load_simple_dataframe()

        pipeline = tf.udf(
            td.col("intColumn"), BatchAddOneUDF([("batch_added", td.Int64)])
        )
        pipeline = pipeline.with_columns(
            td.col("batch_added").mul(2).alias("batch_doubled"),
        )
        pipeline = pipeline.udf(
            td.col("batch_doubled"), ElementSquareUDF([("element_squared", td.Int64)])
        )
        pipeline = pipeline.with_columns(
            (td.col("element_squared") - td.col("batch_doubled")).alias("difference"),
        )
        pipeline = pipeline.udf(
            td.col("difference"), BatchAbsoluteUDF([("difference_abs", td.Int64)])
        )

        collected = pipeline._lf.collect()
        assert collected["batch_added"].to_list() == [2, 3, 4]
        assert collected["batch_doubled"].to_list() == [4, 6, 8]
        assert collected["element_squared"].to_list() == [16, 36, 64]
        assert collected["difference"].to_list() == [12, 30, 56]
        assert collected["difference_abs"].to_list() == [12, 30, 56]
