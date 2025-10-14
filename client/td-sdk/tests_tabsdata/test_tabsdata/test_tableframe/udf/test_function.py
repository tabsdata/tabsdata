#
# Copyright 2025 Tabs Data Inc.
#

import pytest

import tabsdata as td
import tabsdata.tableframe.typing as td_typing
from tabsdata.tableframe.udf.function import UDF


class TestUDFValidation:
    def test_cannot_instantiate_directly(self):
        with pytest.raises(
            TypeError,
            match="Cannot instantiate UDF directly",
        ):
            # noinspection PyAbstractClass
            UDF([("a", td.Int64)])

    # noinspection PyUnusedLocal
    def test_cannot_implement_call_method(self):
        with pytest.raises(
            TypeError,
            match="must not implement '__call__' method",
        ):

            class InvalidUDF(UDF):
                def __call__(self, series):
                    return series

    # noinspection PyUnusedLocal
    def test_must_implement_at_least_one_method(self):
        with pytest.raises(
            TypeError,
            match="must implement exactly one of 'on_element' or 'on_batch' methods",
        ):

            class InvalidUDF(UDF):
                pass

    # noinspection PyUnusedLocal
    def test_cannot_implement_both_methods(self):
        with pytest.raises(
            TypeError,
            match="must implement exactly one of 'on_element' and 'on_batch' methods",
        ):

            class InvalidUDF(UDF):
                def on_batch(self, series):
                    return series

                def on_element(self, values):
                    return values

    def test_valid_on_batch_implementation(self):
        class ValidBatchUDF(UDF):
            def on_batch(self, series):
                return series

        udf = ValidBatchUDF([("a", td.Int64)])
        assert udf is not None

    def test_valid_on_element_implementation(self):
        class ValidElementUDF(UDF):
            def on_element(self, values):
                return values

        udf = ValidElementUDF([("a", td.Int64)])
        assert udf is not None


class TestUDFOnBatch:
    def test_call_delegates_to_on_batch(self):
        class BatchUDF(UDF):
            def on_batch(self, series):
                output_series = []
                for values in zip(*series):
                    output_series.append(sum(values))
                return [td_typing.Series(output_series)]

        udf = BatchUDF([("sum", td.Int64)])
        series_in = [
            td_typing.Series([1, 2, 3]),
            td_typing.Series([10, 20, 30]),
        ]
        series_out = udf(series_in)
        assert len(series_out) == 1
        assert series_out[0].to_list() == [11, 22, 33]

    def test_on_batch_with_empty_input_fails_on_schema_mismatch(self):
        class BatchUDF(UDF):
            def on_batch(self, series):
                return []

        udf = BatchUDF([("a", td.Int64)])
        with pytest.raises(
            ValueError,
            match="produced 0 output columns",
        ):
            udf([])

    def test_on_batch_caching(self):
        class BatchUDF(UDF):
            def on_batch(self, series):
                return series

        udf = BatchUDF([("a", td.Int64)])
        assert udf._on_batch is True
        assert udf._on_element is False

    def test_on_batch_multiple_outputs(self):
        class MultiOutputUDF(UDF):
            def on_batch(self, series):
                sums = []
                products = []
                for values in zip(*series):
                    sums.append(sum(values))
                    product = 1
                    for value in values:
                        product *= value
                    products.append(product)
                return [
                    td_typing.Series(sums),
                    td_typing.Series(products),
                ]

        udf = MultiOutputUDF([("sum", td.Int64), ("product", td.Int64)])
        series_in = [
            td_typing.Series([1, 2, 3]),
            td_typing.Series([4, 5, 6]),
        ]
        series_out = udf(series_in)
        assert len(series_out) == 2
        assert series_out[0].name == "sum"
        assert series_out[1].name == "product"
        assert series_out[0].to_list() == [5, 7, 9]
        assert series_out[1].to_list() == [4, 10, 18]

    def test_on_batch_single_column(self):
        class SquareUDF(UDF):
            def on_batch(self, series):
                squared = [value * value for value in series[0]]
                return [td_typing.Series(squared)]

        udf = SquareUDF([("squared", td.Int64)])
        series_in = [td_typing.Series([1, 2, 3])]
        series_out = udf(series_in)
        assert len(series_out) == 1
        assert series_out[0].name == "squared"
        assert series_out[0].to_list() == [1, 4, 9]


class TestUDFOnElement:
    def test_call_delegates_to_on_element(self):
        class ElementUDF(UDF):
            def on_element(self, values):
                return [sum(values)]

        udf = ElementUDF([("sum", td.Int64)])
        series_in = [
            td_typing.Series([1, 2, 3]),
            td_typing.Series([10, 20, 30]),
        ]
        series_out = udf(series_in)
        assert len(series_out) == 1
        assert series_out[0].name == "sum"
        assert series_out[0].to_list() == [11, 22, 33]

    def test_on_element_with_empty_input_fails_on_schema_mismatch(self):
        class ElementUDF(UDF):
            def on_element(self, values):
                return values

        udf = ElementUDF([("a", td.Int64)])  # Schema expects 1 column
        with pytest.raises(
            ValueError,
            match="produced 0 output columns",
        ):
            udf([])

    def test_on_element_caching(self):
        class ElementUDF(UDF):
            def on_element(self, values):
                return values

        udf = ElementUDF([("a", td.Int64)])
        assert udf._on_batch is False
        assert udf._on_element is True

    def test_on_element_multiple_outputs(self):
        class MultiOutputUDF(UDF):
            def on_element(self, values):
                values_sum = sum(values)
                values_product = 1
                for value in values:
                    values_product *= value
                return [values_sum, values_product]

        udf = MultiOutputUDF([("sum", td.Int64), ("product", td.Int64)])
        series_in = [
            td_typing.Series([1, 2, 3]),
            td_typing.Series([4, 5, 6]),
        ]
        series_out = udf(series_in)
        assert len(series_out) == 2
        assert series_out[0].name == "sum"
        assert series_out[1].name == "product"
        assert series_out[0].to_list() == [5, 7, 9]
        assert series_out[1].to_list() == [4, 10, 18]

    def test_on_element_single_column(self):
        class SquareUDF(UDF):
            def on_element(self, values):
                return [values[0] * values[0]]

        udf = SquareUDF([("squared", td.Int64)])
        series_in = [td_typing.Series([1, 2, 3])]
        result = udf(series_in)
        assert len(result) == 1
        assert result[0].name == "squared"
        assert result[0].to_list() == [1, 4, 9]


class TestUDFEdgeCases:
    def test_neither_implemented_raises_runtime_error(self):
        class TestUDF(UDF):
            def on_element(self, values):
                return values

        udf = TestUDF([("a", td.Int64)])
        udf._on_batch = False
        udf._on_element = False

        with pytest.raises(
            RuntimeError,
            match="has neither on_batch nor on_element implemented",
        ):
            udf([td_typing.Series([1, 2, 3])])

    def test_complex_transformation(self):
        class NormalizeUDF(UDF):
            def on_element(self, values):
                total = sum(values)
                if total == 0:
                    return [0.0] * len(values)
                return [value / total for value in values]

        udf = NormalizeUDF(
            [
                ("norm1", td.Float64),
                ("norm2", td.Float64),
                ("norm3", td.Float64),
            ]
        )
        series_in = [
            td_typing.Series([1.0, 2.0, 3.0]),
            td_typing.Series([2.0, 3.0, 4.0]),
            td_typing.Series([3.0, 4.0, 5.0]),
        ]
        series_out = udf(series_in)
        assert len(series_out) == 3
        assert series_out[0].to_list() == pytest.approx([1 / 6, 2 / 9, 3 / 12])
        assert series_out[1].to_list() == pytest.approx([2 / 6, 3 / 9, 4 / 12])
        assert series_out[2].to_list() == pytest.approx([3 / 6, 4 / 9, 5 / 12])
