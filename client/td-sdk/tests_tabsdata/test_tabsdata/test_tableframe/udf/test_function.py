#
# Copyright 2025 Tabs Data Inc.
#

import pytest

import tabsdata as td
import tabsdata.tableframe.typing as td_typing
from tabsdata.tableframe.udf.function import (
    SIGNATURE_LIST,
    SIGNATURE_UNPACKED,
    UDF,
    UDFList,
    UDFUnpacked,
)


class TestUDFValidation:
    @pytest.mark.parametrize("udf_base_class", [UDFList, UDFUnpacked])
    def test_cannot_instantiate_directly(self, udf_base_class):
        with pytest.raises(
            TypeError,
            match="Cannot instantiate",
        ):
            udf_base_class([("a", td.Int64)])

    # noinspection PyUnusedLocal
    @pytest.mark.parametrize("udf_base_class", [UDFList, UDFUnpacked])
    def test_cannot_implement_call_method(self, udf_base_class):
        with pytest.raises(
            TypeError,
            match="must not implement '__call__' method",
        ):

            class InvalidUDF(udf_base_class):
                def __call__(self, series):
                    return series

    # noinspection PyUnusedLocal
    @pytest.mark.parametrize("udf_base_class", [UDFList, UDFUnpacked])
    def test_must_implement_at_least_one_method(self, udf_base_class):
        with pytest.raises(
            TypeError,
            match="must implement exactly one of 'on_element' or 'on_batch' methods",
        ):

            class InvalidUDF(udf_base_class):
                pass

    # noinspection PyUnusedLocal
    @pytest.mark.parametrize("udf_base_class", [UDFList, UDFUnpacked])
    def test_cannot_implement_both_methods(self, udf_base_class):
        with pytest.raises(
            TypeError,
            match="must implement exactly one of 'on_element' and 'on_batch' methods",
        ):

            class InvalidUDF(udf_base_class):
                # noinspection PyMethodMayBeStatic
                def on_batch(self, *args):
                    return args

                # noinspection PyMethodMayBeStatic
                def on_element(self, *args):
                    return args

    @pytest.mark.parametrize("udf_class", [UDFList, UDFUnpacked])
    def test_valid_on_batch_implementation(self, udf_class):
        class ValidBatchUDF(udf_class):
            # noinspection PyMethodMayBeStatic
            def on_batch(self, *series):
                return list(series)

        udf = ValidBatchUDF([("a", td.Int64)])
        assert udf is not None

    @pytest.mark.parametrize("udf_class", [UDFList, UDFUnpacked])
    def test_valid_on_element_implementation(self, udf_class):
        class ValidElementUDF(udf_class):
            # noinspection PyMethodMayBeStatic
            def on_element(self, *values):
                return list(values)

        udf = ValidElementUDF([("a", td.Int64)])
        assert udf is not None


class TestUDFOnBatch:
    @pytest.mark.parametrize("signature", [SIGNATURE_LIST, SIGNATURE_UNPACKED])
    def test_call_delegates_to_on_batch(self, signature):
        class BatchUDFList(UDFList):
            def on_batch(self, series):
                output_series = []
                for values in zip(*series):
                    output_series.append(sum(values))
                return [td_typing.Series(output_series)]

        class BatchUDFUnpacked(UDFUnpacked):
            def on_batch(self, *series):
                output_series = []
                for values in zip(*series):
                    output_series.append(sum(values))
                return [td_typing.Series(output_series)]

        udf_class = BatchUDFList if signature == SIGNATURE_LIST else BatchUDFUnpacked
        udf = udf_class([("sum", td.Int64)])
        series_in = [
            td_typing.Series([1, 2, 3]),
            td_typing.Series([10, 20, 30]),
        ]
        series_out = udf(series_in)
        assert len(series_out) == 1
        assert series_out[0].to_list() == [11, 22, 33]

    @pytest.mark.parametrize("udf_class", [UDFList, UDFUnpacked])
    def test_on_batch_with_empty_input_fails_on_schema_mismatch(self, udf_class):
        class BatchUDF(udf_class, UDF):
            def on_batch(self, *series):
                return []

        udf = BatchUDF([("a", td.Int64)])
        with pytest.raises(
            ValueError,
            match="produced 0 output columns",
        ):
            udf([])

    @pytest.mark.parametrize("udf_class", [UDFList, UDFUnpacked])
    def test_on_batch_caching(self, udf_class):
        class BatchUDF(udf_class):
            # noinspection PyMethodMayBeStatic
            def on_batch(self, *series):
                return list(series)

        udf = BatchUDF([("a", td.Int64)])
        assert udf._on_batch is True
        assert udf._on_element is False

    @pytest.mark.parametrize("signature", [SIGNATURE_LIST, SIGNATURE_UNPACKED])
    def test_on_batch_multiple_outputs(self, signature):
        class MultiOutputUDFList(UDFList):
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

        class MultiOutputUDFUnpacked(UDFUnpacked):
            def on_batch(self, *series):
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

        udf_class = (
            MultiOutputUDFList
            if signature == SIGNATURE_LIST
            else MultiOutputUDFUnpacked
        )
        udf = udf_class([("sum", td.Int64), ("product", td.Int64)])
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

    @pytest.mark.parametrize("signature", [SIGNATURE_LIST, SIGNATURE_UNPACKED])
    def test_on_batch_single_column(self, signature):
        class SquareUDFList(UDFList):
            def on_batch(self, series):
                squared = [value * value for value in series[0]]
                return [td_typing.Series(squared)]

        class SquareUDFUnpacked(UDFUnpacked):
            def on_batch(self, series_0):
                squared = [value * value for value in series_0]
                return [td_typing.Series(squared)]

        udf_class = SquareUDFList if signature == SIGNATURE_LIST else SquareUDFUnpacked
        udf = udf_class([("squared", td.Int64)])
        series_in = [td_typing.Series([1, 2, 3])]
        series_out = udf(series_in)
        assert len(series_out) == 1
        assert series_out[0].name == "squared"
        assert series_out[0].to_list() == [1, 4, 9]


class TestUDFOnElement:
    @pytest.mark.parametrize("signature", [SIGNATURE_LIST, SIGNATURE_UNPACKED])
    def test_call_delegates_to_on_element(self, signature):
        class ElementUDFList(UDFList):
            def on_element(self, values):
                return [sum(values)]

        class ElementUDFUnpacked(UDFUnpacked):
            def on_element(self, *values):
                return [sum(values)]

        udf_class = (
            ElementUDFList if signature == SIGNATURE_LIST else ElementUDFUnpacked
        )
        udf = udf_class([("sum", td.Int64)])
        series_in = [
            td_typing.Series([1, 2, 3]),
            td_typing.Series([10, 20, 30]),
        ]
        series_out = udf(series_in)
        assert len(series_out) == 1
        assert series_out[0].name == "sum"
        assert series_out[0].to_list() == [11, 22, 33]

    @pytest.mark.parametrize("udf_class", [UDFList, UDFUnpacked])
    def test_on_element_with_empty_input_fails_on_schema_mismatch(self, udf_class):
        class ElementUDF(udf_class, UDF):
            def on_element(self, *values):
                return list(values)

        udf = ElementUDF([("a", td.Int64)])
        with pytest.raises(
            ValueError,
            match="produced 0 output columns",
        ):
            udf([])

    @pytest.mark.parametrize("udf_class", [UDFList, UDFUnpacked])
    def test_on_element_caching(self, udf_class):
        class ElementUDF(udf_class):
            # noinspection PyMethodMayBeStatic
            def on_element(self, *values):
                return list(values)

        udf = ElementUDF([("a", td.Int64)])
        assert udf._on_batch is False
        assert udf._on_element is True

    @pytest.mark.parametrize("signature", [SIGNATURE_LIST, SIGNATURE_UNPACKED])
    def test_on_element_multiple_outputs(self, signature):
        class MultiOutputUDFList(UDFList):
            def on_element(self, values):
                values_sum = sum(values)
                values_product = 1
                for value in values:
                    values_product *= value
                return [values_sum, values_product]

        class MultiOutputUDFUnpacked(UDFUnpacked):
            def on_element(self, *values):
                values_sum = sum(values)
                values_product = 1
                for value in values:
                    values_product *= value
                return [values_sum, values_product]

        udf_class = (
            MultiOutputUDFList
            if signature == SIGNATURE_LIST
            else MultiOutputUDFUnpacked
        )
        udf = udf_class([("sum", td.Int64), ("product", td.Int64)])
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

    @pytest.mark.parametrize("signature", [SIGNATURE_LIST, SIGNATURE_UNPACKED])
    def test_on_element_single_column(self, signature):
        class SquareUDFList(UDFList):
            def on_element(self, values):
                return [values[0] * values[0]]

        class SquareUDFUnpacked(UDFUnpacked):
            def on_element(self, value_0):
                return [value_0 * value_0]

        udf_class = SquareUDFList if signature == SIGNATURE_LIST else SquareUDFUnpacked
        udf = udf_class([("squared", td.Int64)])
        series_in = [td_typing.Series([1, 2, 3])]
        result = udf(series_in)
        assert len(result) == 1
        assert result[0].name == "squared"
        assert result[0].to_list() == [1, 4, 9]


class TestUDFEdgeCases:
    @pytest.mark.parametrize("udf_class", [UDFList, UDFUnpacked])
    def test_neither_implemented_raises_runtime_error(self, udf_class):
        class TestUDF(udf_class, UDF):
            def on_element(self, *values):
                return list(values)

        udf = TestUDF([("a", td.Int64)])
        udf._on_batch = False
        udf._on_element = False

        with pytest.raises(
            RuntimeError,
            match="has neither on_batch nor on_element implemented",
        ):
            udf([td_typing.Series([1, 2, 3])])

    @pytest.mark.parametrize("signature", [SIGNATURE_LIST, SIGNATURE_UNPACKED])
    def test_complex_transformation(self, signature):
        class NormalizeUDFList(UDFList):
            def on_element(self, values):
                total = sum(values)
                if total == 0:
                    return [0.0] * len(values)
                return [value / total for value in values]

        class NormalizeUDFUnpacked(UDFUnpacked):
            def on_element(self, *values):
                total = sum(values)
                if total == 0:
                    return [0.0] * len(values)
                return [value / total for value in values]

        udf_class = (
            NormalizeUDFList if signature == SIGNATURE_LIST else NormalizeUDFUnpacked
        )
        udf = udf_class(
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
