#
# Copyright 2025 Tabs Data Inc.
#
from typing import cast

import pytest

import tabsdata as td
import tabsdata.tableframe.typing as td_typing
from tabsdata.tableframe.udf.function import (
    SIGNATURE_LIST,
    SIGNATURE_UNPACKED,
    UDFList,
    UDFUnpacked,
)
from tests_tabsdata.test_tabsdata.test_tableframe.common import pretty_polars

pretty_polars()


@pytest.mark.parametrize("signature", [SIGNATURE_LIST, SIGNATURE_UNPACKED])
def test_multiple_outputs_from_multiple_inputs_on_batch(signature):
    class SumAndProductUDFList(UDFList):
        def __init__(self):
            super().__init__([("sum", td.Int64), ("product", td.Int64)])

        def on_batch(self, series: list[td_typing.Series]) -> list[td_typing.Series]:
            s_sum = series[0] + series[1]
            s_product = series[0] * series[1]
            return [
                s_sum,
                s_product,
            ]

    class SumAndProductUDFUnpacked(UDFUnpacked):
        def __init__(self):
            super().__init__([("sum", td.Int64), ("product", td.Int64)])

        def on_batch(
            self, series_0: td_typing.Series, series_1: td_typing.Series
        ) -> list[td_typing.Series]:
            s_sum = series_0 + series_1
            s_product = series_0 * series_1
            return [
                s_sum,
                s_product,
            ]

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

    sum_and_product_udf = udf_class()

    # fmt: off
    result = tf.udf(td.col("a", "b"),
                    sum_and_product_udf
                    .with_columns([("the_sum", td.Int32),
                                   ("the_product", td.Int32)])
                    .with_columns([("a_sum", None),
                                   (None, td.Int32)])
                    .with_columns([(None, td.Int32),
                                   ("a_product", None)])
                    .with_columns({0: ("this_sum", td.Float64),
                                   1: ("this_product", None)})
                    .with_columns({0: ("this_sum", None),
                                   1: ("this_product", td.Float64)})
                    .with_columns({0: (None, td.Float32)})
                    .with_columns({1: (None, td.Float32)})
                    .with_columns({0: ("that_sum", None)})
                    .with_columns({1: ("that_product", None)}),
                    )
    # fmt: on

    assert isinstance(result, td.TableFrame)

    collected = result._lf.collect()

    assert collected.shape == (original_rows, original_cols + 2)
    assert "that_sum" in collected.columns
    assert "that_product" in collected.columns
    assert collected["that_sum"].to_list() == [5, 7, 9]
    assert collected["that_product"].to_list() == [4, 10, 18]
    for col in original_columns:
        assert col in collected.columns


@pytest.mark.parametrize("signature", [SIGNATURE_LIST, SIGNATURE_UNPACKED])
def test_multiple_outputs_from_multiple_inputs_on_element(signature):
    class SumAndProductUDFList(UDFList):
        def __init__(self):
            super().__init__([("sum", td.Int64), ("product", td.Int64)])

        def on_element(self, values: list) -> list:
            v_sum = values[0] + values[1]
            v_product = values[0] * values[1]
            return [
                v_sum,
                v_product,
            ]

    class SumAndProductUDFUnpacked(UDFUnpacked):
        def __init__(self):
            super().__init__([("sum", td.Int64), ("product", td.Int64)])

        def on_element(self, value_0, value_1) -> list:
            v_sum = value_0 + value_1
            v_product = value_0 * value_1
            return [
                v_sum,
                v_product,
            ]

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

    sum_and_product_udf = udf_class()

    # fmt: off
    result = tf.udf(td.col("a", "b"),
                    sum_and_product_udf
                    .with_columns([("the_sum", td.Int32),
                                   ("the_product", td.Int32)])
                    .with_columns([("a_sum", None),
                                   (None, td.Int32)])
                    .with_columns([(None, td.Int32),
                                   ("a_product", None)])
                    .with_columns({0: ("this_sum", td.Float64),
                                   1: ("this_product", None)})
                    .with_columns({0: ("this_sum", None),
                                   1: ("this_product", td.Float64)})
                    .with_columns({0: (None, td.Float32)})
                    .with_columns({1: (None, td.Float32)})
                    .with_columns({0: ("that_sum", None)})
                    .with_columns({1: ("that_product", None)}),
                    )
    # fmt: on

    assert isinstance(result, td.TableFrame)

    collected = result._lf.collect()

    assert collected.shape == (original_rows, original_cols + 2)
    assert "that_sum" in collected.columns
    assert "that_product" in collected.columns
    assert collected["that_sum"].to_list() == [5, 7, 9]
    assert collected["that_product"].to_list() == [4, 10, 18]
    for col in original_columns:
        assert col in collected.columns


@pytest.mark.parametrize("signature", [SIGNATURE_LIST, SIGNATURE_UNPACKED])
def test_multiple_outputs_from_multiple_inputs_on_batch_with_parameter(signature):
    class SumAndProductUDFList(UDFList):
        def __init__(self, scale=3):
            super().__init__([("sum", td.Int64), ("product", td.Int64)])
            self.scale = scale

        def on_batch(self, series: list[td_typing.Series]) -> list[td_typing.Series]:
            s_sum = cast(
                td_typing.Series, cast(object, series[0] + series[1] + self.scale)
            )
            s_product = cast(
                td_typing.Series, cast(object, series[0] * series[1] * self.scale)
            )
            return [
                s_sum,
                s_product,
            ]

    class SumAndProductUDFUnpacked(UDFUnpacked):
        def __init__(self, scale=3):
            super().__init__([("sum", td.Int64), ("product", td.Int64)])
            self.scale = scale

        def on_batch(
            self, series_0: td_typing.Series, series_1: td_typing.Series
        ) -> list[td_typing.Series]:
            s_sum = cast(
                td_typing.Series, cast(object, series_0 + series_1 + self.scale)
            )
            s_product = cast(
                td_typing.Series, cast(object, series_0 * series_1 * self.scale)
            )
            return [
                s_sum,
                s_product,
            ]

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

    sum_and_product_udf = udf_class

    # fmt: off
    result = tf.udf(td.col("a", "b"),
                    sum_and_product_udf(5)
                    .with_columns([("the_sum", td.Int32),
                                   ("the_product", td.Int32)])
                    .with_columns([("a_sum", None),
                                   (None, td.Int32)])
                    .with_columns([(None, td.Int32),
                                   ("a_product", None)])
                    .with_columns({0: ("this_sum", td.Float64),
                                   1: ("this_product", None)})
                    .with_columns({0: ("this_sum", None),
                                   1: ("this_product", td.Float64)})
                    .with_columns({0: (None, td.Float32)})
                    .with_columns({1: (None, td.Float32)})
                    .with_columns({0: ("that_sum", None)})
                    .with_columns({1: ("that_product", None)}),
                    )
    # fmt: on

    assert isinstance(result, td.TableFrame)

    collected = result._lf.collect()

    assert collected.shape == (original_rows, original_cols + 2)
    assert "that_sum" in collected.columns
    assert "that_product" in collected.columns
    assert collected["that_sum"].to_list() == [5 + 5, 7 + 5, 9 + 5]
    assert collected["that_product"].to_list() == [4 * 5, 10 * 5, 18 * 5]
    for col in original_columns:
        assert col in collected.columns


@pytest.mark.parametrize("signature", [SIGNATURE_LIST, SIGNATURE_UNPACKED])
def test_multiple_outputs_from_multiple_inputs_on_element_with_parameter(signature):
    class SumAndProductUDFList(UDFList):
        def __init__(self, scale=3):
            super().__init__([("sum", td.Int64), ("product", td.Int64)])
            self.scale = scale

        def on_element(self, values: list) -> list:
            v_sum = values[0] + values[1] + self.scale
            v_product = values[0] * values[1] * self.scale
            return [
                v_sum,
                v_product,
            ]

    class SumAndProductUDFUnpacked(UDFUnpacked):
        def __init__(self, scale=3):
            super().__init__([("sum", td.Int64), ("product", td.Int64)])
            self.scale = scale

        def on_element(self, value_0, value_1) -> list:
            v_sum = value_0 + value_1 + self.scale
            v_product = value_0 * value_1 * self.scale
            return [
                v_sum,
                v_product,
            ]

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

    sum_and_product_udf = udf_class

    # fmt: off
    result = tf.udf(td.col("a", "b"),
                    sum_and_product_udf(5)
                    .with_columns([("the_sum", td.Int32),
                                   ("the_product", td.Int32)])
                    .with_columns([("a_sum", None),
                                   (None, td.Int32)])
                    .with_columns([(None, td.Int32),
                                   ("a_product", None)])
                    .with_columns({0: ("this_sum", td.Float64),
                                   1: ("this_product", None)})
                    .with_columns({0: ("this_sum", None),
                                   1: ("this_product", td.Float64)})
                    .with_columns({0: (None, td.Float32)})
                    .with_columns({1: (None, td.Float32)})
                    .with_columns({0: ("that_sum", None)})
                    .with_columns({1: ("that_product", None)}),
                    )
    # fmt: on

    assert isinstance(result, td.TableFrame)

    collected = result._lf.collect()

    assert collected.shape == (original_rows, original_cols + 2)
    assert "that_sum" in collected.columns
    assert "that_product" in collected.columns
    assert collected["that_sum"].to_list() == [5 + 5, 7 + 5, 9 + 5]
    assert collected["that_product"].to_list() == [4 * 5, 10 * 5, 18 * 5]
    for col in original_columns:
        assert col in collected.columns
