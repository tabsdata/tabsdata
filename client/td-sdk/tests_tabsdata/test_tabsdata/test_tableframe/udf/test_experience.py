#
# Copyright 2025 Tabs Data Inc.
#
from typing import cast

import tabsdata as td
import tabsdata.tableframe.typing as td_typing
from tabsdata.tableframe.udf.function import UDF
from tests_tabsdata.test_tabsdata.test_tableframe.common import pretty_polars

pretty_polars()


def test_multiple_outputs_from_multiple_inputs_on_batch():
    class SumAndProductUDF(UDF):
        def __init__(self):
            super().__init__([("sum", td.Int64), ("product", td.Int64)])

        def on_batch(self, series: list[td_typing.Series]) -> list[td_typing.Series]:
            s_sum = series[0] + series[1]
            s_product = series[0] * series[1]
            return [
                s_sum,
                s_product,
            ]

    data = {"a": [1, 2, 3], "b": [4, 5, 6]}
    tf = td.TableFrame(data)

    original_df = tf._lf.collect()
    original_rows, original_cols = original_df.shape
    original_columns = set(original_df.columns)

    sum_and_product_udf = SumAndProductUDF()

    # fmt: off
    result = tf.udf(td.col("a", "b"),
                    sum_and_product_udf
                    .output_columns([("the_sum", td.Int32),
                                     ("the_product", td.Int32)])
                    .output_columns([("a_sum", None),
                                     (None, td.Int32)])
                    .output_columns([(None, td.Int32),
                                     ("a_product", None)])
                    .output_columns({0: ("this_sum", td.Float64),
                                     1: ("this_product", None)})
                    .output_columns({0: ("this_sum", None),
                                     1: ("this_product", td.Float64)})
                    .output_columns({0: (None, td.Float32)})
                    .output_columns({1: (None, td.Float32)})
                    .output_columns({0: ("that_sum", None)})
                    .output_columns({1: ("that_product", None)}),
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


def test_multiple_outputs_from_multiple_inputs_on_element():
    class SumAndProductUDF(UDF):
        def __init__(self):
            super().__init__([("sum", td.Int64), ("product", td.Int64)])

        def on_element(self, values: list) -> list:
            v_sum = values[0] + values[1]
            v_product = values[0] * values[1]
            return [
                v_sum,
                v_product,
            ]

    data = {"a": [1, 2, 3], "b": [4, 5, 6]}
    tf = td.TableFrame(data)

    original_df = tf._lf.collect()
    original_rows, original_cols = original_df.shape
    original_columns = set(original_df.columns)

    sum_and_product_udf = SumAndProductUDF()

    # fmt: off
    result = tf.udf(td.col("a", "b"),
                    sum_and_product_udf
                    .output_columns([("the_sum", td.Int32),
                                     ("the_product", td.Int32)])
                    .output_columns([("a_sum", None),
                                     (None, td.Int32)])
                    .output_columns([(None, td.Int32),
                                     ("a_product", None)])
                    .output_columns({0: ("this_sum", td.Float64),
                                     1: ("this_product", None)})
                    .output_columns({0: ("this_sum", None),
                                     1: ("this_product", td.Float64)})
                    .output_columns({0: (None, td.Float32)})
                    .output_columns({1: (None, td.Float32)})
                    .output_columns({0: ("that_sum", None)})
                    .output_columns({1: ("that_product", None)}),
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


def test_multiple_outputs_from_multiple_inputs_on_batch_with_parameter():
    class SumAndProductUDF(UDF):
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

    data = {"a": [1, 2, 3], "b": [4, 5, 6]}
    tf = td.TableFrame(data)

    original_df = tf._lf.collect()
    original_rows, original_cols = original_df.shape
    original_columns = set(original_df.columns)

    sum_and_product_udf = SumAndProductUDF

    # fmt: off
    result = tf.udf(td.col("a", "b"),
                    sum_and_product_udf(5)
                    .output_columns([("the_sum", td.Int32),
                                     ("the_product", td.Int32)])
                    .output_columns([("a_sum", None),
                                     (None, td.Int32)])
                    .output_columns([(None, td.Int32),
                                     ("a_product", None)])
                    .output_columns({0: ("this_sum", td.Float64),
                                     1: ("this_product", None)})
                    .output_columns({0: ("this_sum", None),
                                     1: ("this_product", td.Float64)})
                    .output_columns({0: (None, td.Float32)})
                    .output_columns({1: (None, td.Float32)})
                    .output_columns({0: ("that_sum", None)})
                    .output_columns({1: ("that_product", None)}),
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


def test_multiple_outputs_from_multiple_inputs_on_element_with_parameter():
    class SumAndProductUDF(UDF):
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

    data = {"a": [1, 2, 3], "b": [4, 5, 6]}
    tf = td.TableFrame(data)

    original_df = tf._lf.collect()
    original_rows, original_cols = original_df.shape
    original_columns = set(original_df.columns)

    sum_and_product_udf = SumAndProductUDF

    # fmt: off
    result = tf.udf(td.col("a", "b"),
                    sum_and_product_udf(5)
                    .output_columns([("the_sum", td.Int32),
                                     ("the_product", td.Int32)])
                    .output_columns([("a_sum", None),
                                     (None, td.Int32)])
                    .output_columns([(None, td.Int32),
                                     ("a_product", None)])
                    .output_columns({0: ("this_sum", td.Float64),
                                     1: ("this_product", None)})
                    .output_columns({0: ("this_sum", None),
                                     1: ("this_product", td.Float64)})
                    .output_columns({0: (None, td.Float32)})
                    .output_columns({1: (None, td.Float32)})
                    .output_columns({0: ("that_sum", None)})
                    .output_columns({1: ("that_product", None)}),
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
