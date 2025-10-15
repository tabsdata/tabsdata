#
# Copyright 2025 Tabs Data Inc.
#

import pytest

import tabsdata as td
from tabsdata.tableframe.udf.function import UDF
from tests_tabsdata.test_tabsdata.test_tableframe.common import pretty_polars

pretty_polars()


class CombineInputsUDF(UDF):
    def __init__(self, num_outputs=1):
        output_schema = [(f"out_{i}", td.String) for i in range(num_outputs)]
        super().__init__(output_schema)
        self.num_outputs = num_outputs

    def on_element(self, values: list) -> list:
        result_val = "_".join(map(str, values))
        return [result_val] * self.num_outputs


def test_udf_single_positional_expr():
    data = {"a": [1, 2], "b": ["x", "y"]}
    tf = td.TableFrame(data)
    result = tf.udf("a", function=CombineInputsUDF())
    collected = result.to_polars_df()
    assert "out_0" in collected.columns
    assert collected["out_0"].to_list() == ["1", "2"]


def test_udf_multiple_positional_exprs():
    data = {"a": [1, 2], "b": ["x", "y"]}
    tf = td.TableFrame(data)
    result = tf.udf("a", "b", function=CombineInputsUDF())
    collected = result.to_polars_df()
    assert "out_0" in collected.columns
    assert collected["out_0"].to_list() == ["1_x", "2_y"]


def test_udf_list_of_positional_exprs():
    data = {"a": [1, 2], "b": ["x", "y"]}
    tf = td.TableFrame(data)
    result = tf.udf(["a", "b"], function=CombineInputsUDF())
    collected = result.to_polars_df()
    assert "out_0" in collected.columns
    assert collected["out_0"].to_list() == ["1_x", "2_y"]


class InitSystemColumnsUDF(UDF):
    def __init__(self):
        output_schema = [("$td.id", td.String)]
        super().__init__(output_schema)

    def on_element(self, values: list) -> list:
        columns_out = "_".join(map(str, values))
        return [columns_out]


def test_system_column_ini_init():
    data = {"a": [1, 2], "b": ["x", "y"]}
    tf = td.TableFrame(data)
    with pytest.raises(
        ValueError,
        match=(
            "The output column names of a UDF cannot use the "
            "reserved system columns namespace"
        ),
    ):
        result = tf.udf("a", function=InitSystemColumnsUDF())
        _ = result._lf.collect()


class OutputSystemColumnsUDF(UDF):
    def __init__(self):
        output_schema = [("td.id", td.String)]
        super().__init__(output_schema)

    def on_element(self, values: list) -> list:
        columns_out = "_".join(map(str, values))
        return [columns_out]


def test_system_column_in_output_columns():
    data = {"a": [1, 2], "b": ["x", "y"]}
    tf = td.TableFrame(data)
    with pytest.raises(
        ValueError,
        match=(
            "The output column names of a UDF cannot use the "
            "reserved system columns namespace"
        ),
    ):
        result = tf.udf(
            "a", function=OutputSystemColumnsUDF().output_columns(("$td.id", None))
        )
        _ = result._lf.collect()
