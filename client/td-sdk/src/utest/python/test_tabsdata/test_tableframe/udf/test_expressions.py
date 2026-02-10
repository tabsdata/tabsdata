#
# Copyright 2025 Tabs Data Inc.
#

import pytest

import tabsdata as td
from tabsdata.tableframe.udf.function import UDFList, UDFUnpacked
from tests_tabsdata.test_tabsdata.test_tableframe.common import pretty_polars

pretty_polars()


class CombineInputsUDFList(UDFList):
    def __init__(self, num_outputs=1):
        output_schema = [(f"out_{i}", td.String) for i in range(num_outputs)]
        super().__init__(output_schema)
        self.num_outputs = num_outputs

    def on_element(self, values: list) -> list:
        result_val = "_".join(map(str, values))
        return [result_val] * self.num_outputs


class CombineInputsUDFUnpacked(UDFUnpacked):
    def __init__(self, num_outputs=1):
        output_schema = [(f"out_{i}", td.String) for i in range(num_outputs)]
        super().__init__(output_schema)
        self.num_outputs = num_outputs

    def on_element(self, *values) -> list:
        result_val = "_".join(map(str, values))
        return [result_val] * self.num_outputs


@pytest.mark.parametrize("udf_class", [CombineInputsUDFList, CombineInputsUDFUnpacked])
def test_udf_single_positional_expr(udf_class):
    data = {"a": [1, 2], "b": ["x", "y"]}
    tf = td.TableFrame(data)
    result = tf.udf("a", udf_class())
    collected = result.to_polars_df()
    assert "out_0" in collected.columns
    assert collected["out_0"].to_list() == ["1", "2"]


@pytest.mark.parametrize("udf_class", [CombineInputsUDFList, CombineInputsUDFUnpacked])
def test_udf_multiple_positional_exprs(udf_class):
    data = {"a": [1, 2], "b": ["x", "y"]}
    tf = td.TableFrame(data)
    result = tf.udf(["a", "b"], function=udf_class())
    collected = result.to_polars_df()
    assert "out_0" in collected.columns
    assert collected["out_0"].to_list() == ["1_x", "2_y"]


@pytest.mark.parametrize("udf_class", [CombineInputsUDFList, CombineInputsUDFUnpacked])
def test_udf_list_of_positional_exprs(udf_class):
    data = {"a": [1, 2], "b": ["x", "y"]}
    tf = td.TableFrame(data)
    result = tf.udf(["a", "b"], function=udf_class())
    collected = result.to_polars_df()
    assert "out_0" in collected.columns
    assert collected["out_0"].to_list() == ["1_x", "2_y"]


class InitSystemColumnsUDFList(UDFList):
    def __init__(self):
        output_schema = [("$td.id", td.String)]
        super().__init__(output_schema)

    def on_element(self, values: list) -> list:
        columns_out = "_".join(map(str, values))
        return [columns_out]


class InitSystemColumnsUDFUnpacked(UDFUnpacked):
    def __init__(self):
        output_schema = [("$td.id", td.String)]
        super().__init__(output_schema)

    def on_element(self, *values) -> list:
        columns_out = "_".join(map(str, values))
        return [columns_out]


@pytest.mark.parametrize(
    "udf_class", [InitSystemColumnsUDFList, InitSystemColumnsUDFUnpacked]
)
def test_system_column_ini_init(udf_class):
    data = {"a": [1, 2], "b": ["x", "y"]}
    tf = td.TableFrame(data)
    with pytest.raises(
        ValueError,
        match=(
            "The output column names of a UDF cannot use the "
            "reserved system columns namespace"
        ),
    ):
        result = tf.udf("a", function=udf_class())
        _ = result._lf.collect()


class OutputSystemColumnsUDFList(UDFList):
    def __init__(self):
        output_schema = [("td.id", td.String)]
        super().__init__(output_schema)

    def on_element(self, values: list) -> list:
        columns_out = "_".join(map(str, values))
        return [columns_out]


class OutputSystemColumnsUDFUnpacked(UDFUnpacked):
    def __init__(self):
        output_schema = [("td.id", td.String)]
        super().__init__(output_schema)

    def on_element(self, *values) -> list:
        columns_out = "_".join(map(str, values))
        return [columns_out]


@pytest.mark.parametrize(
    "udf_class", [OutputSystemColumnsUDFList, OutputSystemColumnsUDFUnpacked]
)
def test_system_column_in_output_columns(udf_class):
    data = {"a": [1, 2], "b": ["x", "y"]}
    tf = td.TableFrame(data)
    with pytest.raises(
        ValueError,
        match=(
            "The output column names of a UDF cannot use the "
            "reserved system columns namespace"
        ),
    ):
        result = tf.udf("a", function=udf_class().with_columns(("$td.id", None)))
        _ = result._lf.collect()
