#
# Copyright 2026 Tabsdata Inc.
#

from __future__ import annotations

import datetime as dt

import polars as pl
import pyarrow as pa
import pyarrow.compute as pc
import pytest

from tabsdatak.tableframe import TableFrame
from tabsdatak.tableframe._engine.event.error import SystemColumnWriteError
from tabsdatak.tableframe.datatypes import Float64, Int64, List, String
from tabsdatak.tableframe.functions import col
from tabsdatak.tableframe.udf import (
    UDF,
    UDFPacked,
    UDFUnpacked,
    on_batch,
    on_element,
)
from tabsdatak.tableframe.udf.error import (
    UDFDefinitionException,
    UDFErrorCode,
    UDFOutputException,
)

pytestmark = pytest.mark.tableframe


class Total(UDF):
    def __init__(self):
        super().__init__(("total", Int64))

    def on_batch(self, arrays):
        return [pc.add(arrays[0], arrays[1])]


class Ratio(UDFUnpacked):
    def __init__(self):
        super().__init__([("ratio", Float64), ("label", String)])

    def on_element(self, numerator, denominator):
        return [numerator / denominator, f"{numerator}/{denominator}"]


class Echo(UDFPacked):
    def __init__(self, outputs, result):
        super().__init__(outputs)
        self.result = result

    def on_batch(self, arrays):
        return self.result(arrays)


def _frame(data: dict) -> TableFrame:
    return TableFrame.from_dict(data)


def _rows(frame: TableFrame) -> dict:
    return frame._to_dict()


def test_on_batch_appends_its_columns():
    frame = _frame({"a": [1, 2], "b": [10, 20]}).udf(["a", "b"], Total())

    assert _rows(frame) == {"a": [1, 2], "b": [10, 20], "total": [11, 22]}
    assert frame._to_polars_df().schema["total"] == Int64


def test_on_element_receives_unpacked_values_row_by_row():
    frame = _frame({"n": [1, 3], "d": [2, 4]}).udf(col("n", "d"), Ratio())

    assert _rows(frame) == {
        "n": [1, 3],
        "d": [2, 4],
        "ratio": [0.5, 0.75],
        "label": ["1/2", "3/4"],
    }


def test_on_element_receives_packed_values_by_default():
    class Upper(UDF):
        def __init__(self):
            super().__init__(("upper", String))

        def on_element(self, values):
            return [values[0].upper() if values[0] is not None else None]

    frame = _frame({"s": ["ab", None]}).udf("s", Upper())

    assert _rows(frame)["upper"] == ["AB", None]


def test_on_batch_receives_pyarrow_arrays_of_the_selected_columns_only():
    seen = []

    def record(arrays):
        seen.append([type(array) for array in arrays])
        return [arrays[0]]

    frame = _frame({"a": [1], "b": [2], "c": [3]}).udf(
        "b", Echo(("copy", Int64), record)
    )
    _rows(frame)

    assert seen == [[pa.Int64Array]]


def test_an_output_named_like_an_existing_column_replaces_it():
    frame = _frame({"a": [1, 2], "b": [10, 20], "total": [0, 0]})

    assert _rows(frame.udf(["a", "b"], Total()))["total"] == [11, 22]


def test_output_names_rename_every_column_when_the_function_is_created():
    function = Ratio(output_names=["share", "text"])
    frame = _frame({"n": [1], "d": [2]}).udf(["n", "d"], function)

    assert function.columns() == [("share", Float64), ("text", String)]
    assert _rows(frame) == {"n": [1], "d": [2], "share": [0.5], "text": ["1/2"]}


def test_a_single_output_may_be_renamed_with_a_name_alone():
    assert Total(output_names="sum").columns() == [("sum", Int64)]
    assert Total().with_names("sum").columns() == [("sum", Int64)]


def test_with_names_renames_a_copy_and_keeps_the_data_types():
    function = Ratio()
    renamed = function.with_names(["share", "text"])

    assert renamed.columns() == [("share", Float64), ("text", String)]
    assert function.columns() == [("ratio", Float64), ("label", String)]


def test_a_prefix_and_a_suffix_are_added_to_every_output_name():
    assert Ratio(output_prefix="nd_").columns() == [
        ("nd_ratio", Float64),
        ("nd_label", String),
    ]
    assert Ratio().with_names(output_suffix="_nd").columns() == [
        ("ratio_nd", Float64),
        ("label_nd", String),
    ]
    assert Ratio(
        output_names=["share", "text"], output_prefix="p_", output_suffix="_s"
    ).columns() == [("p_share_s", Float64), ("p_text_s", String)]


def test_the_same_function_applied_twice_is_told_apart_by_a_prefix():
    frame = (
        _frame({"a": [1], "b": [2], "c": [3], "d": [4]})
        .udf(["a", "b"], Total(output_prefix="ab_"))
        .udf(["c", "d"], Total(output_prefix="cd_"))
    )

    assert _rows(frame) == {
        "a": [1],
        "b": [2],
        "c": [3],
        "d": [4],
        "ab_total": [3],
        "cd_total": [7],
    }


def test_output_names_reach_a_function_with_its_own_constructor_arguments():
    function = Echo(("copy", Int64), lambda arrays: arrays, output_names="echo")

    assert _rows(_frame({"a": [1]}).udf("a", function)) == {"a": [1], "echo": [1]}


def test_the_function_streams_batch_by_batch(tmp_path):
    rows = 500_000
    sizes = []

    def record(arrays):
        sizes.append(len(arrays[0]))
        return [arrays[0]]

    frame = TableFrame.from_polars(pl.LazyFrame({"a": range(rows)}))
    path = tmp_path / "out.parquet"
    frame.udf("a", Echo(("copy", Int64), record))._sink_public(path)

    assert len(sizes) > 1
    assert sum(sizes) == rows
    assert pl.read_parquet(path)["copy"].sum() == sum(range(rows))


@pytest.mark.parametrize(
    "build, code",
    [
        (lambda: UDF(("x", Int64)), UDFErrorCode.UDF_1),
        (lambda: UDFUnpacked(("x", Int64)), UDFErrorCode.UDF_1),
        (lambda: Echo([], None), UDFErrorCode.UDF_5),
        (lambda: Echo("x", None), UDFErrorCode.UDF_5),
        (lambda: Echo([("x",)], None), UDFErrorCode.UDF_6),
        (lambda: Echo(("", Int64), None), UDFErrorCode.UDF_6),
        (lambda: Echo(("x", int), None), UDFErrorCode.UDF_6),
        (lambda: Echo([("x", Int64), ("x", String)], None), UDFErrorCode.UDF_7),
        (lambda: Ratio(output_names=["share"]), UDFErrorCode.UDF_17),
        (lambda: Ratio().with_names("share"), UDFErrorCode.UDF_17),
        (lambda: Total().with_names(["y", "z"]), UDFErrorCode.UDF_17),
        (lambda: Total().with_names(("y",)), UDFErrorCode.UDF_17),
        (lambda: Total(output_names=[""]), UDFErrorCode.UDF_6),
        (lambda: Total(output_names=[1]), UDFErrorCode.UDF_6),
        (lambda: Ratio().with_names(["x", "x"]), UDFErrorCode.UDF_7),
        (lambda: Total(output_prefix=1), UDFErrorCode.UDF_19),
        (lambda: Total().with_names(output_suffix=["_x"]), UDFErrorCode.UDF_19),
        (lambda: _frame({"a": [1]}).udf([], Total()), UDFErrorCode.UDF_9),
    ],
)
def test_a_function_defined_wrongly_fails_when_it_is_defined(build, code):
    with pytest.raises(UDFDefinitionException) as error:
        build()

    assert error.value.api_error is code


@pytest.mark.parametrize(
    "body, code",
    [
        ({}, UDFErrorCode.UDF_2),
        (
            {"on_batch": lambda self, arrays: arrays, "columns": lambda self: []},
            UDFErrorCode.UDF_3,
        ),
        (
            {"on_batch": lambda self, arrays: arrays, "with_names": lambda self: []},
            UDFErrorCode.UDF_3,
        ),
        (
            {
                "__init__": lambda self, output_names: None,
                "on_batch": lambda self, arrays: arrays,
            },
            UDFErrorCode.UDF_18,
        ),
        (
            {
                "__init__": lambda self, output_suffix=None: None,
                "on_batch": lambda self, arrays: arrays,
            },
            UDFErrorCode.UDF_18,
        ),
    ],
)
def test_a_subclass_defined_wrongly_fails_when_it_is_declared(body, code):
    with pytest.raises(UDFDefinitionException) as error:
        type("Wrong", (UDF,), body)

    assert error.value.api_error is code


def test_on_batch_runs_on_element_for_each_row_by_default():
    packed = on_element(("sum", Int64), signature="packed")(lambda values: sum(values))
    unpacked = Ratio()

    assert packed.on_batch([pa.array([1, 2]), pa.array([10, 20])])[0].to_pylist() == [
        11,
        22,
    ]
    assert [
        array.to_pylist() for array in unpacked.on_batch(pa.array([1, 3]), pa.array([2, 4]))
    ] == [[0.5, 0.75], ["1/2", "3/4"]]


def test_a_function_implementing_both_methods_runs_on_batch():
    class Both(UDFUnpacked):
        def __init__(self):
            super().__init__(("total", Int64))

        def on_batch(self, a, b):
            return pc.add(a, b)

        def on_element(self, a, b):
            raise AssertionError("on_element must not run")

    frame = _frame({"a": [1, 2], "b": [10, 20]}).udf(["a", "b"], Both())

    assert _rows(frame)["total"] == [11, 22]


def test_a_subclass_that_skips_super_init_fails_when_applied():
    class Uninitialized(UDF):
        def __init__(self):
            pass

        def on_batch(self, arrays):
            return arrays

    with pytest.raises(UDFDefinitionException) as error:
        _frame({"a": [1]}).udf("a", Uninitialized())

    assert error.value.api_error is UDFErrorCode.UDF_4


def test_an_output_system_column_is_rejected_when_applied():
    with pytest.raises(SystemColumnWriteError):
        _frame({"a": [1]}).udf("a", Echo(("$td.id", Int64), lambda arrays: arrays))


@pytest.mark.parametrize(
    "outputs, result, code",
    [
        (
            [("x", Int64), ("y", Int64)],
            lambda arrays: arrays[0],
            UDFErrorCode.UDF_10,
        ),
        (("x", Int64), lambda arrays: [arrays[0], arrays[0]], UDFErrorCode.UDF_10),
        (("x", Int64), lambda arrays: [arrays[0].to_pylist()], UDFErrorCode.UDF_10),
        (("x", Int64), lambda arrays: [arrays[0].slice(1)], UDFErrorCode.UDF_11),
        (
            ("x", Int64),
            lambda arrays: [pa.array(["a"] * len(arrays[0]))],
            UDFErrorCode.UDF_12,
        ),
    ],
)
def test_output_that_breaks_the_declaration_fails_when_data_is_processed(
    outputs, result, code
):
    frame = _frame({"a": [1, 2]}).udf("a", Echo(outputs, result))

    with pytest.raises(UDFOutputException) as error:
        _rows(frame)

    assert error.value.api_error is code


@pytest.mark.parametrize(
    "result, code",
    [
        ([1, 2], UDFErrorCode.UDF_10),
        (["text"], UDFErrorCode.UDF_12),
    ],
)
def test_on_element_output_that_breaks_the_declaration_fails(result, code):
    class Fixed(UDF):
        def __init__(self):
            super().__init__(("x", Int64))

        def on_element(self, values):
            return result

    with pytest.raises(UDFOutputException) as error:
        _rows(_frame({"a": [1]}).udf("a", Fixed()))

    assert error.value.api_error is code


class Counted(UDF):
    def __init__(self, width):
        super().__init__([(f"out_{index}", Int64) for index in range(width)])
        self.calls = 0

    def on_batch(self, arrays):
        self.calls += 1
        return [arrays[0]] * len(self.columns())


@pytest.mark.parametrize(
    "materialize",
    [
        lambda frame, path: frame._to_dict(),
        lambda frame, path: frame._to_polars_df(),
        lambda frame, path: frame._to_pandas(),
        lambda frame, path: frame._sink_native(path),
    ],
    ids=["to_dict", "to_polars_df", "to_pandas", "sink"],
)
def test_the_function_runs_once_per_batch_whatever_its_outputs(tmp_path, materialize):
    frame = TableFrame.from_polars(pl.LazyFrame({"a": [1, 2, 3]}))
    one, three = Counted(1), Counted(3)

    materialize(frame.udf("a", one), tmp_path / "one.parquet")
    materialize(frame.udf("a", three), tmp_path / "three.parquet")

    assert one.calls >= 1
    assert three.calls == one.calls


def test_an_error_raised_while_sinking_keeps_its_code(tmp_path):
    function = Echo(("x", Int64), lambda arrays: [arrays[0].slice(1)])
    frame = _frame({"a": [1, 2]}).udf("a", function)

    with pytest.raises(UDFOutputException) as error:
        frame._sink_native(tmp_path / "out.parquet")

    assert error.value.api_error is UDFErrorCode.UDF_11


def test_an_empty_frame_gets_empty_output_columns():
    schema = {"a": pl.Int64, "b": pl.Int64}
    frame = TableFrame.from_polars(pl.LazyFrame(schema=schema))

    assert _rows(frame.udf(["a", "b"], Total())) == {"a": [], "b": [], "total": []}


def test_nulls_reach_the_function_and_come_back():
    class Keep(UDF):
        def __init__(self):
            super().__init__([("on_batch", Int64)])

        def on_batch(self, arrays):
            return [arrays[0]]

    class KeepRow(UDF):
        def __init__(self):
            super().__init__([("on_element", Int64)])

        def on_element(self, values):
            return [values[0]]

    frame = _frame({"a": [1, None, 3]}).udf("a", Keep()).udf("a", KeepRow())

    assert _rows(frame) == {
        "a": [1, None, 3],
        "on_batch": [1, None, 3],
        "on_element": [1, None, 3],
    }


def test_nested_and_temporal_types_round_trip_through_arrow():
    data = {
        "items": [["x", "y"], [], None],
        "point": [{"x": 1, "y": 2.5}, None, {"x": 3, "y": None}],
        "when": [dt.datetime(2026, 1, 15, 10, 30), None, dt.datetime(2026, 3, 5)],
    }
    frame = TableFrame.from_polars(pl.LazyFrame(data))
    types = frame._to_polars_df().schema

    class Identity(UDF):
        def __init__(self):
            super().__init__([(f"{name}_copy", dtype) for name, dtype in types.items()])

        def on_batch(self, arrays):
            return list(arrays)

    class IdentityRow(UDFUnpacked):
        def __init__(self):
            super().__init__([(f"{name}_row", dtype) for name, dtype in types.items()])

        def on_element(self, *values):
            return list(values)

    names = list(types)
    rows = _rows(frame.udf(names, Identity()).udf(names, IdentityRow()))

    for name in names:
        assert rows[f"{name}_copy"] == data[name]
        assert rows[f"{name}_row"] == data[name]


def test_filters_before_and_after_the_function_keep_the_right_rows():
    frame = (
        _frame({"a": list(range(10)), "b": list(range(10, 20))})
        .filter(col("a") > 5)
        .udf(["a", "b"], Total())
        .filter(col("total") < 24)
    )

    assert _rows(frame) == {"a": [6], "b": [16], "total": [22]}


def test_a_decorated_function_is_a_udf():
    @on_batch(("total", Int64))
    def total(a, b):
        return pc.add(a, b)

    @on_element([("ratio", Float64), ("label", String)])
    def ratio(numerator, denominator):
        return [numerator / denominator, f"{numerator}/{denominator}"]

    frame = _frame({"a": [1, 3], "b": [2, 4]}).udf(["a", "b"], total)

    assert isinstance(total, UDF)
    renamed = ratio.with_names(["share", "fraction"])
    assert _rows(frame.udf(["a", "b"], renamed)) == {
        "a": [1, 3],
        "b": [2, 4],
        "total": [3, 7],
        "share": [0.5, 0.75],
        "fraction": ["1/2", "3/4"],
    }


def test_inline_lambdas_receive_unpacked_or_packed_inputs():
    frame = (
        _frame({"a": [1, 3], "b": [2, 4]})
        .udf(["a", "b"], on_element(("sum", Int64))(lambda a, b: a + b))
        .udf(
            ["a", "b"],
            on_batch(("largest", Int64), signature="packed")(
                lambda arrays: pc.max_element_wise(*arrays)
            ),
        )
    )

    assert _rows(frame) == {"a": [1, 3], "b": [2, 4], "sum": [3, 7], "largest": [2, 4]}


def test_a_callable_object_is_accepted():
    class Scale:
        def __init__(self, factor):
            self.factor = factor

        def __call__(self, value):
            return value * self.factor

    frame = _frame({"a": [1, 2]}).udf("a", on_element(("tripled", Int64))(Scale(3)))

    assert _rows(frame)["tripled"] == [3, 6]


def test_a_single_output_may_be_returned_without_a_list():
    class Total(UDF):
        def __init__(self):
            super().__init__(("total", Int64))

        def on_batch(self, a, b):
            return pc.add(a, b)

        @property
        def signature(self):
            return "unpacked"

    frame = _frame({"a": [1], "b": [2]}).udf(["a", "b"], Total())

    assert _rows(frame)["total"] == [3]


def test_a_single_list_output_must_be_wrapped_in_a_list():
    wrapped = on_element(("pair", List(Int64)))(lambda a: [[a, a]])
    bare = on_element(("pair", List(Int64)))(lambda a: [a, a])

    assert _rows(_frame({"a": [1]}).udf("a", wrapped))["pair"] == [[1, 1]]
    with pytest.raises(UDFOutputException) as error:
        _rows(_frame({"a": [1]}).udf("a", bare))
    assert error.value.api_error is UDFErrorCode.UDF_10


def test_errors_name_the_decorated_function():
    @on_batch(("x", Int64))
    def shortened(a):
        return a.slice(1)

    with pytest.raises(UDFOutputException, match="shortened"):
        _rows(_frame({"a": [1, 2]}).udf("a", shortened))


@pytest.mark.parametrize(
    "build, code",
    [
        (lambda: on_element(("x", Int64))(42), UDFErrorCode.UDF_14),
        (
            lambda: on_element(("x", Int64), signature="spread")(lambda a: a),
            UDFErrorCode.UDF_8,
        ),
        (lambda: on_batch([])(lambda a: a), UDFErrorCode.UDF_5),
        (
            lambda: _frame({"a": [1]}).udf(
                ["a", col("a") + 1], on_element(("x", Int64))(lambda a, b: a)
            ),
            UDFErrorCode.UDF_13,
        ),
    ],
)
def test_a_function_defined_wrongly_fails_when_decorated_or_applied(build, code):
    with pytest.raises(UDFDefinitionException) as error:
        build()

    assert error.value.api_error is code
