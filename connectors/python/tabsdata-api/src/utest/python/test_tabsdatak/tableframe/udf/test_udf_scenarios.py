#
# Copyright 2026 Tabsdata Inc.
#

from __future__ import annotations

from collections.abc import Callable

import polars as pl
import pyarrow as pa
import pyarrow.compute as pc
import pytest

from tabsdatak.tableframe import TableFrame
from tabsdatak.tableframe.datatypes import Boolean, Float64, Int64, String
from tabsdatak.tableframe.functions import col, concat, lit
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
    UDFException,
)

pytestmark = pytest.mark.tableframe

PENGUINS = {
    "species": ["Adelie", "Gentoo", "Chinstrap", "Adelie", "Gentoo"],
    "bill_length_mm": [39.1, 46.1, 46.5, None, 50.0],
    "bill_depth_mm": [18.7, 13.2, 17.9, 18.0, 16.3],
    "flipper_length_mm": [181, 211, 192, 190, 230],
    "body_mass_g": [3750, 4500, 3500, 3700, 5700],
}

STYLES = [
    (style, mode, signature)
    for style in ("class", "function")
    for mode in ("batch", "element")
    for signature in ("packed", "unpacked")
]


def build(
    style: str,
    mode: str,
    signature: str,
    outputs,
    on_row: Callable[..., list],
    on_arrays: Callable[..., list],
) -> UDF:
    """The same function written in each of the eight ways a user can write
    it: a subclass or a decorated function, over batches or rows, receiving
    its inputs packed in a list or unpacked."""
    body = on_arrays if mode == "batch" else on_row

    def call(*arguments):
        return body(*(arguments[0] if signature == "packed" else arguments))

    if style == "function":
        decorator = on_batch if mode == "batch" else on_element
        return decorator(outputs, signature=signature)(call)
    base = UDFPacked if signature == "packed" else UDFUnpacked

    def method(self, *arguments):
        return call(*arguments)

    def initialize(self):
        base.__init__(self, outputs)

    name = "on_batch" if mode == "batch" else "on_element"
    return type("Scenario", (base,), {"__init__": initialize, name: method})()


def _penguins() -> TableFrame:
    return TableFrame.from_dict(PENGUINS)


def _rows(frame: TableFrame) -> dict:
    return frame._to_dict()


def _rounded(values: list) -> list:
    return [None if value is None else round(value, 6) for value in values]


@pytest.mark.parametrize("style, mode, signature", STYLES)
@pytest.mark.parametrize(
    "column, factor, expected",
    [
        ("bill_length_mm", 10, [391.0, 461.0, 465.0, None, 500.0]),
        ("bill_depth_mm", -2, [-37.4, -26.4, -35.8, -36.0, -32.6]),
        ("body_mass_g", 0.5, [1875.0, 2250.0, 1750.0, 1850.0, 2850.0]),
    ],
)
def test_a_scaler_multiplies_a_column(style, mode, signature, column, factor, expected):
    scaler = build(
        style,
        mode,
        signature,
        ("scaled", Float64),
        lambda value: [None if value is None else value * factor],
        lambda values: [pc.multiply(values.cast(pa.float64()), factor)],
    )

    scaled = _rows(_penguins().udf(column, scaler))["scaled"]

    assert _rounded(scaled) == _rounded(expected)


@pytest.mark.parametrize("style, mode, signature", STYLES)
def test_a_categorizer_buckets_a_column_keeping_nulls(style, mode, signature):
    def bucket(length):
        if length is None:
            return None
        return "short" if length < 45 else "long"

    categorizer = build(
        style,
        mode,
        signature,
        ("bill_size", String),
        lambda length: [bucket(length)],
        lambda lengths: [pa.array([bucket(value) for value in lengths.to_pylist()])],
    )

    sizes = _rows(_penguins().udf("bill_length_mm", categorizer))["bill_size"]

    assert sizes == ["short", "long", "long", None, "long"]


@pytest.mark.parametrize("style, mode, signature", STYLES)
def test_several_features_become_several_columns(style, mode, signature):
    def row(depth, flipper, mass):
        return [depth * flipper, mass / flipper, mass > 4000]

    def arrays(depth, flipper, mass):
        return [
            pc.multiply(depth, flipper.cast(pa.float64())),
            pc.divide(mass.cast(pa.float64()), flipper.cast(pa.float64())),
            pc.greater(mass, 4000),
        ]

    features = build(
        style,
        mode,
        signature,
        [("area", Float64), ("density", Float64), ("heavy", Boolean)],
        row,
        arrays,
    )

    rows = _rows(
        _penguins().udf(
            ["bill_depth_mm", "flipper_length_mm", "body_mass_g"], features
        )
    )

    assert rows["area"] == pytest.approx([3384.7, 2785.2, 3436.8, 3420.0, 3749.0])
    assert rows["density"] == pytest.approx(
        [3750 / 181, 4500 / 211, 3500 / 192, 3700 / 190, 5700 / 230]
    )
    assert rows["heavy"] == [False, True, False, False, True]
    assert list(rows)[:5] == list(PENGUINS)


@pytest.mark.parametrize("style, mode, signature", STYLES)
def test_renamed_outputs_reach_the_frame(style, mode, signature):
    pair = build(
        style,
        mode,
        signature,
        [("low", Int64), ("high", Int64)],
        lambda flipper, mass: [min(flipper, mass), max(flipper, mass)],
        lambda flipper, mass: [
            pc.min_element_wise(flipper, mass),
            pc.max_element_wise(flipper, mass),
        ],
    )

    renamed = pair.with_names(["smallest", "largest"])
    rows = _rows(_penguins().udf(["flipper_length_mm", "body_mass_g"], renamed))

    assert renamed.columns() == [("smallest", Int64), ("largest", Int64)]
    assert rows["smallest"] == [181, 211, 192, 190, 230]
    assert rows["largest"] == PENGUINS["body_mass_g"]
    assert pair.columns() == [("low", Int64), ("high", Int64)]


@pytest.mark.parametrize("style, mode, signature", STYLES)
def test_functions_compose_with_other_operations(style, mode, signature):
    ratio = build(
        style,
        mode,
        signature,
        ("ratio", Float64),
        lambda mass, flipper: [mass / flipper],
        lambda mass, flipper: [
            pc.divide(mass.cast(pa.float64()), flipper.cast(pa.float64()))
        ],
    )
    label = build(
        style,
        mode,
        signature,
        ("label", String),
        lambda species, ratio: [f"{species}:{ratio:.0f}"],
        lambda species, ratio: [
            pc.binary_join_element_wise(
                species.cast(pa.string()),
                pc.cast(pc.round(ratio, 0).cast(pa.int64()), pa.string()),
                ":",
            )
        ],
    )

    frame = (
        _penguins()
        .filter(col("body_mass_g") > 3600)
        .udf(["body_mass_g", "flipper_length_mm"], ratio)
        .with_columns(doubled=col("ratio") * 2)
        .udf(["species", "ratio"], label)
        .group_by("species")
        .agg(col("doubled").sum(), col("label").first())
        .sort("species")
    )

    rows = _rows(frame)
    assert rows["species"] == ["Adelie", "Gentoo"]
    assert rows["doubled"] == pytest.approx(
        [2 * (3750 / 181 + 3700 / 190), 2 * (4500 / 211 + 5700 / 230)]
    )
    assert rows["label"] == ["Adelie:21", "Gentoo:21"]


def test_frames_from_every_style_concatenate():
    frames = [
        _penguins()
        .select("species", "body_mass_g")
        .udf(
            "body_mass_g",
            build(
                style,
                mode,
                signature,
                ("kg", Float64),
                lambda mass: [mass / 1000],
                lambda masses: [pc.divide(masses.cast(pa.float64()), 1000)],
            ),
        )
        .with_columns(style=lit(f"{style}-{mode}-{signature}"))
        for style, mode, signature in STYLES
    ]

    rows = _rows(concat(frames))

    assert len(rows["kg"]) == len(STYLES) * len(PENGUINS["species"])
    assert set(rows["style"]) == {"-".join(style) for style in STYLES}
    assert rows["kg"][:5] == [3.75, 4.5, 3.5, 3.7, 5.7]


@pytest.mark.parametrize("mode", ["batch", "element"])
def test_a_function_keeps_state_that_does_not_depend_on_row_order(mode):
    class Offset(UDF):
        def __init__(self, offset):
            super().__init__(("shifted", Int64))
            self.offset = offset
            self.calls = 0
            self.rows = 0

        def on_batch(self, values):
            self.calls += 1
            self.rows += len(values[0])
            return pc.add(values[0], self.offset)

    class OffsetRow(UDF):
        def __init__(self, offset):
            super().__init__(("shifted", Int64))
            self.offset = offset
            self.calls = 0
            self.rows = 0

        def on_element(self, values):
            self.calls += 1
            self.rows += 1
            return values[0] + self.offset

    rows = 200_000
    function = Offset(1000) if mode == "batch" else OffsetRow(1000)
    frame = TableFrame.from_polars(pl.LazyFrame({"a": range(rows)}))

    shifted = frame.udf("a", function)._to_polars_df()["shifted"]

    assert shifted.sum() == sum(range(rows)) + 1000 * rows
    assert function.rows == rows
    assert function.calls == rows if mode == "element" else function.calls >= 1


def test_on_element_streams_a_large_frame_row_by_row(tmp_path):
    rows = 1_000_000
    increment = on_element(("incremented", Int64))(lambda value: value + 1)
    frame = TableFrame.from_polars(pl.LazyFrame({"values": range(rows)}))
    path = tmp_path / "out.parquet"

    frame.udf("values", increment)._sink_public(path)

    stored = pl.read_parquet(path)
    assert stored.height == rows
    assert stored["incremented"].sum() == sum(range(rows)) + rows


def test_output_names_may_use_any_characters():
    name = "σpecïalᚣ🧪列👀💩"
    doubled = on_element((name, Int64))(lambda value: value * 2)

    assert _rows(_penguins().udf("body_mass_g", doubled))[name] == [
        7500,
        9000,
        7000,
        7400,
        11400,
    ]


@pytest.mark.parametrize("width", [1, 2, 3])
@pytest.mark.parametrize(
    "dtype, convert",
    [
        (Int64, lambda total: total),
        (Float64, lambda total: total / 2),
        (String, lambda total: f"#{total}"),
        (Boolean, lambda total: total % 2 == 0),
    ],
    ids=["Int64", "Float64", "String", "Boolean"],
)
@pytest.mark.parametrize("mode", ["batch", "element"])
def test_any_number_of_inputs_produces_any_output_type(width, dtype, convert, mode):
    data = {f"c{index}": [index + 1, index + 2] for index in range(width)}
    totals = [sum(data[name][row] for name in data) for row in range(2)]

    def row(*values):
        return convert(sum(values))

    def arrays(*values):
        return pa.array([row(*items) for items in zip(*(v.to_pylist() for v in values))])

    function = (on_batch if mode == "batch" else on_element)(("out", dtype))(
        arrays if mode == "batch" else row
    )

    frame = TableFrame.from_dict(data).udf(list(data), function)

    assert _rows(frame)["out"] == [convert(total) for total in totals]


def test_system_and_connector_columns_can_be_read_and_connector_columns_written():
    frame = TableFrame.from_dict({"a": [1, 2]})
    noted = frame.udf(
        ["a", "$td.id"],
        on_element(("@td.origin.note", String))(lambda a, identifier: f"{a}"),
    )

    columns = noted._to_polars_df(drop_system_columns=False).columns

    assert "@td.origin.note" in columns
    assert "$td.id" in columns


@pytest.mark.parametrize("mode", ["batch", "element"])
@pytest.mark.parametrize(
    "error",
    [ValueError("plain"), KeyError("missing")],
    ids=["ValueError", "KeyError"],
)
def test_an_error_raised_by_the_function_reaches_the_caller(mode, error):
    def fail(*_):
        raise error

    function = (on_batch if mode == "batch" else on_element)(("x", Int64))(fail)

    with pytest.raises(UDFException) as raised:
        _rows(TableFrame.from_dict({"a": [1]}).udf("a", function))

    assert raised.value.api_error is UDFErrorCode.UDF_0
    assert type(error).__name__ in str(raised.value)
    assert str(error) in str(raised.value)


def test_an_error_whose_constructor_needs_arguments_is_not_lost():
    class Rejected(Exception):
        def __init__(self, code, detail):
            super().__init__(f"{code}: {detail}")

    def fail(value):
        raise Rejected(7, "not accepted")

    with pytest.raises(UDFException) as raised:
        _rows(TableFrame.from_dict({"a": [1]}).udf("a", on_element(("x", Int64))(fail)))

    assert raised.value.api_error is UDFErrorCode.UDF_0
    assert "Rejected" in str(raised.value)
    assert "7: not accepted" in str(raised.value)


@pytest.mark.parametrize(
    "function",
    [lambda value: [value], "not a function", None],
    ids=["lambda", "string", "None"],
)
def test_anything_but_a_udf_is_rejected_when_applied(function):
    with pytest.raises(UDFDefinitionException) as raised:
        TableFrame.from_dict({"a": [1]}).udf("a", function)

    assert raised.value.api_error is UDFErrorCode.UDF_15


def test_a_column_given_twice_is_rejected_when_applied():
    function = on_element(("x", Int64))(lambda a, b: a + b)

    with pytest.raises(UDFDefinitionException) as raised:
        TableFrame.from_dict({"a": [1]}).udf(["a", "a"], function)

    assert raised.value.api_error is UDFErrorCode.UDF_13


def test_a_missing_input_column_is_rejected_when_applied():
    function = on_element(("x", Int64))(lambda value: value)

    with pytest.raises(UDFDefinitionException) as raised:
        TableFrame.from_dict({"a": [1]}).udf(["a", col("missing") * 2], function)

    assert raised.value.api_error is UDFErrorCode.UDF_16
    assert "missing" in str(raised.value)
