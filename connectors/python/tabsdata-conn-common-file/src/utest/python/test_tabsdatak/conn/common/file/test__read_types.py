#
# Copyright 2026 Tabsdata Inc.
#

from __future__ import annotations

import polars as pl
import pytest

# noinspection PyProtectedMember
from tabsdatak.conn.common.file._read._types import Fold, TypeConflictError, merge

pytestmark = [pytest.mark.unit]


@pytest.mark.parametrize(
    "first, second, merged",
    [
        (pl.Int64(), pl.Int64(), pl.Int64()),
        (pl.Int64(), pl.Int128(), pl.Int128()),
        (pl.Int64(), pl.Float64(), pl.Float64()),
        (pl.Null(), pl.Date(), pl.Date()),
        (pl.Boolean(), pl.Int64(), pl.String()),
        (pl.Date(), pl.Datetime("us"), pl.String()),
        (pl.Int64(), pl.String(), pl.String()),
        (pl.List(pl.Int64), pl.List(pl.Float64), pl.List(pl.Float64)),
    ],
    ids=[
        "same",
        "wider integer",
        "integer and float",
        "null",
        "boolean and integer",
        "date and datetime",
        "integer and text",
        "lists",
    ],
)
def test_two_types_merge_into_the_one_serving_both(first, second, merged):
    assert merge("v", first, second) == merged
    assert merge("v", second, first) == merged


def test_objects_merge_their_fields_in_the_order_first_seen():
    first = pl.Struct({"a": pl.Int64, "b": pl.Int64})
    second = pl.Struct({"c": pl.String, "b": pl.Float64})
    assert merge("v", first, second) == pl.Struct(
        {"a": pl.Int64, "b": pl.Float64, "c": pl.String},
    )


@pytest.mark.parametrize(
    "first, second",
    [
        (pl.Struct({"a": pl.Int64}), pl.Int64),
        (pl.List(pl.Int64), pl.String),
        (pl.Struct({"a": pl.Int64}), pl.List(pl.Int64)),
    ],
    ids=["object and scalar", "array and scalar", "object and array"],
)
def test_a_nested_and_another_type_serve_nothing(first, second):
    with pytest.raises(TypeConflictError, match="'v' holds both"):
        merge("v", first, second)


def test_a_conflict_inside_an_object_names_the_field():
    with pytest.raises(TypeConflictError, match="'v.a' holds both"):
        merge("v", pl.Struct({"a": pl.Int64}), pl.Struct({"a": pl.List(pl.Int64)}))


def test_a_fold_keeps_the_column_order_it_is_given():
    fold = Fold()
    fold.observe({"b": pl.Int64()})
    fold.observe({"a": pl.Float64(), "b": pl.Float64()})
    assert fold.schema(["a", "b", "c"]) == pl.Schema(
        {"a": pl.Float64, "b": pl.Float64, "c": pl.String},
    )
    assert list(fold.schema()) == ["b", "a"]


def test_a_fold_types_what_only_held_nulls_as_strings():
    fold = Fold()
    fold.observe({"a": pl.Null(), "o": pl.Struct({"x": pl.Null()})})
    assert fold.schema() == pl.Schema({
        "a": pl.String,
        "o": pl.Struct({"x": pl.String}),
    })
