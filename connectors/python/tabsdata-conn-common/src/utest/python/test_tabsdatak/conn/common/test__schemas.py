#
# Copyright 2026 Tabsdata Inc.
#

from __future__ import annotations

import json

import polars as pl
import pytest

# noinspection PyProtectedMember
from tabsdatak.conn.common._schemas import KeptSchemas, SlotSchemas, schema_key
from tabsdatak.conn.common.error import ConnStateException
from tabsdatak.spi import (
    CsvParseOptions,
    FrameMetadata,
    LazyFrameInput,
    SourceFormat,
)

SCHEMA = pl.Schema({
    "id": pl.Int64,
    "name": pl.String,
    "at": pl.Datetime("us", "UTC"),
    "tags": pl.List(pl.String),
    "point": pl.Struct({"y": pl.Float64, "x": pl.Float64}),
})


class FakeCtx:
    def __init__(self, attributes: dict | None = None) -> None:
        self.attributes = dict(attributes or {})

    def get_attr(self, attr):
        return self.attributes.get(attr)

    def set_attr(self, attr, val):
        self.attributes[attr] = val


def kept(state: dict | None = None) -> tuple[SlotSchemas, dict]:
    state = {} if state is None else state
    return SlotSchemas(state.get, state.__setitem__), state


def test_a_remembered_schema_reads_back_as_it_was():
    schemas, _ = kept()
    schemas.remember(0, SCHEMA)
    last = schemas.last(0)
    assert last == SCHEMA
    assert list(last) == list(SCHEMA)
    assert list(last["point"].to_schema()) == ["y", "x"]


def test_each_slot_keeps_its_own_schema():
    schemas, state = kept()
    schemas.remember(0, pl.Schema({"a": pl.Int64}))
    schemas.remember("orders", pl.Schema({"b": pl.String}))
    assert schemas.last(0) == pl.Schema({"a": pl.Int64})
    assert schemas.last("orders") == pl.Schema({"b": pl.String})
    assert set(state) == {schema_key(0), schema_key("orders")}


def test_the_schema_is_kept_under_its_slot_key_as_json():
    schemas, state = kept()
    schemas.remember(2, pl.Schema({"a": pl.Int64, "b": pl.Date}))
    assert schema_key(2) == "@td.schema.2"
    assert json.loads(state["@td.schema.2"]) == {"a": "Int64", "b": "Date"}


def test_the_last_schema_remembered_is_the_one_kept():
    schemas, _ = kept()
    schemas.remember(0, pl.Schema({"a": pl.Int64}))
    schemas.remember(0, pl.Schema({"a": pl.Int64, "b": pl.String}))
    assert schemas.last(0) == pl.Schema({"a": pl.Int64, "b": pl.String})


def test_a_slot_that_served_no_schema_has_none():
    schemas, _ = kept()
    assert schemas.last(0) is None


@pytest.mark.parametrize("schema", [None, pl.Schema()], ids=["none", "no columns"])
def test_no_schema_leaves_the_last_one_as_it_was(schema):
    schemas, state = kept()
    schemas.remember(0, schema)
    assert state == {}
    schemas.remember(0, SCHEMA)
    schemas.remember(0, schema)
    assert schemas.last(0) == SCHEMA


def test_a_kept_schema_without_columns_reads_as_none():
    schemas, _ = kept({schema_key(0): "{}"})
    assert schemas.last(0) is None


def test_a_schema_of_a_type_that_cannot_be_kept_is_refused():
    schemas, state = kept()
    with pytest.raises(ConnStateException, match="slot 0"):
        schemas.remember(0, pl.Schema({"o": pl.Categorical()}))
    assert state == {}


@pytest.mark.parametrize(
    "stored",
    [7, "not json", "[]", '{"a": "Elephant"}'],
    ids=["not a string", "not json", "not an object", "unknown type"],
)
def test_a_kept_schema_that_cannot_be_read_is_refused(stored):
    schemas, _ = kept({schema_key(0): stored})
    with pytest.raises(ConnStateException, match="slot 0"):
        schemas.last(0)


def test_over_per_flow_attributes():
    ctx = FakeCtx()
    SlotSchemas.of(ctx).remember(1, SCHEMA)
    assert set(ctx.attributes) == {schema_key(1)}
    assert SlotSchemas.of(FakeCtx(ctx.attributes)).last(1) == SCHEMA


# --- served ---------------------------------------------------------------------


def test_a_frame_is_remembered_with_its_own_schema():
    schemas, _ = kept()
    table = LazyFrameInput(pl.LazyFrame({"a": [1]}))
    assert schemas.served(0, table) is table
    assert schemas.last(0) == pl.Schema({"a": pl.Int64})


def test_a_frame_described_with_a_schema_is_remembered_with_that_schema():
    schemas, _ = kept()
    metadata = FrameMetadata(
        pl.Schema({"a": pl.Int64}),
        format=SourceFormat.CSV,
        typed=False,
    )
    schemas.served(0, LazyFrameInput(pl.LazyFrame({"a": ["1"]}), metadata))
    assert schemas.last(0) == pl.Schema({"a": pl.Int64})


def test_metadata_learnt_while_streaming_is_remembered_once_learnt():
    schemas, _ = kept()
    learnt = FrameMetadata(pl.Schema({"a": pl.Date}), format=SourceFormat.CSV)
    served = schemas.served(
        0, LazyFrameInput(pl.LazyFrame({"a": ["2026-01-01"]}), lambda: learnt)
    )
    assert schemas.last(0) is None
    assert served.metadata() is learnt
    assert schemas.last(0) == pl.Schema({"a": pl.Date})


def test_a_frame_without_columns_leaves_the_last_schema():
    schemas, _ = kept()
    schemas.remember(0, SCHEMA)
    schemas.served(0, LazyFrameInput(pl.LazyFrame()))
    assert schemas.last(0) == SCHEMA


# --- empty ----------------------------------------------------------------------


@pytest.mark.parametrize(
    "state", [{}, {schema_key(0): "{}"}], ids=["no schema", "no columns"]
)
def test_a_slot_without_a_schema_gives_no_frame(state):
    schemas, _ = kept(state)
    assert schemas.empty(0, FrameMetadata(columns={"@path": "a"})) is None


def test_an_empty_frame_takes_the_last_schema():
    schemas, _ = kept()
    schemas.remember(0, SCHEMA)
    table = schemas.empty(0)
    collected = table.frame.collect()
    assert collected.schema == SCHEMA
    assert collected.height == 0
    assert table.metadata is None


def test_an_empty_frame_keeps_what_its_metadata_adds_and_is_typed():
    schemas, _ = kept()
    schemas.remember(0, pl.Schema({"a": pl.Int64, "b": pl.String}))
    metadata = FrameMetadata(
        pl.Schema({"x": pl.String}),
        format=SourceFormat.CSV,
        typed=False,
        parsing=CsvParseOptions(decimal_comma=True),
        columns={"@path": "s3://bucket/in"},
    )
    table = schemas.empty(0, metadata)
    assert table.metadata == FrameMetadata(columns={"@path": "s3://bucket/in"})


# --- kept in a state of its own -------------------------------------------------


def test_kept_schemas_are_read_from_the_state_once():
    state = {"offset": 3, schema_key("t"): '{"a": "Int64"}'}
    schemas = KeptSchemas(state)
    state[schema_key("t")] = '{"b": "String"}'
    assert schemas.kept == {schema_key("t"): '{"a": "Int64"}'}
    assert schemas.last("t") == pl.Schema({"a": pl.Int64})


def test_no_state_keeps_no_schema():
    assert KeptSchemas(None).last("t") is None


def test_a_state_is_carried_with_the_schemas_kept():
    schemas = KeptSchemas({"offset": 3})
    schemas.remember("t", pl.Schema({"a": pl.Int64}))
    assert schemas.carried({"offset": 4}) == {
        "offset": 4,
        schema_key("t"): '{"a": "Int64"}',
    }


def test_a_state_carries_the_schemas_as_they_were_when_carried():
    schemas = KeptSchemas()
    schemas.remember("t", pl.Schema({"a": pl.Int64}))
    carried = schemas.carried(lambda: None)
    schemas.remember("t", pl.Schema({"b": pl.String}))
    assert carried() == {schema_key("t"): '{"a": "Int64"}'}


@pytest.mark.parametrize(
    "returned", [None, {"session": "s"}], ids=["nothing", "a state"]
)
def test_a_commit_of_a_source_owned_offset_returns_the_schemas_kept(returned):
    commits: list[str] = []
    schemas = KeptSchemas()
    schemas.remember("t", pl.Schema({"a": pl.Int64}))

    def commit():
        commits.append("committed")
        return returned

    carried = schemas.carried(commit)
    assert commits == []
    assert carried() == {**(returned or {}), schema_key("t"): '{"a": "Int64"}'}
    assert commits == ["committed"]


def test_without_schemas_kept_a_state_is_carried_as_it_is():
    def commit():
        return None

    schemas = KeptSchemas({"offset": 3})
    state = {"offset": 4}
    assert schemas.carried(state) is state
    assert schemas.carried(commit) is commit
