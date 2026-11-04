#
# Copyright 2026 Tabsdata Inc.
#

from __future__ import annotations

import polars as pl
import pytest

# noinspection PyProtectedMember
from tabsdatak.conn.common.file._read._ndjson import AutoNdjson, NdjsonLineError

# noinspection PyProtectedMember
from tabsdatak.conn.common.file._read._types import TypeConflictError
from tabsdatak.spi import Layout, SourceFormat
from test_tabsdatak.conn.common.file.landed import Source, land

pytestmark = [pytest.mark.unit]

BLOCK = 8


def read(content: bytes, *, block_size: int = BLOCK):
    return AutoNdjson(block_size=block_size).read(Source(content))


def test_objects_land_as_their_lines(tmp_path):
    content = b'{"a": 1}\n{"a": 2, "b": "x"}\n'
    landed = land(read(content), tmp_path)
    assert landed.metadata.layout is Layout.SERIALIZED
    assert landed.metadata.format is SourceFormat.NDJSON
    assert not landed.metadata.typed
    assert landed.data["line"].to_list() == ['{"a": 1}', '{"a": 2, "b": "x"}']


def test_a_field_first_seen_on_the_last_line_is_kept(tmp_path):
    content = b'{"a": 1}\n' * 20 + b'{"a": 2, "late": true}\n'
    landed = land(read(content), tmp_path)
    assert landed.schema == pl.Schema({"a": pl.Int64, "late": pl.Boolean})
    assert landed.served["late"].to_list() == [None] * 20 + [True]


@pytest.mark.parametrize(
    "values, dtype, served",
    [
        (["1", '"x"'], pl.String, ["1", "x"]),
        (["1", "1.5"], pl.Float64, [1.0, 1.5]),
        (["1", "null"], pl.Int64, [1, None]),
        (["null", "null"], pl.String, [None, None]),
        (["true", "false"], pl.Boolean, [True, False]),
        (["true", "1"], pl.String, ["true", "1"]),
    ],
    ids=[
        "integer then text",
        "integer then float",
        "null",
        "only nulls",
        "booleans",
        "boolean then integer",
    ],
)
def test_a_field_gets_the_type_of_all_its_values(tmp_path, values, dtype, served):
    content = "".join(f'{{"v": {value}}}\n' for value in values).encode()
    landed = land(read(content, block_size=1), tmp_path)
    assert landed.schema == pl.Schema({"v": dtype})
    assert landed.served["v"].to_list() == served


def test_a_field_absent_from_a_line_is_null(tmp_path):
    landed = land(read(b'{"a": 1, "b": 2}\n{"b": 3}\n'), tmp_path)
    assert landed.served.to_dict(as_series=False) == {"a": [1, None], "b": [2, 3]}


def test_the_order_of_the_fields_does_not_matter(tmp_path):
    landed = land(read(b'{"a": 1, "b": 2}\n{"b": 3, "a": 4}\n'), tmp_path)
    assert landed.served.to_dict(as_series=False) == {"a": [1, 4], "b": [2, 3]}


def test_nested_objects_and_arrays_keep_their_structure(tmp_path):
    content = b'{"o": {"x": 1}, "l": [1]}\n{"o": {"y": "z"}, "l": [2.5]}\n'
    landed = land(read(content), tmp_path)
    assert landed.schema == pl.Schema({
        "o": pl.Struct({"x": pl.Int64, "y": pl.String}),
        "l": pl.List(pl.Float64),
    })
    assert landed.served.to_dicts() == [
        {"o": {"x": 1, "y": None}, "l": [1.0]},
        {"o": {"x": None, "y": "z"}, "l": [2.5]},
    ]


def test_a_field_holding_objects_and_scalars_is_refused(tmp_path):
    with pytest.raises(TypeConflictError, match="'o' holds both"):
        land(read(b'{"o": 1}\n' + b'{"o": {"x": 1}}\n', block_size=1), tmp_path)


def test_empty_lines_and_carriage_returns_are_skipped(tmp_path):
    landed = land(read(b'\n{"a": 1}\r\n\n  \n{"a": 2}\r\n\n'), tmp_path)
    assert landed.served["a"].to_list() == [1, 2]


@pytest.mark.parametrize(
    "line, reason",
    [
        (b"5", "it holds a number"),
        (b'"text"', "it holds a string"),
        (b"[1]", "it holds an array"),
        (b"null", "it holds null"),
        (b"true", "it holds a boolean"),
        (b"{bad", "Expecting property name"),
    ],
    ids=["number", "string", "array", "null", "boolean", "invalid"],
)
def test_a_line_that_is_not_one_json_object_fails_naming_it(tmp_path, line, reason):
    content = b'{"a": 1}\n\n{"a": 2}\n' + line + b"\n"
    with pytest.raises(
        NdjsonLineError,
        match=f"Line 4 is not a JSON object: {reason}",
    ):
        land(read(content, block_size=1 << 20), tmp_path)


def test_repeated_keys_keep_the_first_value(tmp_path):
    content = b'{"a": 1}\n{"a": 2, "a": 3}\n'
    landed = land(read(content, block_size=1 << 20), tmp_path)
    assert landed.served["a"].to_list() == [1, 2]


def test_a_file_without_objects_lands_nothing(tmp_path):
    landed = land(read(b"\n\n"), tmp_path)
    assert landed.data.height == 0
    assert landed.schema == pl.Schema()


def test_the_file_is_read_once_and_closed(tmp_path):
    source = Source(b'{"a": 1}\n')
    land(AutoNdjson(block_size=BLOCK).read(source), tmp_path)
    assert len(source.streams) == 1
    assert source.streams[0].closed
