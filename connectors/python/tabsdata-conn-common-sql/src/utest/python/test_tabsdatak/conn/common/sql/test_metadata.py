#
# Copyright 2026 Tabsdata Inc.
#

from __future__ import annotations

import json
from datetime import date, datetime, timezone
from decimal import Decimal
from pathlib import Path

import pytest

from tabsdatak.conn.common.sql.constants import (
    SQL_META_INITIAL_VALUES,
    SQL_META_QUERY,
)
from tabsdatak.conn.common.sql._metadata import (
    sql_metadata,
    to_sql_table_file_inputs,
)
from tabsdatak.spi import TableFileInput

# ---------------------------------------------------------------------------
# sql_metadata
# ---------------------------------------------------------------------------


def test_carries_the_query_verbatim():
    metadata = sql_metadata("SELECT * FROM t WHERE id > :id", {"id": 7})
    assert metadata[SQL_META_QUERY] == "SELECT * FROM t WHERE id > :id"


def test_initial_values_are_json_serialized():
    metadata = sql_metadata("SELECT 1", {"id": 7, "name": "ana", "on": True})
    assert json.loads(metadata[SQL_META_INITIAL_VALUES]) == {
        "id": 7,
        "name": "ana",
        "on": True,
    }


def test_initial_values_keys_are_sorted():
    # Equal bind dicts must serialize equally regardless of insertion order.
    first = sql_metadata("SELECT 1", {"b": 1, "a": 2})
    second = sql_metadata("SELECT 1", {"a": 2, "b": 1})
    assert first[SQL_META_INITIAL_VALUES] == second[SQL_META_INITIAL_VALUES]
    assert first[SQL_META_INITIAL_VALUES] == '{"a": 2, "b": 1}'


@pytest.mark.parametrize("values", [None, {}])
def test_no_binds_serializes_as_an_empty_object(values):
    # Both entries are always present, so the metadata a read reports
    # does not change shape between a bound and an unbound query.
    metadata = sql_metadata("SELECT 1", values)
    assert set(metadata) == {SQL_META_QUERY, SQL_META_INITIAL_VALUES}
    assert metadata[SQL_META_INITIAL_VALUES] == "{}"


def test_non_json_bind_values_serialize_as_strings():
    metadata = sql_metadata(
        "SELECT 1",
        {
            "amount": Decimal("10.50"),
            "day": date(2026, 8, 5),
            "at": datetime(2026, 8, 5, 12, 30, tzinfo=timezone.utc),
        },
    )
    assert json.loads(metadata[SQL_META_INITIAL_VALUES]) == {
        "amount": "10.50",
        "day": "2026-08-05",
        "at": "2026-08-05T12:30:00+00:00",
    }


def test_falsy_bind_values_survive():
    metadata = sql_metadata("SELECT 1", {"zero": 0, "off": False, "empty": ""})
    assert json.loads(metadata[SQL_META_INITIAL_VALUES]) == {
        "zero": 0,
        "off": False,
        "empty": "",
    }


# ---------------------------------------------------------------------------
# to_sql_table_file_inputs
# ---------------------------------------------------------------------------


def test_every_file_of_a_slot_carries_its_query():
    files = [
        [Path("slot0_chunk0.parquet"), Path("slot0_chunk1.parquet")],
        [Path("slot1_chunk0.parquet")],
    ]
    inputs = to_sql_table_file_inputs(files, ["SELECT 1", "SELECT 2"], {"id": 7})

    assert [[i.file for i in slot] for slot in inputs] == files
    assert all(isinstance(i, TableFileInput) for slot in inputs for i in slot)
    assert [i.metadata[SQL_META_QUERY] for i in inputs[0]] == [
        "SELECT 1",
        "SELECT 1",
    ]
    assert [i.metadata[SQL_META_QUERY] for i in inputs[1]] == ["SELECT 2"]


def test_initial_values_are_shared_by_every_slot():
    inputs = to_sql_table_file_inputs(
        [[Path("a.parquet")], [Path("b.parquet")]],
        ["SELECT 1", "SELECT 2"],
        {"id": 7},
    )
    serialized = {i.metadata[SQL_META_INITIAL_VALUES] for slot in inputs for i in slot}
    assert serialized == {'{"id": 7}'}


def test_empty_slot_yields_no_inputs():
    inputs = to_sql_table_file_inputs([[]], ["SELECT 1"], None)
    assert inputs == [[]]


def test_no_queries_yields_no_slots():
    assert to_sql_table_file_inputs([], [], None) == []


@pytest.mark.parametrize(
    "files, queries",
    [
        ([[Path("a.parquet")]], ["SELECT 1", "SELECT 2"]),
        ([[Path("a.parquet")], [Path("b.parquet")]], ["SELECT 1"]),
    ],
)
def test_slot_query_length_mismatch_raises(files, queries):
    # A read that does not produce one slot per query broke its
    # contract -- fail loudly instead of dropping files or queries.
    with pytest.raises(ValueError):
        to_sql_table_file_inputs(files, queries, None)
