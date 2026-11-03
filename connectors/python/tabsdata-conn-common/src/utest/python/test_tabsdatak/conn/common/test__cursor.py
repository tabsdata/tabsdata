#
# Copyright 2026 Tabsdata Inc.
#

from __future__ import annotations

import base64
import json
from datetime import datetime, timedelta, timezone

import pytest

# noinspection PyProtectedMember
from tabsdatak.conn.common._cursor import CursorError, LastModifiedCursor, Taken

PATTERN = "stream-manifest-*.json"
EPOCH = datetime(1970, 1, 1, tzinfo=timezone.utc)
NOON = datetime(2026, 6, 16, 12, 0, 0, 123456, tzinfo=timezone.utc)
WHOLE_NOON = NOON.replace(microsecond=0)

RELEASED_SECONDS = (
    "eyJpbml0aWFsIjoiMTk3MC0wMS0wMVQwMDowMDowMFoiLCJlbnRyaWVzIjp7InN0cmVhbS1tYW5p"
    "ZmVzdC0qLmpzb24iOnsibGFzdG1vZCI6IjIwMjYtMDYtMTZUMTI6MDA6MDBaIiwibmFtZXMiOlsi"
    "c3RyZWFtLW1hbmlmZXN0LWEuanNvbiJdfX19"
)
RELEASED_NANOSECONDS = (
    "eyJpbml0aWFsIjoiMTk3MC0wMS0wMVQwMDowMDowMFoiLCJlbnRyaWVzIjp7IiouY3N2Ijp7Imxh"
    "c3Rtb2QiOiIyMDI2LTA2LTE2VDEyOjAwOjAwLjEyMzQ1Njc4OVoiLCJuYW1lcyI6WyJhLmNzdiIs"
    "ImIuY3N2Il19fX0"
)
RELEASED_MILLISECONDS = (
    "eyJpbml0aWFsIjoiMjAyNi0wNi0xNlQxMjowMDowMC4wMDVaIiwiZW50cmllcyI6e319"
)


def stored(state: dict) -> str:
    return base64.b64encode(json.dumps(state).encode()).decode().rstrip("=")


def document(value: str) -> dict:
    return json.loads(base64.b64decode(value + "=" * (-len(value) % 4)))


def round_trip(cursor: LastModifiedCursor) -> LastModifiedCursor:
    return LastModifiedCursor.decode(cursor.encode(), EPOCH)


def test_without_a_stored_state_the_cursor_starts_at_its_initial_time():
    cursor = LastModifiedCursor.decode(None, NOON)
    assert cursor.take(PATTERN, "old.json", NOON - timedelta(seconds=1)) is False
    assert cursor.take(PATTERN, "new.json", NOON) is True


def test_a_newer_file_replaces_what_was_taken():
    cursor = LastModifiedCursor.decode(None, EPOCH)
    cursor.take(PATTERN, "a.json", NOON)
    assert cursor.take(PATTERN, "b.json", NOON + timedelta(seconds=1)) is True
    assert cursor.entries[PATTERN] == Taken(NOON + timedelta(seconds=1), ["b.json"])


def test_an_older_file_is_not_taken():
    cursor = LastModifiedCursor.decode(None, EPOCH)
    cursor.take(PATTERN, "b.json", NOON)
    assert cursor.take(PATTERN, "a.json", NOON - timedelta(seconds=1)) is False


def test_files_sharing_a_time_are_each_taken_once():
    cursor = LastModifiedCursor.decode(None, EPOCH)
    assert cursor.take(PATTERN, "a.json", NOON) is True
    assert cursor.take(PATTERN, "b.json", NOON) is True
    assert cursor.take(PATTERN, "a.json", NOON) is False
    assert cursor.entries[PATTERN] == Taken(NOON, ["a.json", "b.json"])


def test_patterns_are_tracked_apart():
    cursor = LastModifiedCursor.decode(None, EPOCH)
    cursor.take(PATTERN, "a.json", NOON)
    assert cursor.take("other-*.json", "a.json", NOON) is True


def test_a_cursor_reads_back_what_it_stores():
    cursor = LastModifiedCursor.decode(None, EPOCH)
    cursor.take(PATTERN, "a.json", NOON)
    cursor.take(PATTERN, "b.json", NOON)
    assert round_trip(cursor) == cursor


def test_a_cursor_stored_by_a_released_source_reads_back():
    cursor = LastModifiedCursor.decode(RELEASED_SECONDS, NOON)
    assert cursor.initial == EPOCH
    assert cursor.entries == {PATTERN: Taken(WHOLE_NOON, ["stream-manifest-a.json"])}


def test_a_cursor_is_stored_as_released_sources_store_it():
    assert LastModifiedCursor.decode(RELEASED_SECONDS, EPOCH).encode() == (
        RELEASED_SECONDS
    )
    assert LastModifiedCursor.decode(RELEASED_MILLISECONDS, EPOCH).encode() == (
        RELEASED_MILLISECONDS
    )


def test_a_released_time_with_nanoseconds_reads_back_to_microseconds():
    cursor = LastModifiedCursor.decode(RELEASED_NANOSECONDS, EPOCH)
    assert cursor.entries == {"*.csv": Taken(NOON, ["a.csv", "b.csv"])}
    assert cursor.take("*.csv", "a.csv", NOON) is False
    assert cursor.take("*.csv", "c.csv", NOON) is True


def test_a_v1_cursor_reads_back_and_is_stored_in_the_released_format():
    value = stored({
        "V1": {
            "initial_last_modified": "1970-01-01T00:00:00Z",
            "entries": {PATTERN: ["2026-06-16T12:00:00Z", ["stream-manifest-a.json"]]},
        },
    })
    cursor = LastModifiedCursor.decode(value, NOON)
    assert cursor.encode() == RELEASED_SECONDS


def test_a_stored_state_with_nanoseconds_reads_back():
    value = stored({
        "V1": {
            "initial_last_modified": "1970-01-01T00:00:00Z",
            "entries": {
                PATTERN: ["2026-06-16T12:00:00.123456789Z", ["a.json"]],
            },
        },
    })
    cursor = LastModifiedCursor.decode(value, NOON)
    assert cursor.initial == EPOCH
    assert cursor.entries == {PATTERN: Taken(NOON, ["a.json"])}
    assert cursor.take(PATTERN, "a.json", NOON) is False
    assert cursor.take(PATTERN, "b.json", NOON) is True


@pytest.mark.parametrize(
    "text, expected",
    [
        ("2026-06-16T12:00:00Z", WHOLE_NOON),
        ("2026-06-16T12:00:00.123Z", NOON.replace(microsecond=123000)),
        ("2026-06-16T14:00:00.123456+02:00", NOON),
        ("2026-06-16T14:00:00+02:00", WHOLE_NOON),
        ("2026-06-16T10:00:00.123456-02:00", NOON),
    ],
    ids=["seconds", "milliseconds", "offset", "offset-seconds", "negative-offset"],
)
def test_stored_times_read_back_in_utc(text, expected):
    value = stored({"initial": text, "entries": {}})
    assert LastModifiedCursor.decode(value, EPOCH).initial == expected


@pytest.mark.parametrize(
    "instant, text",
    [
        (WHOLE_NOON, "2026-06-16T12:00:00Z"),
        (NOON.replace(microsecond=5000), "2026-06-16T12:00:00.005Z"),
        (NOON.replace(microsecond=1500), "2026-06-16T12:00:00.001500Z"),
        (NOON, "2026-06-16T12:00:00.123456Z"),
        (
            datetime(2026, 6, 16, 14, 0, tzinfo=timezone(timedelta(hours=2))),
            "2026-06-16T12:00:00Z",
        ),
    ],
    ids=["seconds", "milliseconds", "microseconds", "full", "offset"],
)
def test_times_are_stored_as_released_sources_write_them(instant, text):
    assert document(LastModifiedCursor(instant).encode()) == {
        "initial": text,
        "entries": {},
    }


def test_a_cursor_is_stored_unpadded_as_compact_json():
    cursor = LastModifiedCursor(EPOCH)
    cursor.take(PATTERN, "a.json", NOON)
    value = cursor.encode()
    assert not value.endswith("=")
    assert base64.b64decode(value + "=" * (-len(value) % 4)).decode() == (
        '{"initial":"1970-01-01T00:00:00Z","entries":{"stream-manifest-*.json":'
        '{"lastmod":"2026-06-16T12:00:00.123456Z","names":["a.json"]}}}'
    )


def test_a_cursor_takes_and_turns_down_files_across_stored_round_trips():
    pattern = "file*.csv"
    before = NOON - timedelta(milliseconds=2)
    initial = NOON - timedelta(milliseconds=1)
    after = NOON
    cursor = round_trip(LastModifiedCursor(initial))

    assert cursor.take(pattern, "file1.csv", before) is False
    assert pattern not in cursor.entries
    cursor = round_trip(cursor)

    assert cursor.take(pattern, "file1.csv", initial) is True
    assert cursor.entries[pattern] == Taken(initial, ["file1.csv"])
    cursor = round_trip(cursor)

    assert cursor.take(pattern, "file1.csv", initial) is False
    assert cursor.entries[pattern] == Taken(initial, ["file1.csv"])
    cursor = round_trip(cursor)

    assert cursor.take(pattern, "file2.csv", before) is False
    assert cursor.entries[pattern] == Taken(initial, ["file1.csv"])
    cursor = round_trip(cursor)

    assert cursor.take(pattern, "file2.csv", initial) is True
    assert cursor.entries[pattern] == Taken(initial, ["file1.csv", "file2.csv"])
    cursor = round_trip(cursor)

    assert cursor.take(pattern, "file2.csv", initial) is False
    assert cursor.entries[pattern] == Taken(initial, ["file1.csv", "file2.csv"])
    cursor = round_trip(cursor)

    assert cursor.take(pattern, "file3.csv", after) is True
    assert cursor.entries[pattern] == Taken(after, ["file3.csv"])
    cursor = round_trip(cursor)

    assert cursor.take(pattern, "file3.csv", initial) is False
    assert cursor.entries[pattern] == Taken(after, ["file3.csv"])


def test_a_stored_cursor_that_is_not_base64_is_refused():
    with pytest.raises(CursorError, match="not base64"):
        LastModifiedCursor.decode("@@JWMSI6IHsiaW5pdGlhbF9sYXN0X21vZGlmaWVkIjogIjIw", EPOCH)


def test_a_stored_time_without_an_offset_is_refused():
    value = (
        "eyJWMSI6IHsiaW5pdGlhbF9sYXN0X21vZGlmaWVkIjogIjIwMjQtMDktMDVUMDE6MDE6MDAuM"
        "DEwWiIsICJlbnRyaWVzIjogeyIvdGVzdGluZ19uZXN0ZWRfaW1wb3J0LyouY3N2IjogWyIyMD"
        "I2LTAyLTE4VDE0OjA0OjE0LjQzMzcyMCIsIFsibmV3X2RhdGEuY3N2Il1dfX19"
    )
    with pytest.raises(CursorError, match="no offset"):
        LastModifiedCursor.decode(value, EPOCH)


@pytest.mark.parametrize(
    "state",
    [
        {"entries": {}},
        {"initial": "1970-01-01T00:00:00Z"},
        {"initial": "1970-01-01T00:00:00Z", "entries": {PATTERN: ["x", ["a.json"]]}},
        {"V1": {"entries": {}}},
        ["not", "a", "cursor"],
    ],
    ids=["no-initial", "no-entries", "v1-entry-in-v2", "v1-no-initial", "a-list"],
)
def test_a_stored_cursor_of_an_unknown_shape_is_refused(state):
    with pytest.raises(CursorError, match="cannot be read"):
        LastModifiedCursor.decode(stored(state), EPOCH)


def test_a_stored_cursor_that_is_not_json_is_refused():
    value = base64.b64encode(b"not json").decode()
    with pytest.raises(CursorError, match="cannot be read"):
        LastModifiedCursor.decode(value, EPOCH)
