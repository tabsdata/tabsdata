#
# Copyright 2026 Tabsdata Inc.
#

from __future__ import annotations

import base64
import json
from datetime import datetime, timedelta, timezone

import pytest

# noinspection PyProtectedMember
from tabsdatak.conn.common._cursor import LastModifiedCursor, Taken

PATTERN = "/stage/manifests/stream-manifest-*.json"
EPOCH = datetime(1970, 1, 1, tzinfo=timezone.utc)
NOON = datetime(2026, 6, 16, 12, 0, 0, 123456, tzinfo=timezone.utc)


def stored(state: dict) -> str:
    return base64.b64encode(json.dumps(state).encode()).decode().rstrip("=")


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
    assert cursor.take("/other/*.json", "a.json", NOON) is True


def test_a_cursor_reads_back_what_it_stores():
    cursor = LastModifiedCursor.decode(None, EPOCH)
    cursor.take(PATTERN, "a.json", NOON)
    cursor.take(PATTERN, "b.json", NOON)
    assert LastModifiedCursor.decode(cursor.encode(), EPOCH) == cursor


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
        ("2026-06-16T12:00:00Z", NOON.replace(microsecond=0)),
        ("2026-06-16T12:00:00.123Z", NOON.replace(microsecond=123000)),
        ("2026-06-16T14:00:00.123456+02:00", NOON),
    ],
    ids=["seconds", "milliseconds", "offset"],
)
def test_stored_times_read_back_in_utc(text, expected):
    value = stored({"V1": {"initial_last_modified": text, "entries": {}}})
    assert LastModifiedCursor.decode(value, EPOCH).initial == expected
