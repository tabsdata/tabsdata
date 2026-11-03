#
# Copyright 2026 Tabsdata Inc.
#

from __future__ import annotations

from datetime import datetime, timedelta, timezone

import pytest

# noinspection PyProtectedMember
from tabsdatak.conn.common._cursor import LastModifiedCursor, Taken

# noinspection PyProtectedMember
from tabsdatak.conn.common._find import FindError, Listed, find, split_pattern

BASELINE = datetime(2026, 6, 16, 12, 0, 0, tzinfo=timezone.utc)


def at(seconds: float) -> datetime:
    return BASELINE + timedelta(seconds=seconds)


def names(files: list[Listed]) -> list[str]:
    return [file.name for file in files]


def test_a_path_splits_on_its_last_slash():
    assert split_pattern("a/b/c.csv") == ("a/b", "c.csv")
    assert split_pattern("c.csv") == ("", "c.csv")
    assert split_pattern("a/b/") == ("a/b", "")


def test_a_path_that_names_no_file_is_refused():
    with pytest.raises(FindError):
        find([], "", None)


@pytest.mark.parametrize(
    "pattern, name, matches",
    [
        ("*.csv", "a.csv", True),
        ("*.csv", ".csv", True),
        ("*.csv", "a.csv.gz", False),
        ("*.csv*", "a.csv.gz", True),
        ("file?.csv", "file1.csv", True),
        ("file?.csv", "file10.csv", False),
        ("file?.csv", "file.csv", False),
        ("[ab].csv", "a.csv", False),
        ("[ab].csv", "[ab].csv", True),
        ("a+b.csv", "a+b.csv", True),
        ("a+b.csv", "aab.csv", False),
        ("*.CSV", "a.csv", False),
        ("*", "badge_scans copy.csv", True),
        ("*.csv", "line\nbreak.csv", True),
    ],
)
def test_a_wildcard_matches_file_names(pattern, name, matches):
    found = find([Listed(name, at(0))], pattern, None)
    assert names(found.selected) == ([name] if matches else [])


def test_nothing_is_found_when_no_file_matches():
    found = find([Listed("other.txt", at(0))], "file*.csv", None)
    assert found.selected == [] and found.purgeable == []


def test_matched_files_are_found_oldest_first():
    listed = [Listed(f"{n}.csv", at(-n)) for n in range(5)]
    found = find(listed, "*.csv", None)
    assert names(found.selected) == ["4.csv", "3.csv", "2.csv", "1.csv", "0.csv"]


def test_files_sharing_a_time_are_found_by_name():
    listed = [Listed("b.csv", at(0)), Listed("c.csv", at(0)), Listed("a.csv", at(0))]
    assert names(find(listed, "*.csv", None).selected) == ["a.csv", "b.csv", "c.csv"]


def test_a_file_name_with_a_space_is_found():
    found = find([Listed("badge_scans copy.csv", at(0))], "*.csv", None)
    assert names(found.selected) == ["badge_scans copy.csv"]


def test_files_older_than_the_baseline_are_not_taken():
    cursor = LastModifiedCursor(at(0))
    listed = [Listed("file1.csv", at(-2)), Listed("file2.csv", at(-1))]
    found = find(listed, "file*.csv", cursor)
    assert found.selected == []
    assert "file*.csv" not in cursor.entries


def test_only_files_from_the_baseline_on_are_taken():
    cursor = LastModifiedCursor(at(0))
    listed = [Listed("file1.csv", at(-1)), Listed("file2.csv", at(1))]
    found = find(listed, "file*.csv", cursor)
    assert names(found.selected) == ["file2.csv"]
    assert cursor.entries["file*.csv"] == Taken(at(1), ["file2.csv"])


def test_a_path_without_a_wildcard_takes_its_one_file():
    cursor = LastModifiedCursor(at(0))
    assert find([], "file.csv", cursor).selected == []
    assert find([Listed("file.csv", at(-1))], "file.csv", cursor).selected == []
    found = find([Listed("file.csv", at(1))], "file.csv", cursor)
    assert names(found.selected) == ["file.csv"]
    assert cursor.entries["file.csv"] == Taken(at(1), ["file.csv"])


def test_max_files_takes_the_oldest_and_a_stored_cursor_resumes_past_it():
    listed = [Listed(f"file{n}.csv", at(n)) for n in (1, 2, 3)]
    cursor = LastModifiedCursor(at(1))
    assert names(find(listed, "file*.csv", cursor, max_files=1).selected) == [
        "file1.csv"
    ]
    resumed = LastModifiedCursor.decode(cursor.encode(), at(1))
    assert names(find(listed, "file*.csv", resumed, max_files=1).selected) == [
        "file2.csv"
    ]


def test_max_files_without_a_cursor_keeps_the_oldest():
    listed = [Listed(f"file{n}.csv", at(n)) for n in (3, 1, 2)]
    found = find(listed, "file*.csv", None, max_files=2)
    assert names(found.selected) == ["file1.csv", "file2.csv"]


def test_max_files_counts_only_files_taken_now():
    listed = [Listed(f"file{n}.csv", at(n)) for n in (1, 2, 3)]
    cursor = LastModifiedCursor(at(0))
    find(listed, "file*.csv", cursor, max_files=2)
    found = find(listed, "file*.csv", cursor, max_files=2)
    assert names(found.selected) == ["file3.csv"]


def test_files_sharing_a_time_are_taken_across_runs_once_each():
    listed = [Listed("b.csv", at(0)), Listed("a.csv", at(0))]
    cursor = LastModifiedCursor(at(0))
    assert names(find(listed, "*.csv", cursor, max_files=1).selected) == ["a.csv"]
    assert names(find(listed, "*.csv", cursor, max_files=1).selected) == ["b.csv"]
    assert find(listed, "*.csv", cursor, max_files=1).selected == []


def test_a_file_below_the_baseline_is_never_purgeable():
    listed = [Listed("old.csv", at(-1000)), Listed("new.csv", at(1000))]
    found = find(listed, "*.csv", LastModifiedCursor(at(0)))
    assert names(found.selected) == ["new.csv"]
    assert found.purgeable == []


def test_a_file_already_consumed_is_purgeable_on_the_next_pass():
    listed = [Listed("a.csv", at(1000)), Listed("b.csv", at(2000))]
    cursor = LastModifiedCursor(at(0))
    found = find(listed, "*.csv", cursor)
    assert names(found.selected) == ["a.csv", "b.csv"]
    assert found.purgeable == []
    found = find(listed, "*.csv", cursor)
    assert found.selected == []
    assert names(found.purgeable) == ["a.csv", "b.csv"]


def test_without_a_cursor_nothing_is_purgeable():
    found = find([Listed("a.csv", at(-1000))], "*.csv", None)
    assert names(found.selected) == ["a.csv"]
    assert found.purgeable == []


def test_a_file_behind_the_watermark_but_not_below_the_baseline_is_purgeable():
    cursor = LastModifiedCursor(at(0))
    find([Listed("b.csv", at(2000))], "*.csv", cursor)
    found = find([Listed("a.csv", at(1000)), Listed("b.csv", at(2000))], "*.csv", cursor)
    assert found.selected == []
    assert names(found.purgeable) == ["a.csv", "b.csv"]


def test_files_past_max_files_are_neither_taken_nor_purgeable():
    listed = [Listed(f"file{n}.csv", at(n)) for n in (1, 2, 3)]
    cursor = LastModifiedCursor(at(0))
    found = find(listed, "file*.csv", cursor, max_files=1)
    assert names(found.selected) == ["file1.csv"]
    assert found.purgeable == []
    assert cursor.entries["file*.csv"] == Taken(at(1), ["file1.csv"])
