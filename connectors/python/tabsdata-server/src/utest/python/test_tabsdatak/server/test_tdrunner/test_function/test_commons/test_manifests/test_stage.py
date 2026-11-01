#
# Copyright 2026 Tabsdata Inc.
#

from __future__ import annotations

import json
from datetime import datetime, timedelta, timezone
from pathlib import Path

import pytest

from tabsdatak.server._tdrunner.function._commons.document.message.request import (
    Location,
)
from tabsdatak.server._tdrunner.function._commons.manifests.stage import (
    InvalidManifestError,
    StageManifest,
)
from tabsdatak.server._tdrunner.function._commons.storage.stage import Stage

pytestmark = [pytest.mark.unit, pytest.mark.tabsdatak]

FILENAME = "stream-manifest-2026-06-16T10-30-12.345000.json"
PATH = f"manifests/{FILENAME}"
TIMESTAMP = datetime(2026, 6, 16, 10, 30, 12, 345_000)


def stage_of(root: Path) -> Stage:
    return Stage(Location(uri=root.as_uri()))


def written(root: Path, text: str) -> Stage:
    (root / "manifests").mkdir(exist_ok=True)
    (root / PATH).write_text(text)
    return stage_of(root)


def test_a_naive_timestamp_names_its_manifest():
    manifest = StageManifest(TIMESTAMP)
    assert manifest.filename == FILENAME
    assert manifest.path == PATH


def test_an_aware_timestamp_names_its_manifest_in_utc():
    plus_two = timezone(timedelta(hours=2))
    aware = datetime(2026, 6, 16, 12, 30, 12, 345_000, tzinfo=plus_two)
    assert StageManifest(aware).filename == FILENAME


def test_a_name_parses_back_to_its_naive_timestamp():
    parsed = StageManifest.parse(FILENAME)
    assert parsed.timestamp == TIMESTAMP
    assert parsed.timestamp.tzinfo is None


def test_a_path_parses_by_its_filename():
    assert StageManifest.parse(PATH) == StageManifest(TIMESTAMP)


def test_names_sort_by_time():
    earlier = StageManifest(datetime(2026, 6, 16, 9, 59, 59, 999_999)).filename
    later = StageManifest(datetime(2026, 6, 16, 10, 0, 0, 1)).filename
    assert sorted([later, earlier]) == [earlier, later]


@pytest.mark.parametrize(
    "filename",
    [
        "not-a-manifest-2026-06-16T10-30-12.345000.json",
        "stream-manifest-2026-06-16T10-30-12.345000.yaml",
        "stream-manifest-1234567890.json",
        "stream-manifest-2026-02-30T10-30-12.345000.json",
    ],
    ids=["no prefix", "no suffix", "no timestamp", "no such date"],
)
def test_an_invalid_name_is_refused(filename):
    with pytest.raises(InvalidManifestError):
        StageManifest.parse(filename)


def test_the_batch_is_the_name_timestamp():
    assert StageManifest.parse(FILENAME).batch == "2026-06-16T10-30-12.345000"


def test_a_batch_file_lives_in_its_batch_folder():
    assert StageManifest(TIMESTAMP).file("a.parquet") == (
        "files/2026-06-16T10-30-12.345000/a.parquet"
    )


@pytest.mark.parametrize(
    "tables",
    [
        [["a.parquet", "b.parquet"], ["c.parquet"]],
        [],
        [[], ["c.parquet"]],
    ],
    ids=["files", "no tables", "a table without files"],
)
def test_a_written_manifest_reads_back_its_files_per_table(tmp_path, tables):
    stage = stage_of(tmp_path)
    StageManifest(TIMESTAMP).with_tables(tables).write(stage)
    assert StageManifest.read(stage, PATH).tables == tables
    assert json.loads((tmp_path / PATH).read_text()) == tables


def test_a_missing_manifest_reads_as_none(tmp_path):
    assert StageManifest.read(stage_of(tmp_path), PATH) is None


@pytest.mark.parametrize(
    "text",
    [
        "{not-json}",
        json.dumps({"slots": []}),
        json.dumps([[1, 2], [3]]),
    ],
    ids=["not json", "not a list", "not file names"],
)
def test_an_invalid_manifest_is_refused(tmp_path, text):
    with pytest.raises(InvalidManifestError):
        StageManifest.read(written(tmp_path, text), PATH)
