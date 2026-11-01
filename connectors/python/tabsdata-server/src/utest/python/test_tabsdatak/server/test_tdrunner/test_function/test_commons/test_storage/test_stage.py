#
# Copyright 2026 Tabsdata Inc.
#

from __future__ import annotations

from pathlib import Path

import pytest
from obstore.store import MemoryStore

from tabsdatak.common._storage.transfer import OBJECT_STORES
from tabsdatak.server._tdrunner.function._commons.document.message.request import (
    Location,
)
from tabsdatak.server._tdrunner.function._commons.storage.stage import (
    FILES_FOLDER,
    MANIFESTS_FOLDER,
    STREAM_STATE_FILE,
    Stage,
)

pytestmark = [pytest.mark.unit, pytest.mark.tabsdatak]


@pytest.fixture(params=["local", "object store"])
def stage(request, tmp_path, monkeypatch) -> Stage:
    if request.param == "object store":
        memory = MemoryStore()
        monkeypatch.setattr(OBJECT_STORES, "store", lambda root, options: memory)
    return Stage(Location(uri=(tmp_path / "stage").as_uri()))


def test_the_stage_layout_names_are_pinned():
    assert MANIFESTS_FOLDER == "manifests"
    assert FILES_FOLDER == "files"
    assert STREAM_STATE_FILE == "stream-state.json"


def test_a_written_text_is_read_back(stage):
    stage.write("manifests/one.json", '[["files/1/a.parquet"]]')
    assert stage.read("manifests/one.json") == '[["files/1/a.parquet"]]'


def test_a_missing_text_reads_as_none(stage):
    assert stage.read("stream-state.json") is None


def test_an_uploaded_file_is_read_back(stage, tmp_path):
    source = tmp_path / "work" / "a.parquet"
    source.parent.mkdir()
    source.write_text("data")
    stage.upload(source, "files/1/a.parquet")
    assert stage.read("files/1/a.parquet") == "data"


def test_a_discarded_name_is_gone(stage):
    stage.write("files/1/a.parquet", "data")
    stage.discard("files/1/a.parquet")
    assert stage.read("files/1/a.parquet") is None


def test_discarding_a_missing_name_is_not_an_error(stage):
    stage.discard("files/1/gone.parquet")


def test_discarding_the_last_file_of_a_local_folder_removes_it(tmp_path):
    stage = Stage(Location(uri=(tmp_path / "stage").as_uri()))
    stage.write("files/1/a.parquet", "data")
    stage.discard("files/1/a.parquet")
    assert not Path(tmp_path / "stage" / "files" / "1").exists()


def test_next_names_the_oldest_entry_of_a_folder(stage):
    for name in ("b.json", "a.json", "c.json"):
        stage.write(f"manifests/{name}", "[]")
    stage.write("files/0/z.parquet", "data")
    assert stage.next("manifests", None) == "manifests/a.json"


def test_next_names_the_entry_after_the_offset(stage):
    for name in ("a.json", "b.json", "c.json"):
        stage.write(f"manifests/{name}", "[]")
    assert stage.next("manifests", "manifests/a.json") == "manifests/b.json"
    assert stage.next("manifests", "manifests/c.json") is None


def test_next_in_an_empty_folder_is_none(stage):
    assert stage.next("manifests", None) is None
