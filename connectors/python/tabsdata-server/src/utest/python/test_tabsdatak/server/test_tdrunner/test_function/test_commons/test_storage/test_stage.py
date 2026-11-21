#
# Copyright 2026 Tabsdata Inc.
#

from __future__ import annotations

import errno
from pathlib import Path

import pytest
from obstore.store import MemoryStore

from tabsdatak.common._storage.transfer import OBJECT_STORES
from tabsdatak.server._tdrunner.function._commons.document.message.request import (
    Location,
)
from tabsdatak.server._tdrunner.function._commons.storage import (
    stage as stage_module,
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


@pytest.fixture
def flushed(monkeypatch) -> list[Path]:
    paths: list[Path] = []
    monkeypatch.setattr(stage_module, "flush", paths.append)
    return paths


def test_a_local_write_flushes_the_file_and_every_folder_up_to_the_stage(
    tmp_path, flushed, monkeypatch
):
    monkeypatch.setattr(stage_module, "FLUSH_FOLDERS", True)
    root = tmp_path / "stage"
    Stage(Location(uri=root.as_uri())).write("manifests/one.json", "[]")
    assert flushed == [
        root / "manifests" / "one.json",
        tmp_path,
        root,
        root / "manifests",
    ]


def test_a_local_upload_flushes_what_it_uploaded(tmp_path, flushed):
    source = tmp_path / "a.parquet"
    source.write_text("data")
    root = tmp_path / "stage"
    Stage(Location(uri=root.as_uri())).upload(source, "files/1/a.parquet")
    assert flushed[0] == root / "files" / "1" / "a.parquet"


def test_folders_are_not_flushed_where_they_cannot_be_opened(
    tmp_path, flushed, monkeypatch
):
    monkeypatch.setattr(stage_module, "FLUSH_FOLDERS", False)
    root = tmp_path / "stage"
    Stage(Location(uri=root.as_uri())).write("stream-state.json", "{}")
    assert flushed == [root / "stream-state.json"]


def test_an_object_store_flushes_nothing(tmp_path, flushed, monkeypatch):
    memory = MemoryStore()
    monkeypatch.setattr(OBJECT_STORES, "store", lambda root, options: memory)
    Stage(Location(uri=(tmp_path / "stage").as_uri())).write("stream-state.json", "{}")
    assert flushed == []


@pytest.mark.parametrize("code", [errno.EINVAL, errno.ENOTSUP])
def test_a_folder_the_filesystem_cannot_flush_is_left_unflushed(
    tmp_path, monkeypatch, code
):
    monkeypatch.setattr(stage_module, "FLUSH_FOLDERS", True)

    def flush(path: Path) -> None:
        if path.is_dir():
            raise OSError(code, "cannot flush a folder")

    monkeypatch.setattr(stage_module, "flush", flush)
    stage = Stage(Location(uri=(tmp_path / "stage").as_uri()))
    stage.write("stream-state.json", "{}")
    assert stage.read("stream-state.json") == "{}"


def test_a_folder_that_fails_to_flush_otherwise_fails_the_write(tmp_path, monkeypatch):
    monkeypatch.setattr(stage_module, "FLUSH_FOLDERS", True)

    def flush(path: Path) -> None:
        if path.is_dir():
            raise OSError(errno.EIO, "disk failure")

    monkeypatch.setattr(stage_module, "flush", flush)
    stage = Stage(Location(uri=(tmp_path / "stage").as_uri()))
    with pytest.raises(OSError, match="disk failure"):
        stage.write("stream-state.json", "{}")


def test_a_file_that_fails_to_flush_fails_the_write(tmp_path, monkeypatch):
    def flush(path: Path) -> None:
        raise OSError(errno.EIO, "disk failure")

    monkeypatch.setattr(stage_module, "flush", flush)
    stage = Stage(Location(uri=(tmp_path / "stage").as_uri()))
    with pytest.raises(OSError, match="disk failure"):
        stage.write("stream-state.json", "{}")


def test_flush_runs_on_this_platform(tmp_path):
    file = tmp_path / "a.json"
    file.write_text("{}", encoding="utf-8")
    stage_module.flush(file)
    if stage_module.FLUSH_FOLDERS:
        stage_module.flush(tmp_path)
    assert file.read_text(encoding="utf-8") == "{}"
