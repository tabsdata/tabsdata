#
# Copyright 2026 Tabsdata Inc.
#

from __future__ import annotations

import json
import time
from datetime import datetime
from pathlib import Path

import pytest

from tabsdatak.server._tdrunner.function._commons.document.message.request import (
    Location,
)
from tabsdatak.server._tdrunner.function._commons.manifests.stage import (
    StageManifest,
)
from tabsdatak.server._tdrunner.function._commons.storage.stage import Stage
from tabsdatak.server._tdrunner.function._handlers.sweeper import Sweeper

pytestmark = [pytest.mark.unit, pytest.mark.tabsdatak]

TIMESTAMP = datetime(2026, 6, 16, 10, 30, 12, 345_000)


def sweeper(root: Path) -> Sweeper:
    return Sweeper(Stage(Location(uri=root.as_uri())))


def batch_files(*names: str) -> list[str]:
    manifest = StageManifest(TIMESTAMP)
    return [manifest.file(name) for name in names]


def plant(root: Path, tables: list[list[str]]) -> str:
    for files in tables:
        for name in files:
            file = root / name
            file.parent.mkdir(parents=True, exist_ok=True)
            file.write_text("parquet-stub")
    path = StageManifest(TIMESTAMP).path
    (root / path).parent.mkdir(parents=True, exist_ok=True)
    (root / path).write_text(json.dumps(tables))
    return path


def test_a_consumed_batch_queues_its_files_then_its_manifest(tmp_path):
    first = batch_files("a.parquet")
    second = batch_files("b.parquet", "c.parquet")
    path = plant(tmp_path, [first, second])
    queue = sweeper(tmp_path)
    queue.enqueue(path)
    assert list(queue.queue) == [*first, *second, path]


def test_a_batch_without_files_queues_its_manifest_only(tmp_path):
    path = plant(tmp_path, [[], []])
    queue = sweeper(tmp_path)
    queue.enqueue(path)
    assert list(queue.queue) == [path]


def test_a_missing_manifest_is_queued_alone(tmp_path):
    path = StageManifest(TIMESTAMP).path
    queue = sweeper(tmp_path)
    queue.enqueue(path)
    assert list(queue.queue) == [path]


def test_an_unreadable_manifest_is_queued_alone(tmp_path):
    path = StageManifest(TIMESTAMP).path
    (tmp_path / path).parent.mkdir(parents=True)
    (tmp_path / path).write_text("not json")
    queue = sweeper(tmp_path)
    queue.enqueue(path)
    assert list(queue.queue) == [path]


def test_sweeping_deletes_the_queued_files(tmp_path):
    files = batch_files("a.parquet")
    plant(tmp_path, [files])
    queue = sweeper(tmp_path)
    queue.queue.extend(files)
    queue.sweep_until(time.monotonic() + 5, lambda: True)
    assert not (tmp_path / files[0]).exists()
    assert len(queue) == 0


def test_a_batch_folder_goes_with_its_last_file(tmp_path):
    files = batch_files("a.parquet")
    plant(tmp_path, [files])
    queue = sweeper(tmp_path)
    queue.queue.extend(files)
    queue.sweep_until(time.monotonic() + 5, lambda: True)
    assert not (tmp_path / files[0]).parent.exists()


def test_a_file_already_gone_is_not_an_error(tmp_path):
    queue = sweeper(tmp_path)
    queue.queue.append("files/nope/ghost.parquet")
    queue.sweep_until(time.monotonic() + 1, lambda: True)
    assert len(queue) == 0


def test_a_failed_delete_is_logged_and_skipped(tmp_path, caplog, monkeypatch):
    queue = sweeper(tmp_path)

    def failing(name: str) -> None:
        raise OSError(f"cannot discard {name}")

    monkeypatch.setattr(queue.stage, "discard", failing)
    queue.queue.append("files/x/leftover.parquet")
    with caplog.at_level("WARNING"):
        queue.sweep_until(time.monotonic() + 1, lambda: True)
    assert len(queue) == 0
    assert any("Cannot delete" in record.message for record in caplog.records)


def test_an_empty_queue_waits_until_the_deadline(tmp_path):
    start = time.monotonic()
    sweeper(tmp_path).sweep_until(start + 0.3, lambda: True)
    assert 0.25 <= time.monotonic() - start <= 0.8


def test_an_inactive_sweep_deletes_nothing(tmp_path):
    files = batch_files("a.parquet")
    plant(tmp_path, [files])
    queue = sweeper(tmp_path)
    queue.queue.extend(files)
    queue.sweep_until(time.monotonic() + 10, lambda: False)
    assert (tmp_path / files[0]).exists()
    assert list(queue.queue) == files
