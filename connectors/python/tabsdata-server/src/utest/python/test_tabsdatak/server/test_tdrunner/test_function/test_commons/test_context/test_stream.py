#
# Copyright 2026 Tabsdata Inc.
#

from __future__ import annotations

import json
from pathlib import Path

import pytest
from obstore.store import MemoryStore

from tabsdatak.api import BatchThresholds, StreamThresholds
from tabsdatak.common._storage.transfer import OBJECT_STORES, TransferError
from tabsdatak.server._tdrunner.function._commons.context.stream import (
    StreamSourceContext,
)
from tabsdatak.server._tdrunner.function._commons.document.message.request import (
    Location,
)
from tabsdatak.server._tdrunner.function._commons.document.state.stream import (
    API_VERSION,
    KIND,
    StreamState,
)
from tabsdatak.server._tdrunner.function._commons.manifests.stage import (
    MANIFEST_PREFIX,
)
from tabsdatak.server._tdrunner.function._commons.storage.stage import (
    FILES_FOLDER,
    MANIFESTS_FOLDER,
    STREAM_STATE_FILE,
    Stage,
)
from tabsdatak.server._tdrunner.function._lib.errors import (
    BadStreamStateError,
    CommandErrorCode,
)

pytestmark = [pytest.mark.unit, pytest.mark.tabsdatak]

TABSDATA_VERSION = "2.0.0-test"
THRESHOLDS = StreamThresholds(batch=BatchThresholds(max_secs=300))


def make_context(tmp_path: Path, uri: str | None = None) -> StreamSourceContext:
    work = tmp_path / "work"
    work.mkdir(exist_ok=True)
    if uri is None:
        stage = tmp_path / "stage"
        stage.mkdir(exist_ok=True)
        uri = stage.as_uri()
    return StreamSourceContext(
        work_folder=work,
        project_id="p-1",
        project="p",
        collection_id="C-1",
        collection="c",
        function_id="F-1",
        function="stream_people",
        function_version_id="fv-1",
        staging=Stage(Location(uri=uri)),
        thresholds=THRESHOLDS,
        tabsdata_version=TABSDATA_VERSION,
    )


def local_stage(root: Path) -> Stage:
    return Stage(Location(uri=root.as_uri()))


def work_file(tmp_path: Path, name: str) -> Path:
    file = tmp_path / "work" / name
    file.parent.mkdir(parents=True, exist_ok=True)
    file.write_bytes(b"PAR1stub")
    return file


def envelope(state: dict) -> dict:
    return {
        "kind": KIND,
        "apiVersion": API_VERSION,
        "tabsdataVersion": "2.0.0",
        "externalId": "2026-06-16T10-30-12.345000",
        "state": state,
    }


def manifests(tmp_path: Path) -> list[Path]:
    return sorted((tmp_path / "stage" / MANIFESTS_FOLDER).glob(f"{MANIFEST_PREFIX}*"))


def batch(manifest: Path) -> str:
    return manifest.stem[len(MANIFEST_PREFIX) :]


def saved(tmp_path: Path) -> dict:
    return json.loads((tmp_path / "stage" / STREAM_STATE_FILE).read_text())


def test_no_state_file_means_a_first_run(tmp_path):
    assert StreamState.load(local_stage(tmp_path)) is None


@pytest.mark.parametrize(
    "text",
    [
        "not-json{",
        "[1, 2, 3]",
        json.dumps({"kind": KIND, "apiVersion": 999, "state": {}}),
    ],
    ids=["not json", "not an object", "unknown api version"],
)
def test_a_state_the_streamer_did_not_write_is_refused(tmp_path, text):
    (tmp_path / STREAM_STATE_FILE).write_text(text)
    with pytest.raises(BadStreamStateError) as raised:
        StreamState.load(local_stage(tmp_path))
    assert raised.value.api_error is CommandErrorCode.CMD_7


def test_a_state_that_cannot_be_read_is_not_a_first_run(tmp_path, monkeypatch):
    stage = local_stage(tmp_path)

    def unreadable(name: str) -> str | None:
        raise PermissionError("stage not mounted yet")

    monkeypatch.setattr(stage, "read", unreadable)
    with pytest.raises(BadStreamStateError) as raised:
        StreamState.load(stage)
    assert raised.value.api_error is CommandErrorCode.CMD_6
    assert isinstance(raised.value.__cause__, PermissionError)


@pytest.mark.parametrize("state", [{"offset": 42}, {}])
def test_a_saved_state_is_loaded(tmp_path, state):
    (tmp_path / STREAM_STATE_FILE).write_text(json.dumps(envelope(state)))
    view = StreamState.load(local_stage(tmp_path))
    assert view.state == state
    assert view.external_id == "2026-06-16T10-30-12.345000"
    assert view.tabsdata_version == "2.0.0"


def test_a_new_context_starts_active_and_unstaged(tmp_path):
    context = make_context(tmp_path)
    assert context.active is True
    assert context.first_stage is True
    assert context.stream_state is None
    assert context.thresholds == THRESHOLDS


def test_a_context_on_an_empty_object_store_starts_unstaged(tmp_path, monkeypatch):
    memory = MemoryStore()
    monkeypatch.setattr(OBJECT_STORES, "store", lambda root, options: memory)
    assert make_context(tmp_path, "s3://bucket/stage").first_stage is True


def test_a_stage_on_an_unknown_scheme_is_refused():
    with pytest.raises(TransferError):
        Stage(Location(uri="ftp://bucket/stage"))


def test_a_context_resumes_from_the_saved_state(tmp_path):
    (tmp_path / "stage").mkdir()
    (tmp_path / "stage" / STREAM_STATE_FILE).write_text(
        json.dumps(envelope({"offset": 100}))
    )
    context = make_context(tmp_path)
    assert context.stream_state == {"offset": 100}
    assert context.first_stage is False


def test_staged_files_move_into_their_batch_folder(tmp_path):
    context = make_context(tmp_path)
    first = work_file(tmp_path, "people-1.parquet")
    second = work_file(tmp_path, "people-2.parquet")
    context.stage([[first, second]], {"offset": 1})
    (manifest,) = manifests(tmp_path)
    folder = tmp_path / "stage" / FILES_FOLDER / batch(manifest)
    assert (folder / "people-1.parquet").is_file()
    assert (folder / "people-2.parquet").is_file()
    assert not first.exists()
    assert not second.exists()


def test_a_manifest_names_its_files_per_table(tmp_path):
    context = make_context(tmp_path)
    context.stage([[work_file(tmp_path, "data.parquet")], []], {})
    (manifest,) = manifests(tmp_path)
    assert json.loads(manifest.read_text()) == [
        [f"{FILES_FOLDER}/{batch(manifest)}/data.parquet"],
        [],
    ]


def test_a_staged_state_is_saved_with_its_batch(tmp_path):
    context = make_context(tmp_path)
    context.stage([[work_file(tmp_path, "data.parquet")]], {"offset": 42})
    (manifest,) = manifests(tmp_path)
    assert saved(tmp_path) == {
        "kind": KIND,
        "apiVersion": API_VERSION,
        "tabsdataVersion": TABSDATA_VERSION,
        "externalId": batch(manifest),
        "state": {"offset": 42},
    }
    assert context.stream_state == {"offset": 42}
    assert context.first_stage is False


def test_an_external_commit_runs_before_the_marker_is_saved(tmp_path):
    context = make_context(tmp_path)
    state_file = tmp_path / "stage" / STREAM_STATE_FILE
    marker_existed: list[bool] = []
    context.stage(
        [[work_file(tmp_path, "data.parquet")]],
        lambda: marker_existed.append(state_file.is_file()),
    )
    assert marker_existed == [False]
    assert saved(tmp_path)["state"] == {}
    assert context.stream_state == {}


def test_a_failed_external_commit_saves_no_marker(tmp_path):
    context = make_context(tmp_path)

    def unreachable() -> None:
        raise RuntimeError("external system unreachable")

    with pytest.raises(RuntimeError):
        context.stage([[work_file(tmp_path, "data.parquet")]], unreachable)
    assert not (tmp_path / "stage" / STREAM_STATE_FILE).exists()


def test_staging_a_missing_file_is_refused(tmp_path):
    context = make_context(tmp_path)
    with pytest.raises(FileNotFoundError):
        context.stage([[tmp_path / "work" / "missing.parquet"]], {})


def test_each_batch_has_its_own_manifest_and_folder(tmp_path):
    context = make_context(tmp_path)
    context.stage([[work_file(tmp_path, "batch-1.parquet")]], {"n": 1})
    context.stage([[work_file(tmp_path, "batch-2.parquet")]], {"n": 2})
    first, second = manifests(tmp_path)
    assert batch(first) < batch(second)
    files = tmp_path / "stage" / FILES_FOLDER
    assert (files / batch(first) / "batch-1.parquet").is_file()
    assert (files / batch(second) / "batch-2.parquet").is_file()


def test_a_checkpoint_saves_the_state_alone(tmp_path):
    context = make_context(tmp_path)
    context.stage([[work_file(tmp_path, "data.parquet")]], {"o": 1})
    staged = saved(tmp_path)["externalId"]
    context.checkpoint({"o": 2})
    assert saved(tmp_path)["state"] == {"o": 2}
    assert saved(tmp_path)["externalId"] == staged
    assert len(manifests(tmp_path)) == 1
    assert context.stream_state == {"o": 2}


def test_a_checkpoint_is_read_back_by_the_next_run(tmp_path):
    context = make_context(tmp_path)
    context.stage([[work_file(tmp_path, "data.parquet")]], {"o": 1})
    context.checkpoint({"o": 2})
    assert make_context(tmp_path).stream_state == {"o": 2}


def test_deactivating_twice_leaves_it_inactive(tmp_path):
    context = make_context(tmp_path)
    context.deactivate()
    context.deactivate()
    assert context.active is False
