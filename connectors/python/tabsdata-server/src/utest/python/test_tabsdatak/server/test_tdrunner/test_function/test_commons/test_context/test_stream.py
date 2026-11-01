#
# Copyright 2026 Tabsdata Inc.
#

from __future__ import annotations

import json
from pathlib import Path

import polars as pl
import pytest
from obstore.store import MemoryStore

from tabsdatak._api import LegacyStreamStageWarning
from tabsdatak.api import BatchThresholds, StreamThresholds
from tabsdatak.common._storage.transfer import OBJECT_STORES, TransferError
from tabsdatak.server._tdrunner.function._commons.context.stream import (
    StageForm,
    StageFormError,
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
from tabsdatak.server._tdrunner.function._handlers.batch import (
    NotParquetError,
    NotPendingError,
    StageBatch,
    TableCountError,
    TableTypeError,
)
from tabsdatak.server._tdrunner.function._lib.errors import (
    BadStreamStateError,
    CommandErrorCode,
)
from tabsdatak.spi import LazyFrameInput, StageFileStats, StreamSrcPlugin

pytestmark = [pytest.mark.unit, pytest.mark.tabsdatak]

TABSDATA_VERSION = "2.0.0-test"
THRESHOLDS = StreamThresholds(batch=BatchThresholds(max_secs=300))


class PeoplePlugin(StreamSrcPlugin):
    def run(self, ctx, conn, src) -> None:
        pass


def make_context(
    tmp_path: Path,
    uri: str | None = None,
    tables: int = 1,
) -> StreamSourceContext:
    work = tmp_path / "work"
    work.mkdir(exist_ok=True)
    if uri is None:
        stage = tmp_path / "stage"
        stage.mkdir(exist_ok=True)
        uri = stage.as_uri()
    staging = Stage(Location(uri=uri))
    return StreamSourceContext(
        work_folder=work,
        project_id="p-1",
        project="p",
        collection_id="C-1",
        collection="c",
        function_id="F-1",
        function="stream_people",
        function_version_id="fv-1",
        staging=staging,
        batch=StageBatch(staging, tables),
        plugin=PeoplePlugin,
        thresholds=THRESHOLDS,
        tabsdata_version=TABSDATA_VERSION,
    )


def local_stage(root: Path) -> Stage:
    return Stage(Location(uri=root.as_uri()))


def work_file(tmp_path: Path, name: str) -> Path:
    file = tmp_path / "work" / name
    file.parent.mkdir(parents=True, exist_ok=True)
    pl.DataFrame({"id": [1, 2]}).write_parquet(file)
    return file


def frame(*ids: int) -> LazyFrameInput:
    return LazyFrameInput(pl.LazyFrame({"id": list(ids)}))


def published(manifest: Path) -> list[list[str]]:
    return json.loads(manifest.read_text())


def staged_files(tmp_path: Path) -> list[str]:
    files = tmp_path / "stage" / FILES_FOLDER
    return sorted(
        file.relative_to(files).as_posix()
        for file in files.rglob("*")
        if file.is_file()
    )


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


@pytest.mark.parametrize(
    "content",
    [b"name,age\nada,36\n", b"PAR1stub", b""],
    ids=["text", "a truncated parquet", "empty"],
)
def test_staging_a_file_that_is_not_parquet_is_refused(tmp_path, content):
    context = make_context(tmp_path)
    file = tmp_path / "work" / "people.parquet"
    file.write_bytes(content)
    with pytest.raises(NotParquetError) as raised:
        context.stage([[file]], {})
    assert (raised.value.position, raised.value.index) == (0, 0)
    assert raised.value.file == file


def test_a_bad_file_stages_nothing_of_its_call(tmp_path):
    context = make_context(tmp_path, tables=2)
    good = work_file(tmp_path, "good.parquet")
    bad = tmp_path / "work" / "bad.parquet"
    bad.write_text("not parquet")
    with pytest.raises(NotParquetError) as raised:
        context.stage([[good], [bad]], {"offset": 1})
    assert (raised.value.position, raised.value.index) == (1, 0)
    assert good.is_file()
    assert staged_files(tmp_path) == []
    assert manifests(tmp_path) == []
    assert context.pending == [[], []]
    assert context.first_stage is True


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


@pytest.mark.filterwarnings("ignore::DeprecationWarning")
def test_staging_files_warns_once_that_the_form_is_deprecated(tmp_path, caplog):
    context = make_context(tmp_path)
    with pytest.warns(LegacyStreamStageWarning, match="PeoplePlugin"):
        context.stage([[work_file(tmp_path, "batch-1.parquet")]], {"n": 1})
    context.stage([[work_file(tmp_path, "batch-2.parquet")]], {"n": 2})
    warnings = [record for record in caplog.records if "deprecated" in record.message]
    assert len(warnings) == 1


def test_sending_tables_writes_them_without_publishing(tmp_path):
    context = make_context(tmp_path, tables=2)
    sent = context.stage([[frame(1, 2), frame(3)], None])
    assert [[stats.name for stats in table] for table in sent] == [
        ["0.0.t", "0.1.t"],
        [],
    ]
    assert [[stats.rows for stats in table] for table in sent] == [[2, 1], []]
    assert all(stats.bytes > 0 for stats in sent[0])
    assert context.pending == sent
    assert manifests(tmp_path) == []
    assert context.first_stage is True
    assert not (tmp_path / "stage" / STREAM_STATE_FILE).exists()


def test_a_sent_table_lands_with_its_schema(tmp_path):
    context = make_context(tmp_path)
    context.stage([[frame(1)]])
    (schema, data) = staged_files(tmp_path)
    assert data.endswith("/0.0.t")
    assert schema.endswith("/0.0.s")


def test_publishing_without_tables_publishes_what_was_sent(tmp_path):
    context = make_context(tmp_path, tables=2)
    context.stage([[frame(1)], []])
    context.stage([None, [frame(2)]], None)
    assert context.stage(None, {"offset": 7}) is None
    (manifest,) = manifests(tmp_path)
    folder = f"{FILES_FOLDER}/{batch(manifest)}"
    assert published(manifest) == [[f"{folder}/0.0.t"], [f"{folder}/1.0.t"]]
    assert context.stream_state == {"offset": 7}
    assert context.pending == [[], []]


def test_sending_with_a_state_publishes_the_sent_and_the_new_tables(tmp_path):
    context = make_context(tmp_path)
    context.stage([[frame(1)]])
    sent = context.stage([[frame(2)]], {"offset": 2})
    assert [stats.name for stats in sent[0]] == ["0.1.t"]
    (manifest,) = manifests(tmp_path)
    folder = f"{FILES_FOLDER}/{batch(manifest)}"
    assert published(manifest) == [[f"{folder}/0.0.t", f"{folder}/0.1.t"]]


def test_publishing_with_nothing_sent_publishes_an_empty_batch(tmp_path):
    context = make_context(tmp_path, tables=2)
    context.stage(None, {"offset": 1})
    (manifest,) = manifests(tmp_path)
    assert published(manifest) == [[], []]
    assert context.first_stage is False


def test_files_cannot_be_staged_once_frames_were(tmp_path):
    context = make_context(tmp_path)
    context.stage([[frame(1)]])
    with pytest.raises(StageFormError) as raised:
        context.stage([[work_file(tmp_path, "legacy.parquet")]], {"offset": 1})
    assert raised.value.locked is StageForm.FRAMES
    assert raised.value.attempted is StageForm.FILES
    assert manifests(tmp_path) == []
    assert (tmp_path / "work" / "legacy.parquet").is_file()


@pytest.mark.parametrize(
    "tables_files, new_state",
    [([[frame(1)]], None), ([[frame(1)]], {"offset": 2}), (None, {"offset": 2})],
    ids=["a send", "a send with a state", "a publish"],
)
def test_frames_cannot_be_staged_once_files_were(tmp_path, tables_files, new_state):
    context = make_context(tmp_path)
    with pytest.warns(LegacyStreamStageWarning):
        context.stage([[work_file(tmp_path, "legacy.parquet")]], {"offset": 1})
    with pytest.raises(StageFormError, match="after staging local Parquet Files"):
        context.stage(tables_files, new_state)
    assert context.pending == [[]]
    assert context.stream_state == {"offset": 1}


def test_tables_are_taken_from_any_iterable(tmp_path):
    context = make_context(tmp_path)
    sent = context.stage([(frame(index) for index in range(3))])
    assert [stats.name for stats in sent[0]] == ["0.0.t", "0.1.t", "0.2.t"]


def test_unstaging_deletes_what_was_sent_and_keeps_the_state(tmp_path):
    context = make_context(tmp_path)
    context.stage([[frame(1)]], {"offset": 1})
    context.stage([[frame(2)]])
    context.unstage()
    assert context.pending == [[]]
    (manifest,) = manifests(tmp_path)
    assert staged_files(tmp_path) == [
        f"{batch(manifest)}/0.0.s",
        f"{batch(manifest)}/0.0.t",
    ]
    assert context.stream_state == {"offset": 1}


def test_a_send_after_unstaging_starts_a_new_batch(tmp_path):
    context = make_context(tmp_path)
    context.stage([[frame(1)]])
    context.unstage()
    sent = context.stage([[frame(2)]], {"offset": 1})
    assert sent == [[StageFileStats(name="0.0.t", rows=1, bytes=sent[0][0].bytes)]]
    assert len(staged_files(tmp_path)) == 2


@pytest.mark.parametrize(
    "tables_files, error",
    [
        ([[frame(1)], [frame(2)]], TableCountError),
        ([[frame(1), "0.0.t"]], TableTypeError),
        ([[frame(1), Path("people.parquet")]], TableTypeError),
    ],
    ids=["one entry per table", "a name", "a file among frames"],
)
def test_a_send_that_does_not_match_the_tables_is_refused(
    tmp_path,
    tables_files,
    error,
):
    with pytest.raises(error):
        make_context(tmp_path).stage(tables_files)


@pytest.mark.parametrize(
    "tables_files, new_state, message",
    [
        (None, None, "Cannot stage without tables"),
        ([[Path("people.parquet")]], None, "without a new state"),
        (None, ["offset"], "Cannot commit a list"),
    ],
    ids=["nothing", "files without a state", "a state of another type"],
)
def test_a_stage_without_meaning_is_refused(tmp_path, tables_files, new_state, message):
    with pytest.raises(TypeError, match=message):
        make_context(tmp_path).stage(tables_files, new_state)


def ids(frame: pl.LazyFrame) -> list[int]:
    return frame.collect()["id"].to_list()


def test_a_sent_table_is_read_back_as_sent(tmp_path):
    context = make_context(tmp_path)
    ((sent,),) = context.stage([[frame(1, 2)]])
    assert ids(context.scan(sent)) == [1, 2]


def test_a_published_table_cannot_be_read_back(tmp_path):
    context = make_context(tmp_path)
    ((sent,),) = context.stage([[frame(1)]], {"offset": 1})
    with pytest.raises(NotPendingError, match="0.0.t"):
        context.scan(sent)


def test_unstaging_some_tables_keeps_the_others_pending(tmp_path):
    context = make_context(tmp_path, tables=2)
    sent = context.stage([[frame(1), frame(2)], [frame(3)]])
    context.unstage([sent[0][0], sent[1][0]])
    assert context.pending == [[sent[0][1]], []]
    context.stage(None, {"offset": 1})
    (manifest,) = manifests(tmp_path)
    folder = f"{FILES_FOLDER}/{batch(manifest)}"
    assert published(manifest) == [[f"{folder}/0.1.t"], []]
    assert staged_files(tmp_path) == [
        f"{batch(manifest)}/0.1.s",
        f"{batch(manifest)}/0.1.t",
    ]


def test_a_name_is_not_reused_after_unstaging_some_tables(tmp_path):
    context = make_context(tmp_path)
    first, second = context.stage([[frame(1), frame(2)]])[0]
    context.unstage([second])
    (third,) = context.stage([[frame(3)]])[0]
    assert third.name == "0.2.t"
    assert context.pending == [[first, third]]


def test_unstaging_a_table_that_is_not_pending_drops_nothing(tmp_path):
    context = make_context(tmp_path)
    (sent,) = context.stage([[frame(1)]])[0]
    stranger = StageFileStats(name="0.9.t", rows=1, bytes=1)
    with pytest.raises(NotPendingError, match="0.9.t"):
        context.unstage([sent, stranger])
    assert context.pending == [[sent]]
    assert len(staged_files(tmp_path)) == 2


def test_a_file_that_cannot_be_deleted_is_still_unstaged(tmp_path, monkeypatch, caplog):
    context = make_context(tmp_path)
    (sent,) = context.stage([[frame(1)]])[0]

    def unreachable(name: str) -> None:
        raise PermissionError("stage not mounted")

    monkeypatch.setattr(context.staging, "discard", unreachable)
    context.unstage([sent])
    assert context.pending == [[]]
    assert "Cannot delete" in caplog.text


@pytest.mark.parametrize(
    "call",
    [
        lambda context, stats: context.scan(stats),
        lambda context, stats: context.unstage([stats]),
    ],
    ids=["scan", "unstage some"],
)
def test_reading_back_or_unstaging_some_is_refused_once_files_were_staged(
    tmp_path,
    call,
):
    context = make_context(tmp_path)
    with pytest.warns(LegacyStreamStageWarning):
        context.stage([[work_file(tmp_path, "legacy.parquet")]], {"offset": 1})
    stats = StageFileStats(name="legacy.parquet", rows=2, bytes=1)
    with pytest.raises(StageFormError):
        call(context, stats)


def test_a_load_is_deduplicated_against_its_window_on_the_stage(tmp_path):
    context = make_context(tmp_path)
    load = context.stage([[frame(1, 2), frame(3)]])[0]
    window = context.stage([[frame(2, 4)]])[0]
    loaded = pl.concat([context.scan(stats) for stats in load])
    changes = pl.concat([context.scan(stats) for stats in window])
    kept = changes.join(loaded, on="id", how="anti")
    context.stage([[LazyFrameInput(kept)]])
    context.unstage(window)
    context.stage(None, {"offset": 1})
    (manifest,) = manifests(tmp_path)
    folder = f"{FILES_FOLDER}/{batch(manifest)}"
    assert published(manifest) == [
        [f"{folder}/0.0.t", f"{folder}/0.1.t", f"{folder}/0.3.t"],
    ]
    stage = tmp_path / "stage"
    rows = pl.concat(pl.scan_parquet(stage / name) for name in published(manifest)[0])
    assert sorted(ids(rows)) == [1, 2, 3, 4]
