#
# Copyright 2026 Tabs Data Inc.
#

"""Tests for `_lib/stream_src_ctx.py`.

Covers:
  * `_resolve_local_path`: file:// happy + non-file:// rejection
  * `_atomic_write_text`: tmp + rename, parent-mkdir
  * `_load_state`: missing file, malformed JSON, non-dict, envelope-mismatch,
    happy round-trip
  * `StreamerPluginCtx` construction: scheme rejection, initial state
  * `StreamerPluginCtx.stage`:
    - dict branch (Tabsdata-owned state): files moved to `files/`, manifest
      in `manifests/`, state envelope persisted with the manifest's TS as
      external_id
    - lambda branch: callback invoked first, then empty-dict marker persisted
    - missing input file raises FileNotFoundError
    - `first_stage` flips after first call
    - back-to-back stages produce distinct manifests
  * `mark_inactive`: flips `active` to False
"""

from __future__ import annotations

import json
from pathlib import Path

import pytest

from tabsdatak.server._tdrunner.function._lib.request import Location
from tabsdatak.server._tdrunner.function._lib.stage_layout import (
    FILES_SUBDIR,
    MANIFESTS_SUBDIR,
    STREAM_STATE_API_VERSION,
    STREAM_STATE_FILENAME,
    STREAM_STATE_KIND,
    manifest_files_dir,
)
from tabsdatak.server._tdrunner.function._lib.stream_src_ctx import (
    StreamerPluginCtx,
    UnsupportedStageSchemeError,
    _atomic_write_text,
    _load_state,
    _resolve_local_path,
)

pytestmark = [pytest.mark.unit, pytest.mark.tabsdatak]


# --- helpers --------------------------------------------------------------


_TABSDATA_VERSION = "2.0.0-test"


def _file_location(path: Path) -> Location:
    return Location(uri=f"file://{path}")


def _make_ctx(
    *,
    tmp_path: Path,
    flush_interval_mins: int = 5,
) -> StreamerPluginCtx:
    """Build a ctx whose work_dir + stage_root are siblings under tmp_path."""
    work_dir = tmp_path / "work"
    work_dir.mkdir(parents=True, exist_ok=True)
    stage = tmp_path / "stage"
    stage.mkdir(parents=True, exist_ok=True)
    return StreamerPluginCtx(
        work_dir=work_dir,
        collection_id="C-1",
        collection_name="c",
        function_id="F-1",
        function_name="stream_people",
        function_stage=_file_location(stage),
        flush_interval_mins=flush_interval_mins,
        tabsdata_version=_TABSDATA_VERSION,
    )


def _write_parquet_stub(path: Path, payload: bytes = b"PAR1stub") -> Path:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_bytes(payload)
    return path


# --- _resolve_local_path --------------------------------------------------


def test_resolve_local_path_accepts_file_uri(tmp_path):
    p = _resolve_local_path(_file_location(tmp_path))
    assert p == tmp_path


@pytest.mark.parametrize("scheme", ["s3", "az", "gcs", "http"])
def test_resolve_local_path_rejects_non_file_schemes(scheme):
    """The streamer is co-located with its stage volume today;
    non-file schemes will land once cloud-stage deployments do."""
    loc = Location(uri=f"{scheme}://bucket/stage")
    with pytest.raises(UnsupportedStageSchemeError, match=scheme):
        _resolve_local_path(loc)


# --- _atomic_write_text ---------------------------------------------------


def test_atomic_write_text_creates_parent(tmp_path):
    target = tmp_path / "a" / "b" / "out.txt"
    _atomic_write_text(target, "hello")
    assert target.read_text() == "hello"


def test_atomic_write_text_no_tmp_leftover(tmp_path):
    """tmp + replace must leave no `.tmp` sibling after success --
    matters because the sidecar's deletion sweeper would otherwise
    have to know about an interim file shape."""
    target = tmp_path / "out.json"
    _atomic_write_text(target, "{}")
    siblings = list(tmp_path.iterdir())
    assert siblings == [target]


def test_atomic_write_text_overwrites_existing(tmp_path):
    target = tmp_path / "out.txt"
    target.write_text("old")
    _atomic_write_text(target, "new")
    assert target.read_text() == "new"


# --- _load_state ----------------------------------------------------------


def test_load_state_returns_none_when_file_missing(tmp_path):
    """The streamer hasn't run before; ctx.stream_state will be None →
    plugin's consumption loop will use config.initial_values."""
    assert _load_state(tmp_path) is None


def test_load_state_returns_none_on_malformed_json(tmp_path, caplog):
    """Corrupt files don't lock the pod into a fail-loop -- fall back to
    first-stage semantics and log."""
    (tmp_path / STREAM_STATE_FILENAME).write_text("not-json{")
    assert _load_state(tmp_path) is None


def test_load_state_returns_none_on_non_dict_top_level(tmp_path):
    (tmp_path / STREAM_STATE_FILENAME).write_text("[1, 2, 3]")
    assert _load_state(tmp_path) is None


def test_load_state_returns_none_on_envelope_mismatch(tmp_path):
    """e.g. an envelope from a future apiVersion -- treat as unknown
    schema, fall back to first-stage. The plugin's recovery path is
    config.initial_values."""
    (tmp_path / STREAM_STATE_FILENAME).write_text(
        json.dumps({"kind": "streamState", "apiVersion": 999, "state": {}})
    )
    assert _load_state(tmp_path) is None


def test_load_state_returns_view_for_valid_envelope(tmp_path):
    envelope = {
        "kind": "streamState",
        "apiVersion": 1,
        "tabsdataVersion": "2.0.0",
        "externalId": "2026-06-16T10-30-12.345",
        "state": {"offset": 42},
    }
    (tmp_path / STREAM_STATE_FILENAME).write_text(json.dumps(envelope))
    view = _load_state(tmp_path)
    assert view is not None
    assert view.state == {"offset": 42}
    assert view.external_id == "2026-06-16T10-30-12.345"
    assert view.tabsdata_version == "2.0.0"


def test_load_state_returns_view_for_empty_state_marker(tmp_path):
    """The first-invocation marker (state={}) round-trips as a valid
    view; downstream the plugin distinguishes None vs {} on its own."""
    envelope = {
        "kind": "streamState",
        "apiVersion": 1,
        "tabsdataVersion": "2.0.0",
        "externalId": "2026-06-16T10-30-12.345",
        "state": {},
    }
    (tmp_path / STREAM_STATE_FILENAME).write_text(json.dumps(envelope))
    view = _load_state(tmp_path)
    assert view is not None
    assert view.state == {}


# --- StreamerPluginCtx construction ---------------------------------------


def test_ctx_construction_succeeds_for_file_stage(tmp_path):
    ctx = _make_ctx(tmp_path=tmp_path)
    assert ctx.active is True
    assert ctx.first_stage is True
    assert ctx.stream_state is None  # no prior state file
    assert ctx.flush_interval_mins == 5


def test_ctx_construction_rejects_non_file_stage(tmp_path):
    work_dir = tmp_path / "work"
    work_dir.mkdir()
    with pytest.raises(UnsupportedStageSchemeError):
        StreamerPluginCtx(
            work_dir=work_dir,
            collection_id="C-1",
            collection_name="c",
            function_id="F-1",
            function_name="stream_people",
            function_stage=Location(uri="s3://bucket/stage"),
            flush_interval_mins=5,
            tabsdata_version=_TABSDATA_VERSION,
        )


def test_ctx_construction_loads_existing_state(tmp_path):
    """Pre-seed `<stage>/stream-state.json` before ctx construction --
    `stream_state` returns the inner state from the envelope."""
    stage = tmp_path / "stage"
    stage.mkdir()
    (stage / STREAM_STATE_FILENAME).write_text(
        json.dumps({
            "kind": STREAM_STATE_KIND,
            "apiVersion": STREAM_STATE_API_VERSION,
            "tabsdataVersion": "older-version",
            "externalId": "2026-06-16T10-30-12.345",
            "state": {"offset": 100},
        })
    )
    work_dir = tmp_path / "work"
    work_dir.mkdir()
    ctx = StreamerPluginCtx(
        work_dir=work_dir,
        collection_id="C-1",
        collection_name="c",
        function_id="F-1",
        function_name="stream_people",
        function_stage=_file_location(stage),
        flush_interval_mins=5,
        tabsdata_version=_TABSDATA_VERSION,
    )
    assert ctx.stream_state == {"offset": 100}


# --- stage(): dict (Tabsdata-owned state) branch --------------------------


def test_stage_dict_state_moves_files_to_per_manifest_files_subdir(tmp_path):
    """The parquets the plugin produced in work_dir must end up under
    `<stage>/files/<TS>/` -- a per-batch subdir named for the manifest's
    timestamp, where the publisher's ManifestStageSrcPlugin (via NU5's
    conn pointing at stage_root) will resolve them."""
    ctx = _make_ctx(tmp_path=tmp_path)
    work_dir = tmp_path / "work"
    f1 = _write_parquet_stub(work_dir / "people-1.parquet")
    f2 = _write_parquet_stub(work_dir / "people-2.parquet")
    ctx.stage([[f1, f2]], {"offset": 1})
    manifest_path = next(
        (tmp_path / "stage" / MANIFESTS_SUBDIR).glob("stream-manifest-*.json")
    )
    external_id = manifest_path.stem[len("stream-manifest-") :]
    batch_dir = manifest_files_dir(tmp_path / "stage", external_id)
    assert (batch_dir / "people-1.parquet").is_file()
    assert (batch_dir / "people-2.parquet").is_file()
    # nothing landed flat under files/
    assert not (tmp_path / "stage" / FILES_SUBDIR / "people-1.parquet").exists()
    # work_dir entries gone
    assert not f1.exists()
    assert not f2.exists()


def test_stage_dict_state_writes_manifest_under_manifests_subdir(tmp_path):
    ctx = _make_ctx(tmp_path=tmp_path)
    work_dir = tmp_path / "work"
    f1 = _write_parquet_stub(work_dir / "data.parquet")
    ctx.stage([[f1]], {})
    manifests_dir = tmp_path / "stage" / MANIFESTS_SUBDIR
    manifests = list(manifests_dir.glob("stream-manifest-*.json"))
    assert len(manifests) == 1
    # Manifest content holds the path *relative to stage_root* --
    # NU5's publisher conn points at stage_root and resolves these.
    content = json.loads(manifests[0].read_text())
    external_id = manifests[0].stem[len("stream-manifest-") :]
    assert content == [[f"files/{external_id}/data.parquet"]]


def test_stage_dict_state_persists_state_envelope(tmp_path):
    """The state file gets the full envelope with the inner state,
    the externalId derived from the manifest's TS, and tabsdataVersion."""
    ctx = _make_ctx(tmp_path=tmp_path)
    work_dir = tmp_path / "work"
    f1 = _write_parquet_stub(work_dir / "data.parquet")
    ctx.stage([[f1]], {"offset": 42, "topic": "people"})

    state_path = tmp_path / "stage" / STREAM_STATE_FILENAME
    assert state_path.is_file()
    envelope = json.loads(state_path.read_text())
    assert envelope["kind"] == STREAM_STATE_KIND
    assert envelope["apiVersion"] == STREAM_STATE_API_VERSION
    assert envelope["tabsdataVersion"] == _TABSDATA_VERSION
    assert envelope["state"] == {"offset": 42, "topic": "people"}
    # externalId matches the manifest filename's TS portion
    manifest_path = next(
        (tmp_path / "stage" / MANIFESTS_SUBDIR).glob("stream-manifest-*.json")
    )
    expected_id = manifest_path.stem[len("stream-manifest-") :]
    assert envelope["externalId"] == expected_id


def test_stage_dict_state_updates_stream_state_property(tmp_path):
    ctx = _make_ctx(tmp_path=tmp_path)
    work_dir = tmp_path / "work"
    f1 = _write_parquet_stub(work_dir / "data.parquet")
    ctx.stage([[f1]], {"offset": 99})
    assert ctx.stream_state == {"offset": 99}


# --- stage(): lambda branch -----------------------------------------------


def test_stage_lambda_invokes_callback_before_persisting_marker(tmp_path):
    """At-least-once semantics: the callable is the side effect of
    committing to the external system; the marker only goes down
    *after* it returns so a callback failure leaves us in first-run
    mode on next start."""
    ctx = _make_ctx(tmp_path=tmp_path)
    work_dir = tmp_path / "work"
    f1 = _write_parquet_stub(work_dir / "data.parquet")

    state_path = tmp_path / "stage" / STREAM_STATE_FILENAME
    callback_calls: list[bool] = []

    def _commit():
        # When the callback runs, the marker file must NOT yet exist.
        callback_calls.append(state_path.is_file())

    ctx.stage([[f1]], _commit)
    assert callback_calls == [False]  # callback ran with no prior marker
    assert state_path.is_file()  # marker landed after callback returned


def test_stage_lambda_persists_empty_dict_marker(tmp_path):
    ctx = _make_ctx(tmp_path=tmp_path)
    work_dir = tmp_path / "work"
    f1 = _write_parquet_stub(work_dir / "data.parquet")
    ctx.stage([[f1]], lambda: None)
    envelope = json.loads((tmp_path / "stage" / STREAM_STATE_FILENAME).read_text())
    assert envelope["state"] == {}
    assert envelope["tabsdataVersion"] == _TABSDATA_VERSION


def test_stage_lambda_exception_leaves_no_marker(tmp_path):
    """If the external-system commit fails, the marker is not written
    -- next start the plugin sees ctx.state is None and seeds from
    config.initial_values (replay; at-least-once)."""
    ctx = _make_ctx(tmp_path=tmp_path)
    work_dir = tmp_path / "work"
    f1 = _write_parquet_stub(work_dir / "data.parquet")

    def _explode():
        raise RuntimeError("external system unreachable")

    with pytest.raises(RuntimeError, match="external system unreachable"):
        ctx.stage([[f1]], _explode)
    state_path = tmp_path / "stage" / STREAM_STATE_FILENAME
    assert not state_path.is_file()


def test_stage_lambda_updates_stream_state_to_empty_dict(tmp_path):
    """After the marker lands, ``stream_state`` returns the empty dict
    (distinguishable from ``None`` -- the plugin's start-up code
    branches on ``ctx.state is None``)."""
    ctx = _make_ctx(tmp_path=tmp_path)
    work_dir = tmp_path / "work"
    f1 = _write_parquet_stub(work_dir / "data.parquet")
    ctx.stage([[f1]], lambda: None)
    assert ctx.stream_state == {}


# --- stage(): general behaviour -------------------------------------------


def test_stage_missing_input_file_raises(tmp_path):
    """Plugin contract: paths in ``tables_files`` must exist when
    stage() is called. We surface FileNotFoundError so the streamer
    pod's outer try/except logs and exits non-zero -- silently
    skipping would mask a buggy plugin."""
    ctx = _make_ctx(tmp_path=tmp_path)
    missing = tmp_path / "work" / "does-not-exist.parquet"
    with pytest.raises(FileNotFoundError, match="does-not-exist"):
        ctx.stage([[missing]], {})


def test_stage_flips_first_stage_flag(tmp_path):
    ctx = _make_ctx(tmp_path=tmp_path)
    assert ctx.first_stage is True
    work_dir = tmp_path / "work"
    f1 = _write_parquet_stub(work_dir / "data.parquet")
    ctx.stage([[f1]], {})
    assert ctx.first_stage is False


def test_stage_multiple_batches_produce_distinct_manifests(tmp_path):
    """Two stage() calls produce two manifests with monotonically
    increasing TS in their filenames -- guards against a regression
    where the runtime accidentally reuses a single ts."""
    ctx = _make_ctx(tmp_path=tmp_path)
    work_dir = tmp_path / "work"
    f1 = _write_parquet_stub(work_dir / "batch-1.parquet")
    ctx.stage([[f1]], {"n": 1})
    f2 = _write_parquet_stub(work_dir / "batch-2.parquet")
    ctx.stage([[f2]], {"n": 2})
    manifests = sorted(
        (tmp_path / "stage" / MANIFESTS_SUBDIR).glob("stream-manifest-*.json")
    )
    assert len(manifests) == 2
    assert manifests[0].name < manifests[1].name


def test_stage_multiple_batches_use_distinct_files_subdirs(tmp_path):
    """Each batch's parquets land in their own files/<TS>/ subdir keyed
    to that batch's manifest, so two batches never share a directory --
    this is what lets the sidecar reclaim one batch with a single rmdir."""
    ctx = _make_ctx(tmp_path=tmp_path)
    work_dir = tmp_path / "work"
    f1 = _write_parquet_stub(work_dir / "batch-1.parquet")
    ctx.stage([[f1]], {"n": 1})
    f2 = _write_parquet_stub(work_dir / "batch-2.parquet")
    ctx.stage([[f2]], {"n": 2})
    manifests = sorted(
        (tmp_path / "stage" / MANIFESTS_SUBDIR).glob("stream-manifest-*.json")
    )
    ids = [m.stem[len("stream-manifest-") :] for m in manifests]
    dir1 = manifest_files_dir(tmp_path / "stage", ids[0])
    dir2 = manifest_files_dir(tmp_path / "stage", ids[1])
    assert dir1 != dir2
    assert (dir1 / "batch-1.parquet").is_file()
    assert (dir2 / "batch-2.parquet").is_file()


def test_stage_handles_empty_slots(tmp_path):
    """A slot with zero files this batch is valid (e.g. one of N
    output_tables produced nothing this rollover). The manifest must
    still reflect the slot's existence with an empty list."""
    ctx = _make_ctx(tmp_path=tmp_path)
    work_dir = tmp_path / "work"
    f1 = _write_parquet_stub(work_dir / "data.parquet")
    ctx.stage([[f1], []], {})
    manifests = list((tmp_path / "stage" / MANIFESTS_SUBDIR).glob("*.json"))
    assert len(manifests) == 1
    content = json.loads(manifests[0].read_text())
    external_id = manifests[0].stem[len("stream-manifest-") :]
    assert content == [[f"files/{external_id}/data.parquet"], []]


# --- mark_inactive --------------------------------------------------------


def test_mark_inactive_flips_active(tmp_path):
    ctx = _make_ctx(tmp_path=tmp_path)
    assert ctx.active is True
    ctx.mark_inactive()
    assert ctx.active is False


def test_mark_inactive_is_idempotent(tmp_path):
    """The streamer command may install handlers for both SIGTERM and
    SIGINT; if both fire, the second mark_inactive shouldn't error."""
    ctx = _make_ctx(tmp_path=tmp_path)
    ctx.mark_inactive()
    ctx.mark_inactive()
    assert ctx.active is False
