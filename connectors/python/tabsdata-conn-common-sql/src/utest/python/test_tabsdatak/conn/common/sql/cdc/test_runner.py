#
#  Copyright 2026 Tabsdata Inc.
#

"""Unit tests for the dialect-agnostic CDC run loop
(:mod:`tabsdatak.conn.common.sql.cdc._runner`): ``_publish``,
``_run_loop`` (seeding, threshold vs interval flush, graceful vs
interrupted/errored shutdown), ``_setup_signal_handlers`` and the
``CdcRunConfig`` defaults. The sentinels are faked out so the loop's
orchestration is tested in isolation, without a database or parquet."""

from __future__ import annotations

import threading
from pathlib import Path

import pytest

from tabsdatak.conn.common.sql.cdc import _runner
from tabsdatak.conn.common.sql.cdc._ingester import CDC_INITIAL_LOAD_STATE_KEY
from tabsdatak.conn.common.sql.cdc._runner import (
    CdcRunConfig,
    _checkpoint,
    _initial_load_pending,
    _publish,
    _run_loop,
    _setup_signal_handlers,
)
from tabsdatak.conn.common.sql.cdc.constants import (
    DEFAULT_BUFFER_MAX_BYTES,
    DEFAULT_BUFFER_MAX_ROWS,
    DEFAULT_BUFFER_MAX_SECS,
    DEFAULT_FETCH_SIZE,
    DEFAULT_MAX_LARGE_TEXT_LENGTH,
    DEFAULT_POLL_INTERVAL_SECONDS,
    DEFAULT_TRIGGER_MAX_BYTES,
    DEFAULT_TRIGGER_MAX_ROWS,
    DEFAULT_TRIGGER_MAX_SECS,
)
from tabsdatak.conn.common.sql.cdc.error import SqlCdcErrorCode, SqlCdcException

# --- fakes --------------------------------------------------------------


class FakeCtx:
    """Duck-typed :class:`StreamSrcPluginCtx`. ``active`` is driven by a
    scripted sequence so the loop runs a controlled number of times."""

    def __init__(
        self,
        *,
        active_values,
        flush_interval_mins=0,
        first_stage=True,
        stream_state=None,
    ):
        self._active = iter(active_values)
        self.flush_interval_mins = flush_interval_mins
        self.first_stage = first_stage
        self.stream_state = stream_state
        self.work_dir = Path("/tmp")
        self.staged: list[tuple] = []
        self.checkpoints: list = []
        self.stage_raises = False

    @property
    def active(self) -> bool:
        try:
            return next(self._active)
        except StopIteration:
            return False

    def stage(self, tables_files, new_state) -> None:
        if self.stage_raises:
            raise RuntimeError("boom-stage")
        self.staged.append((tables_files, new_state))

    def checkpoint(self, new_state) -> None:
        self.checkpoints.append(new_state)


class FakeDisk:
    def __init__(self, *, manifest, due_force=False, due_normal=False):
        self._manifest = manifest
        self.has_data = bool(manifest and any(manifest))
        self._force = due_force
        self._normal = due_normal
        self.cleared = 0

    def reset_timer(self) -> None:
        pass

    def due(self, force: bool = False) -> bool:
        return self._force if force else self._normal

    def manifest(self):
        return self._manifest

    def clear(self) -> None:
        self.cleared += 1


class FakeBuffer:
    def __init__(self):
        self.offset = object()
        self.flushed = 0
        self.processed = 0

    def reset_timer(self) -> None:
        pass

    def process_changes(self, changes) -> None:
        self.processed += 1

    def expire(self) -> None:
        pass

    def flush(self) -> None:
        self.flushed += 1


class _FakeInitialLoadSession:
    def __init__(self, window_end=None):
        self.closed = False
        self.engine = object()
        self.window_end = window_end

    def close(self) -> None:
        self.closed = True


class FakeIngester:
    def __init__(
        self,
        *,
        resume_value=None,
        poll_raises=False,
        open_raises=False,
        window_end=None,
    ):
        self._resume_value = resume_value
        self._window_end = window_end
        self._poll_raises = poll_raises
        self._open_raises = open_raises
        self.resume_args = None
        self.seeded = "UNSET"
        self.session = object()
        self.closed = False
        self.commit_state_arg = None
        self.commit_state_marker = object()
        self.committed_offsets: list = []
        self.tracked_tables: dict = {"s.t": object()}
        self.checkpoint = None
        self.settled = 0

    def resume_offset(self, *, first_stage, stream_state):
        self.resume_args = (first_stage, stream_state)
        return self._resume_value

    def seed_offset(self, offset) -> None:
        self.seeded = offset

    def watch_position(self, checkpoint) -> None:
        self.checkpoint = checkpoint

    def settle(self) -> None:
        self.settled += 1

    def open_session(self):
        if self._open_raises:
            raise RuntimeError("boom-open")
        return self.session

    def open_initial_load(self):
        self.initial_load_session = _FakeInitialLoadSession(self._window_end)
        return self.initial_load_session

    def harvest_tables(self, *, session, cdc_format):
        return {"s.t": object()}

    def collect_tables(self, *, session, cdc_format):
        return self.tracked_tables

    def base_offset(self):
        return object()

    def poll_changes(self, session):
        if self._poll_raises:
            raise RuntimeError("boom-poll")
        return []

    def close_session(self, session) -> None:
        self.closed = True

    def commit_state(self, offset):
        self.commit_state_arg = offset
        return self.commit_state_marker

    def commit_offset(self, offset) -> None:
        self.committed_offsets.append(offset)


def _patch_sentinels(monkeypatch, disk, buffer):
    monkeypatch.setattr(_runner, "FileLedger", lambda **kw: disk)
    monkeypatch.setattr(_runner, "DiskSentinel", lambda **kw: disk)
    monkeypatch.setattr(_runner, "BufferSentinel", lambda **kw: buffer)


def _cfg(**overrides) -> CdcRunConfig:
    # poll_interval_seconds=0 so the loop's stop_signal.wait() returns
    # immediately and the test never blocks.
    base = dict(poll_interval_seconds=0)
    base.update(overrides)
    return CdcRunConfig(**base)


# --- _publish -----------------------------------------------------------


def test_publish_empty_manifest_does_not_stage():
    """An all-empty manifest must not stage, clear, or commit an offset
    (otherwise a no-data batch would advance the resume point)."""
    ctx = FakeCtx(active_values=[])
    disk = FakeDisk(manifest=[[], []])
    buffer = FakeBuffer()
    ingester = FakeIngester()

    assert _publish(ctx, disk, disk, buffer, ingester) is False
    assert ctx.staged == []
    assert disk.cleared == 0


def test_publish_stages_manifest_and_commits_offset():
    ctx = FakeCtx(active_values=[])
    files = [[Path("t0.parquet")], []]
    disk = FakeDisk(manifest=files)
    buffer = FakeBuffer()
    ingester = FakeIngester()

    assert _publish(ctx, disk, disk, buffer, ingester) is True
    assert ctx.staged == [(files, ingester.commit_state_marker)]
    assert ingester.commit_state_arg is buffer.offset
    assert ingester.committed_offsets == [buffer.offset]
    assert disk.cleared == 1


def test_a_stage_does_not_cover_changes_still_in_memory():
    """The disk sentinel's thresholds can stage while the buffer still holds
    changes it has not written: the staged offset must stop at what is on
    disk, or a stop before the next stage loses those changes."""
    from datetime import datetime

    from tabsdatak.conn.common.sql.cdc._model import CdcDataChange
    from tabsdatak.conn.common.sql.cdc._sentinel.buffer import BufferSentinel

    class Positions:
        def __init__(self, applied=()):
            self.applied = list(applied)

        def update(self, other) -> None:
            self.applied.extend(other.applied)

    def change(position: str) -> CdcDataChange:
        return CdcDataChange(
            operation="i",
            transaction="tx",
            sequence=1,
            timestamp=datetime(2026, 1, 1),
            table="s.t",
            schema=None,
            new_values={"id": 1},
            old_values=None,
            offset=Positions([position]),
        )

    class SilentEncoder:
        def encode(self, table, rows):
            return []

    disk = FakeDisk(manifest=[[Path("t0.parquet")]])
    buffer = BufferSentinel(
        ledger=disk,
        encoder=SilentEncoder(),
        max_rows=1_000,
        max_bytes=None,
        max_secs=10_000,
        tables={"s.t": None},
        offset=Positions(),
    )
    buffer.process_data_change(change("written"))
    buffer.flush()
    buffer.process_data_change(change("in-memory"))
    ingester = FakeIngester()

    assert _publish(FakeCtx(active_values=[]), disk, disk, buffer, ingester) is True
    assert ingester.commit_state_arg.applied == ["written"]
    assert ingester.committed_offsets[0].applied == ["written"]


def test_publish_does_not_commit_offset_when_stage_fails():
    """The source is acknowledged only for a batch Tabsdata has made
    durable; a failed stage must leave the source cursor where it was."""
    ctx = FakeCtx(active_values=[])
    ctx.stage_raises = True
    disk = FakeDisk(manifest=[[Path("t0.parquet")]])
    buffer = FakeBuffer()
    ingester = FakeIngester()

    with pytest.raises(RuntimeError, match="boom-stage"):
        _publish(ctx, disk, disk, buffer, ingester)

    assert ingester.committed_offsets == []
    assert disk.cleared == 0


# --- _run_loop: seeding -------------------------------------------------


def test_run_loop_seeds_resume_offset_from_ctx(monkeypatch):
    disk = FakeDisk(manifest=[[]], due_force=False)
    buffer = FakeBuffer()
    _patch_sentinels(monkeypatch, disk, buffer)
    sentinel_offset = object()
    ingester = FakeIngester(resume_value=sentinel_offset)
    ctx = FakeCtx(active_values=[False], first_stage=False, stream_state={"x": 1})

    _run_loop(ctx=ctx, ingester=ingester, cfg=_cfg())

    assert ingester.resume_args == (False, {"x": 1})
    assert ingester.seeded is sentinel_offset
    assert ingester.closed is True


# --- _run_loop: flush triggers ------------------------------------------


def test_run_loop_threshold_flush_stages_once(monkeypatch):
    disk = FakeDisk(manifest=[[Path("f")]], due_normal=True)
    buffer = FakeBuffer()
    _patch_sentinels(monkeypatch, disk, buffer)
    ingester = FakeIngester()
    ctx = FakeCtx(active_values=[True, False])

    _run_loop(ctx=ctx, ingester=ingester, cfg=_cfg())

    assert len(ctx.staged) == 1


def test_run_loop_interval_flush_stages_when_interval_elapses(monkeypatch):
    """With size/row thresholds never firing, an elapsed
    ``flush_interval_mins`` alone must trigger a stage."""
    disk = FakeDisk(
        manifest=[[Path("f")]],
        due_normal=False,
        due_force=False,
    )
    buffer = FakeBuffer()
    _patch_sentinels(monkeypatch, disk, buffer)

    # last_flush is set from the first time.time(); the interval check on
    # the next iteration sees +120s, exceeding the 60s (1 min) interval.
    clock = iter([1000.0, 1120.0, 1120.0, 1120.0, 1120.0])
    monkeypatch.setattr(_runner.time, "time", lambda: next(clock))

    ingester = FakeIngester()
    ctx = FakeCtx(active_values=[True, False], flush_interval_mins=1)

    _run_loop(ctx=ctx, ingester=ingester, cfg=_cfg())

    assert len(ctx.staged) == 1


def test_run_loop_interval_flush_skipped_when_no_data(monkeypatch):
    """The interval must not fire on an empty sentinel (no no-data
    publishes)."""
    disk = FakeDisk(manifest=[[]], due_normal=False)
    disk.has_data = False
    buffer = FakeBuffer()
    _patch_sentinels(monkeypatch, disk, buffer)
    clock = iter([1000.0, 1120.0, 1120.0, 1120.0, 1120.0])
    monkeypatch.setattr(_runner.time, "time", lambda: next(clock))
    ingester = FakeIngester()
    ctx = FakeCtx(active_values=[True, False], flush_interval_mins=1)

    _run_loop(ctx=ctx, ingester=ingester, cfg=_cfg())

    assert ctx.staged == []


# --- _run_loop: shutdown ------------------------------------------------


def test_run_loop_graceful_end_flushes_trailing_batch(monkeypatch):
    """A clean end-of-stream (``ctx.active`` -> False, no signal/error)
    flushes the trailing partial batch."""
    disk = FakeDisk(manifest=[[Path("f")]], due_force=True)
    buffer = FakeBuffer()
    _patch_sentinels(monkeypatch, disk, buffer)
    ingester = FakeIngester()
    ctx = FakeCtx(active_values=[False])

    _run_loop(ctx=ctx, ingester=ingester, cfg=_cfg())

    assert buffer.flushed == 1
    assert len(ctx.staged) == 1
    assert ingester.closed is True


def test_run_loop_interrupted_skips_trailing_flush(monkeypatch):
    """A signalled exit leaves the buffer for the next run (no trailing
    flush) but still closes the session."""
    preset = threading.Event()
    preset.set()
    monkeypatch.setattr(_runner, "_setup_signal_handlers", lambda: preset)

    disk = FakeDisk(manifest=[[Path("f")]], due_force=True)
    buffer = FakeBuffer()
    _patch_sentinels(monkeypatch, disk, buffer)
    ingester = FakeIngester()
    ctx = FakeCtx(active_values=[True, True])

    _run_loop(ctx=ctx, ingester=ingester, cfg=_cfg())

    assert buffer.flushed == 0
    assert ctx.staged == []
    assert ingester.closed is True


def test_run_loop_errored_skips_trailing_flush_but_closes(monkeypatch):
    """An error propagates, the trailing batch is left for resume, and
    the session is still closed in the finally."""
    disk = FakeDisk(manifest=[[Path("f")]], due_force=True)
    buffer = FakeBuffer()
    _patch_sentinels(monkeypatch, disk, buffer)
    ingester = FakeIngester(poll_raises=True)
    ctx = FakeCtx(active_values=[True])

    with pytest.raises(RuntimeError, match="boom-poll"):
        _run_loop(ctx=ctx, ingester=ingester, cfg=_cfg())

    assert buffer.flushed == 0
    assert ctx.staged == []
    assert ingester.closed is True


# --- _run_loop: positions recorded between stages -------------------------


class _OwnedOffsetIngester(FakeIngester):
    def commit_state(self, offset):
        return {"cdc.offset": offset}


def test_run_loop_hands_the_ingester_a_checkpoint_that_records_state(monkeypatch):
    _patch_sentinels(monkeypatch, FakeDisk(manifest=[]), FakeBuffer())
    ingester = _OwnedOffsetIngester()
    ctx = FakeCtx(
        active_values=[False],
        first_stage=False,
        stream_state={"cdc.offset": "staged"},
    )

    _run_loop(ctx=ctx, ingester=ingester, cfg=_cfg())
    ingester.checkpoint("moved")

    assert ctx.checkpoints == [{"cdc.offset": "moved"}]
    assert ctx.staged == []


def test_a_checkpoint_keeps_the_record_of_a_done_initial_load():
    ctx = FakeCtx(
        active_values=[],
        first_stage=False,
        stream_state={"cdc.offset": "staged", CDC_INITIAL_LOAD_STATE_KEY: False},
    )
    _checkpoint(ctx, _OwnedOffsetIngester(), _cfg(initial_load=True), "moved")
    assert ctx.checkpoints == [
        {"cdc.offset": "moved", CDC_INITIAL_LOAD_STATE_KEY: False}
    ]


@pytest.mark.parametrize(
    ("first_stage", "stream_state", "initial_load"),
    [
        (True, None, False),
        (False, {"cdc.offset": "staged"}, True),
        (False, {CDC_INITIAL_LOAD_STATE_KEY: True}, True),
    ],
    ids=["before-a-first-stage", "load-never-staged", "load-owed"],
)
def test_there_is_no_checkpoint_without_a_state_to_move_on(
    first_stage, stream_state, initial_load
):
    """Until a stage, or while an initial load is owed, the next run starts
    over from the configuration, so there is no resume state to move on."""
    ctx = FakeCtx(active_values=[], first_stage=first_stage, stream_state=stream_state)
    _checkpoint(ctx, _OwnedOffsetIngester(), _cfg(initial_load=initial_load), "moved")
    assert ctx.checkpoints == []


@pytest.mark.parametrize(
    ("ingester", "offset"),
    [(_OwnedOffsetIngester(), None), (FakeIngester(), "moved")],
    ids=["no-offset", "source-owned-offset"],
)
def test_a_checkpoint_without_a_state_to_record_is_a_bug(ingester, offset):
    ctx = FakeCtx(
        active_values=[], first_stage=False, stream_state={"cdc.offset": "staged"}
    )
    with pytest.raises(SqlCdcException) as ei:
        _checkpoint(ctx, ingester, _cfg(), offset)
    assert ei.value.api_error is SqlCdcErrorCode.CDC_30
    assert ctx.checkpoints == []


def test_a_clean_end_settles_the_position_after_the_last_stage(monkeypatch):
    disk = FakeDisk(manifest=[[Path("f")]], due_force=True)
    _patch_sentinels(monkeypatch, disk, FakeBuffer())
    ingester = FakeIngester()
    ctx = FakeCtx(active_values=[False])

    _run_loop(ctx=ctx, ingester=ingester, cfg=_cfg())

    assert (len(ctx.staged), ingester.settled, ingester.closed) == (1, 1, True)


def test_a_signalled_stop_settles_the_position(monkeypatch):
    preset = threading.Event()
    preset.set()
    monkeypatch.setattr(_runner, "_setup_signal_handlers", lambda: preset)
    _patch_sentinels(monkeypatch, FakeDisk(manifest=[]), FakeBuffer())
    ingester = FakeIngester()

    _run_loop(ctx=FakeCtx(active_values=[True]), ingester=ingester, cfg=_cfg())

    assert ingester.settled == 1


def test_an_errored_run_does_not_settle(monkeypatch):
    """After an error the stream may no longer hold its position, so what
    the source reports then cannot be told to be the stream's own."""
    _patch_sentinels(monkeypatch, FakeDisk(manifest=[]), FakeBuffer())
    ingester = FakeIngester(poll_raises=True)

    with pytest.raises(RuntimeError, match="boom-poll"):
        _run_loop(ctx=FakeCtx(active_values=[True]), ingester=ingester, cfg=_cfg())

    assert (ingester.settled, ingester.closed) == (0, True)


def test_a_failed_settle_still_closes_the_session(monkeypatch):
    _patch_sentinels(monkeypatch, FakeDisk(manifest=[]), FakeBuffer())
    ingester = FakeIngester()
    ingester.settle = lambda: (_ for _ in ()).throw(RuntimeError("boom-settle"))

    _run_loop(ctx=FakeCtx(active_values=[False]), ingester=ingester, cfg=_cfg())

    assert ingester.closed is True


# --- _setup_signal_handlers ---------------------------------------------


def test_setup_signal_handlers_off_main_thread_is_noop():
    """Off the main thread the handler install is skipped (it would raise
    in CPython), returning a plain unset Event."""
    result = {}

    def worker():
        event = _setup_signal_handlers()
        result["event"] = event
        result["is_set"] = event.is_set()

    thread = threading.Thread(target=worker)
    thread.start()
    thread.join()

    assert isinstance(result["event"], threading.Event)
    assert result["is_set"] is False


# --- initial load -------------------------------------------------------


@pytest.mark.parametrize(
    "initial_load,stream_state,pending",
    [
        (False, None, False),
        (False, {CDC_INITIAL_LOAD_STATE_KEY: True}, False),
        (True, None, True),
        (True, {}, True),
        (True, {"cdc.offset": "x"}, True),
        (True, {CDC_INITIAL_LOAD_STATE_KEY: False}, False),
        (True, {CDC_INITIAL_LOAD_STATE_KEY: True}, True),
    ],
)
def test_initial_load_pending_decision(initial_load, stream_state, pending):
    """A load is owed while the source asks for one and no earlier run has
    recorded having staged it."""
    ctx = FakeCtx(active_values=[], stream_state=stream_state)
    cfg = _cfg(initial_load=initial_load)
    assert _initial_load_pending(ctx, cfg) is pending


def _loaded(monkeypatch, tmp_path, *, ctx, loader_raises=False, window_end=None):
    """Run ``_load_initially`` with the loader and sentinel faked out, so
    only the runner's own orchestration is under test."""
    manifest = [[tmp_path / "t0.parquet"], []]
    disk = FakeDisk(manifest=manifest)
    monkeypatch.setattr(_runner, "FileLedger", lambda **kw: disk)

    offset = object()

    class FakeLoader:
        def __init__(self, **kw):
            self.kw = kw

        def load(self, session):
            if loader_raises:
                raise RuntimeError("boom-load")
            return offset

    monkeypatch.setattr(_runner, "InitialLoader", FakeLoader)
    ingester = FakeIngester(window_end=window_end)
    ingester.commit_state_marker = {"cdc.offset": "o"}
    result = _runner._load_initially(
        ctx=ctx,
        ingester=ingester,
        cfg=_cfg(initial_load=True),
        large_column_policy=object(),
    )
    return result, offset, ingester, manifest


def test_initial_load_stages_once_with_offset_and_flag(monkeypatch, tmp_path):
    ctx = FakeCtx(active_values=[])
    ctx.work_dir = tmp_path
    result, offset, ingester, manifest = _loaded(monkeypatch, tmp_path, ctx=ctx)

    assert result == (offset, None)
    assert len(ctx.staged) == 1
    staged_files, staged_state = ctx.staged[0]
    assert staged_files == manifest
    # The resume offset and the "load done" flag are persisted together,
    # so a crash cannot leave one without the other.
    assert staged_state == {"cdc.offset": "o", CDC_INITIAL_LOAD_STATE_KEY: False}
    assert ingester.committed_offsets == [offset]


def test_initial_load_failure_stages_nothing(monkeypatch, tmp_path):
    ctx = FakeCtx(active_values=[])
    ctx.work_dir = tmp_path
    with pytest.raises(RuntimeError, match="boom-load"):
        _loaded(monkeypatch, tmp_path, ctx=ctx, loader_raises=True)
    assert ctx.staged == []


def test_initial_load_closes_the_snapshot_session(monkeypatch, tmp_path):
    ctx = FakeCtx(active_values=[])
    ctx.work_dir = tmp_path
    _, _, ingester, _ = _loaded(monkeypatch, tmp_path, ctx=ctx)
    assert ingester.initial_load_session.closed is True


def test_initial_load_requires_a_tabsdata_owned_offset(monkeypatch, tmp_path):
    """An ingester whose offset the source owns hands back a callable
    instead of a state dict; there is nowhere to record the load, so the
    runner refuses rather than repeating the snapshot on every restart."""
    ctx = FakeCtx(active_values=[])
    ctx.work_dir = tmp_path
    disk = FakeDisk(manifest=[[tmp_path / "t0.parquet"]])
    monkeypatch.setattr(_runner, "FileLedger", lambda **kw: disk)
    monkeypatch.setattr(
        _runner,
        "InitialLoader",
        lambda **kw: type("L", (), {"load": lambda self, session: object()})(),
    )
    ingester = FakeIngester()
    ingester.commit_state_marker = lambda: None

    with pytest.raises(SqlCdcException):
        _runner._load_initially(
            ctx=ctx,
            ingester=ingester,
            cfg=_cfg(initial_load=True),
            large_column_policy=object(),
        )
    assert ctx.staged == []


def test_a_load_with_a_window_is_left_unstaged(monkeypatch, tmp_path):
    """The load is staged with the window's changes, once they are read and
    deduplicated, so the load alone stages nothing."""
    ctx = FakeCtx(active_values=[])
    ctx.work_dir = tmp_path
    end = object()
    result, offset, ingester, manifest = _loaded(
        monkeypatch, tmp_path, ctx=ctx, window_end=end
    )
    start, pending = result
    assert start is offset
    assert pending.manifest == manifest
    assert pending.window_end is end
    assert ctx.staged == []
    assert ingester.committed_offsets == []


class _FakeRegistry:
    def version(self, table, ordinal):
        return ("version", table, ordinal)


def _with_window(monkeypatch, tmp_path, *, window):
    """Run the loop over a pending load, with the window read and the
    deduplication faked out."""
    disk = FakeDisk(manifest=[[]])
    buffer = FakeBuffer()
    _patch_sentinels(monkeypatch, disk, buffer)
    start, end = object(), object()
    load_file = tmp_path / "load.parquet"
    kept_file = tmp_path / "kept.parquet"
    pending = _runner._PendingLoad(manifest=[[load_file]], window_end=end)
    monkeypatch.setattr(_runner, "_load_initially", lambda **kw: (start, pending))
    monkeypatch.setattr(_runner, "_read_window", lambda **kw: window)
    deduplicated: list = []
    monkeypatch.setattr(
        _runner,
        "deduplicate_initial_load_files",
        lambda **kw: deduplicated.append(kw) or [[kept_file]],
    )
    ingester = FakeIngester()
    ingester.schema_registry = _FakeRegistry()
    ingester.commit_state_marker = {"cdc.offset": "end"}
    ctx = FakeCtx(active_values=[False], stream_state=None)
    ctx.work_dir = tmp_path
    _run_loop(ctx=ctx, ingester=ingester, cfg=_cfg(initial_load=True))
    return ctx, ingester, deduplicated, end, load_file, kept_file


def test_the_load_is_staged_with_what_deduplication_keeps(monkeypatch, tmp_path):
    window_file = tmp_path / "window.parquet"
    ctx, ingester, deduplicated, end, load_file, kept_file = _with_window(
        monkeypatch, tmp_path, window=[[window_file]]
    )
    assert ctx.staged == [
        (
            [[load_file, kept_file]],
            {"cdc.offset": "end", CDC_INITIAL_LOAD_STATE_KEY: False},
        )
    ]
    assert ingester.commit_state_arg is end
    assert ingester.committed_offsets == [end]
    (call,) = deduplicated
    assert call["loaded"] == [[load_file]]
    assert call["changes"] == [[window_file]]
    assert call["schemas"] == {"s.t": ("version", "s.t", 0)}


def test_an_interrupted_window_read_stages_nothing(monkeypatch, tmp_path):
    ctx, ingester, deduplicated, *_ = _with_window(monkeypatch, tmp_path, window=None)
    assert ctx.staged == []
    assert deduplicated == []
    assert ingester.closed is True


class _BoundedIngester(FakeIngester):
    """Delivers `batches` one poll each, then reports the bound reached."""

    def __init__(self, batches):
        super().__init__()
        self._batches = list(batches)
        self.bound = None

    def bound_stream(self, end):
        self.bound = end

    def stream_bound_reached(self):
        return not self._batches

    def poll_changes(self, session):
        return self._batches.pop(0)


class _Identity:
    def apply(self, changes):
        return changes


def _read(monkeypatch, tmp_path, ingester, limit):
    disk = FakeDisk(manifest=[["window.parquet"]])
    buffer = FakeBuffer()
    _patch_sentinels(monkeypatch, disk, buffer)
    ctx = FakeCtx(active_values=[])
    ctx.work_dir = tmp_path
    end = object()
    manifest = _runner._read_window(
        ctx=ctx,
        ingester=ingester,
        session=object(),
        tracked_tables={"s.t": object()},
        cfg=_cfg(initial_load_max_window_changes=limit),
        large_column_policy=_Identity(),
        window_end=end,
        stop_signal=threading.Event(),
    )
    return manifest, buffer, end


def test_the_window_is_read_by_the_bounded_poll(monkeypatch, tmp_path):
    ingester = _BoundedIngester([["a", "b"], ["c"]])
    manifest, buffer, end = _read(monkeypatch, tmp_path, ingester, limit=3)
    assert manifest == [["window.parquet"]]
    assert ingester.bound is end
    assert buffer.processed == 3
    assert buffer.flushed == 1


def test_a_window_past_the_limit_fails(monkeypatch, tmp_path):
    ingester = _BoundedIngester([["a", "b"], ["c", "d"]])
    with pytest.raises(SqlCdcException, match="CDC_29"):
        _read(monkeypatch, tmp_path, ingester, limit=3)


def test_run_loop_seeds_the_mark_from_the_initial_load(monkeypatch, tmp_path):
    """Capture resumes exactly where the snapshot ended, so the load's
    offset seeds the mark last. The previous offset is seeded before it,
    so what it recorded outlives the reload."""
    disk = FakeDisk(manifest=[[]])
    buffer = FakeBuffer()
    _patch_sentinels(monkeypatch, disk, buffer)
    load_offset = object()
    monkeypatch.setattr(_runner, "_load_initially", lambda **kw: (load_offset, None))
    previous = object()
    ingester = FakeIngester(resume_value=previous)
    seeds: list = []
    ingester.seed_offset = seeds.append
    ctx = FakeCtx(active_values=[False], stream_state=None)
    ctx.work_dir = tmp_path

    _run_loop(ctx=ctx, ingester=ingester, cfg=_cfg(initial_load=True))

    assert seeds == [previous, load_offset]


def test_run_loop_skips_a_completed_initial_load(monkeypatch, tmp_path):
    disk = FakeDisk(manifest=[[]])
    buffer = FakeBuffer()
    _patch_sentinels(monkeypatch, disk, buffer)
    calls: list = []
    monkeypatch.setattr(
        _runner, "_load_initially", lambda **kw: calls.append(kw) or (object(), None)
    )
    resume = object()
    ingester = FakeIngester(resume_value=resume)
    ctx = FakeCtx(
        active_values=[False],
        first_stage=False,
        stream_state={CDC_INITIAL_LOAD_STATE_KEY: False, "cdc.offset": "o"},
    )
    ctx.work_dir = tmp_path

    _run_loop(ctx=ctx, ingester=ingester, cfg=_cfg(initial_load=True))

    assert calls == []
    assert ingester.seeded is resume


# --- CdcRunConfig defaults ----------------------------------------------


def test_cdc_run_config_uses_package_defaults():
    cfg = CdcRunConfig()
    assert cfg.buffer_max_rows == DEFAULT_BUFFER_MAX_ROWS
    assert cfg.buffer_max_bytes == DEFAULT_BUFFER_MAX_BYTES
    assert cfg.buffer_max_secs == DEFAULT_BUFFER_MAX_SECS
    assert cfg.trigger_max_rows == DEFAULT_TRIGGER_MAX_ROWS
    assert cfg.trigger_max_bytes == DEFAULT_TRIGGER_MAX_BYTES
    assert cfg.trigger_max_secs == DEFAULT_TRIGGER_MAX_SECS
    assert cfg.poll_interval_seconds == DEFAULT_POLL_INTERVAL_SECONDS
    assert cfg.max_large_text_length == DEFAULT_MAX_LARGE_TEXT_LENGTH
    assert cfg.initial_load is False
    assert cfg.fetch_size == DEFAULT_FETCH_SIZE
