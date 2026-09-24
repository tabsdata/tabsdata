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
from tabsdatak.conn.common.sql.cdc._runner import (
    CdcRunConfig,
    _publish,
    _run_loop,
    _setup_signal_handlers,
)
from tabsdatak.conn.common.sql.cdc.constants import (
    DEFAULT_BUFFER_MAX_BYTES,
    DEFAULT_BUFFER_MAX_ROWS,
    DEFAULT_BUFFER_MAX_SECS,
    DEFAULT_MAX_LARGE_TEXT_LENGTH,
    DEFAULT_POLL_INTERVAL_SECONDS,
    DEFAULT_TRIGGER_MAX_BYTES,
    DEFAULT_TRIGGER_MAX_ROWS,
    DEFAULT_TRIGGER_MAX_SECS,
)


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

    @property
    def active(self) -> bool:
        try:
            return next(self._active)
        except StopIteration:
            return False

    def stage(self, tables_files, new_state) -> None:
        self.staged.append((tables_files, new_state))


class FakeDisk:
    def __init__(self, *, manifest, should_flush_force=False, should_flush_normal=False):
        self.manifest = manifest
        self.has_data = bool(manifest and any(manifest))
        self._force = should_flush_force
        self._normal = should_flush_normal
        self.cleared = 0

    def reset_timer(self) -> None:
        pass

    def should_flush(self, force: bool = False) -> bool:
        return self._force if force else self._normal

    def build_manifest(self):
        return self.manifest

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


class FakeIngester:
    def __init__(self, *, resume_value=None, poll_raises=False, open_raises=False):
        self._resume_value = resume_value
        self._poll_raises = poll_raises
        self._open_raises = open_raises
        self.resume_args = None
        self.seeded = "UNSET"
        self.session = object()
        self.closed = False
        self.commit_state_arg = None
        self.commit_state_marker = object()

    def resume_offset(self, *, first_stage, stream_state):
        self.resume_args = (first_stage, stream_state)
        return self._resume_value

    def seed_offset(self, offset) -> None:
        self.seeded = offset

    def open_session(self):
        if self._open_raises:
            raise RuntimeError("boom-open")
        return self.session

    def harvest_tables(self, *, session, cdc_format):
        return {"s.t": object()}

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


def _patch_sentinels(monkeypatch, disk, buffer):
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

    assert _publish(ctx, disk, buffer, ingester) is False
    assert ctx.staged == []
    assert disk.cleared == 0


def test_publish_stages_manifest_and_commits_offset():
    ctx = FakeCtx(active_values=[])
    files = [[Path("t0.parquet")], []]
    disk = FakeDisk(manifest=files)
    buffer = FakeBuffer()
    ingester = FakeIngester()

    assert _publish(ctx, disk, buffer, ingester) is True
    assert ctx.staged == [(files, ingester.commit_state_marker)]
    assert ingester.commit_state_arg is buffer.offset
    assert disk.cleared == 1


# --- _run_loop: seeding -------------------------------------------------


def test_run_loop_seeds_resume_offset_from_ctx(monkeypatch):
    disk = FakeDisk(manifest=[[]], should_flush_force=False)
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
    disk = FakeDisk(manifest=[[Path("f")]], should_flush_normal=True)
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
        should_flush_normal=False,
        should_flush_force=False,
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
    disk = FakeDisk(manifest=[[]], should_flush_normal=False)
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
    disk = FakeDisk(manifest=[[Path("f")]], should_flush_force=True)
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

    disk = FakeDisk(manifest=[[Path("f")]], should_flush_force=True)
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
    disk = FakeDisk(manifest=[[Path("f")]], should_flush_force=True)
    buffer = FakeBuffer()
    _patch_sentinels(monkeypatch, disk, buffer)
    ingester = FakeIngester(poll_raises=True)
    ctx = FakeCtx(active_values=[True])

    with pytest.raises(RuntimeError, match="boom-poll"):
        _run_loop(ctx=ctx, ingester=ingester, cfg=_cfg())

    assert buffer.flushed == 0
    assert ctx.staged == []
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
