#
#  Copyright 2026 Tabsdata Inc.
#

"""Unit tests for the CDC hooks of the shared stream run loop
(:mod:`tabsdatak.conn.common.sql.cdc._runner`): the initial load and its
window, the initial-load flag kept in the stream state and in checkpoints,
and the ``CdcRunConfig`` defaults. The sentinels of the run loop are faked
out so the hooks are tested in isolation, without a database; what a hook
stages lands on a local stand-in for the function stage."""

from __future__ import annotations

from pathlib import Path

import polars as pl
import pyarrow as pa
import pytest
from tabsdatax.itest.fixture.functions.stream import LocalStage

# noinspection PyProtectedMember
from tabsdatak.api import BatchThresholds, PollThresholds, StreamThresholds
from tabsdatak.conn.common._stream import _plugin

# noinspection PyProtectedMember
from tabsdatak.conn.common._stream._signal import StopSignal

# noinspection PyProtectedMember
from tabsdatak.conn.common._stream.constants import (
    DEFAULT_BUFFER_MAX_BYTES,
    DEFAULT_BUFFER_MAX_ROWS,
    DEFAULT_BUFFER_MAX_SECS,
    DEFAULT_POLL_IDLE_SECS,
)
from tabsdatak.conn.common.sql.cdc import _runner
from tabsdatak.conn.common.sql.cdc._deduplicator import Deduplicated
from tabsdatak.conn.common.sql.cdc._ingester import CDC_INITIAL_LOAD_STATE_KEY
from tabsdatak.conn.common.sql.cdc._runner import (
    CdcPlugin,
    CdcRunConfig,
    _initial_load_pending,
)
from tabsdatak.conn.common.sql.cdc.constants import (
    DEFAULT_FETCH_SIZE,
    DEFAULT_MAX_LARGE_TEXT_LENGTH,
)
from tabsdatak.conn.common.sql.cdc.error import SqlCdcException
from tabsdatak.spi import LazyFrameInput

# --- fakes --------------------------------------------------------------


class FakeCtx:
    """Duck-typed :class:`StreamSrcPluginCtx`. ``active`` is driven by a
    scripted sequence so the loop runs a controlled number of times; what is
    staged lands on a :class:`LocalStage` under ``work_dir``, and ``staged``
    lists each published batch with its state."""

    def __init__(
        self,
        *,
        active_values,
        thresholds=None,
        first_stage=True,
        stream_state=None,
    ):
        self._active = iter(active_values)
        self.thresholds = thresholds or StreamThresholds(
            batch=BatchThresholds(max_rows=1 << 62)
        )
        self.first_stage = first_stage
        self.stream_state = stream_state
        self.work_dir = Path("/tmp")
        self.checkpoints: list = []
        self.stage_raises = False
        self._local: LocalStage | None = None

    @property
    def active(self) -> bool:
        try:
            return next(self._active)
        except StopIteration:
            return False

    @property
    def local(self) -> LocalStage:
        if self._local is None:
            self._local = LocalStage(self.work_dir / "stage")
        return self._local

    @property
    def staged(self) -> list[tuple]:
        return [(batch.tables, batch.state) for batch in self.local.published]

    @property
    def pending(self):
        return self.local.pending

    def stage(self, tables_files=None, new_state=None):
        if self.stage_raises:
            raise RuntimeError("boom-stage")
        return self.local.stage(tables_files, new_state)

    def scan(self, file) -> pl.LazyFrame:
        return self.local.scan(file)

    def unstage(self, files=None) -> None:
        self.local.unstage(files)

    def checkpoint(self, new_state) -> None:
        self.checkpoints.append(new_state)


def ids(files: list[list[Path]]) -> list[list[list[int]]]:
    return [[pl.read_parquet(file)["id"].to_list() for file in slot] for slot in files]


def collected(frames: list[list[pl.LazyFrame]]) -> list[list[list[int]]]:
    return [[frame.collect()["id"].to_list() for frame in slot] for slot in frames]


def sent(ctx: FakeCtx, *values: list[int]):
    return ctx.stage([[LazyFrameInput(pl.LazyFrame({"id": row})) for row in values]])


class FakeDisk:
    """Stands in for both the stream's ledger and its disk sentinel."""

    def __init__(self, *, manifest, due_force=False, due_normal=False):
        self._manifest = manifest
        self.has_data = bool(manifest and any(manifest))
        self.has_files = self.has_data
        self._force = due_force
        self._normal = due_normal
        self.cleared = 0
        self.ctx: FakeCtx | None = None

    def publish(self, new_state) -> None:
        self.ctx.stage(None, new_state)
        self.clear()

    def discard(self) -> None:
        self.clear()

    def reset_timer(self) -> None:
        pass

    def due(self, force: bool = False) -> bool:
        return self._force if force else self._normal

    def manifest(self):
        return self._manifest

    def clear(self) -> None:
        self.cleared += 1


class _Offset:
    def __init__(self, mark: str = "o") -> None:
        self.mark = mark

    def write(self) -> str:
        return self.mark

    @classmethod
    def read(cls, data: str) -> "_Offset":
        return cls(data)


class FakeBuffer:
    def __init__(self):
        self.offset = _Offset()
        self.flushed = 0
        self.processed = 0

    def reset_timer(self) -> None:
        pass

    def add_all(self, changes) -> None:
        self.processed += 1

    def add(self, change) -> None:
        self.processed += 1

    def due(self) -> bool:
        return False

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
        self.stage_state_arg = None
        self.stage_state_marker = object()
        self.acknowledged_offsets: list = []
        self.tracked_tables: dict = {"s.t": object()}
        self.lifecycle = None
        self.settled = 0

    def resume_offset(self, *, first_stage, stream_state):
        self.resume_args = (first_stage, stream_state)
        return self._resume_value

    def seed_offset(self, offset) -> None:
        self.seeded = offset

    def attach_lifecycle(self, lifecycle) -> None:
        self.lifecycle = lifecycle

    def settle_offset(self) -> None:
        self.settled += 1

    def open_session(self, work_dir):
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

    def stage_state(self, offset):
        self.stage_state_arg = offset
        return self.stage_state_marker

    def acknowledge_offset(self, offset) -> None:
        self.acknowledged_offsets.append(offset)


def bound(disk: FakeDisk, ctx: FakeCtx) -> FakeDisk:
    disk.ctx = ctx
    return disk


def _patch_sentinels(monkeypatch, disk, buffer):
    monkeypatch.setattr(_plugin, "StageLedger", lambda **kw: bound(disk, kw["ctx"]))
    for module in (_plugin, _runner):
        monkeypatch.setattr(module, "BufferSentinel", lambda **kw: buffer)
    monkeypatch.setattr(_plugin, "DiskSentinel", lambda **kw: disk)


def _cfg(**overrides) -> CdcRunConfig:
    # An idle wait of 0 so the loop's stop_signal.wait() returns
    # immediately and the test never blocks.
    base = dict(poll=PollThresholds(max_secs=1, idle_secs=0))
    base.update(overrides)
    return CdcRunConfig(**base)


class _Plugin(CdcPlugin):
    def build_run_config(self, stream_src) -> CdcRunConfig:
        return _cfg()

    def build_ingester(self, conn, stream_src):
        return FakeIngester()


def _stream(ctx, ingester, cfg: CdcRunConfig) -> None:
    _Plugin().stream(ctx=ctx, ingester=ingester, cfg=cfg)


def _checkpoint(ctx, ingester, cfg: CdcRunConfig, offset) -> None:
    plugin = _Plugin()
    run = plugin.build_run(
        ctx=ctx, ingester=ingester, cfg=cfg, stop_signal=StopSignal()
    )
    plugin.checkpoint(run, offset)


# --- checkpoints ----------------------------------------------------------


class _OwnedOffsetIngester(FakeIngester):
    def stage_state(self, offset):
        return {"cdc.offset": offset}


def test_a_checkpoint_keeps_the_record_of_a_done_initial_load():
    ctx = FakeCtx(
        active_values=[],
        first_stage=False,
        stream_state={"cdc.offset": "staged", CDC_INITIAL_LOAD_STATE_KEY: False},
    )
    moved = _Offset("moved")
    _checkpoint(ctx, _OwnedOffsetIngester(), _cfg(initial_load=True), moved)
    assert ctx.checkpoints == [{"cdc.offset": moved, CDC_INITIAL_LOAD_STATE_KEY: False}]


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
    _checkpoint(
        ctx, _OwnedOffsetIngester(), _cfg(initial_load=initial_load), _Offset("moved")
    )
    assert ctx.checkpoints == []


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
    """Run ``_load_initially`` with the loader faked out to read rows 1 and 2,
    so only the runner's own orchestration is under test."""
    offset = _Offset()

    class FakeLoader:
        def __init__(self, **kw):
            self.kw = kw

        def load(self, session):
            if loader_raises:
                raise RuntimeError("boom-load")
            chunks = self.kw["chunk_sentinel"]
            chunks.add("s.t", "k", pa.table({"id": [1, 2]}))
            chunks.flush()
            return offset

    monkeypatch.setattr(_runner, "InitialLoader", FakeLoader)
    ingester = FakeIngester(window_end=window_end)
    ingester.stage_state_marker = {"cdc.offset": "o"}
    result = _runner._load_initially(
        ctx=ctx,
        ingester=ingester,
        cfg=_cfg(initial_load=True),
        large_column_policy=object(),
    )
    return result, offset, ingester


def test_initial_load_stages_once_with_offset_and_flag(monkeypatch, tmp_path):
    ctx = FakeCtx(active_values=[])
    ctx.work_dir = tmp_path
    result, offset, ingester = _loaded(monkeypatch, tmp_path, ctx=ctx)

    assert result == (offset, None)
    ((tables, state),) = ctx.staged
    assert ids(tables) == [[[1, 2]]]
    # The resume offset and the "load done" flag are persisted together,
    # so a crash cannot leave one without the other.
    assert state == {"cdc.offset": "o", CDC_INITIAL_LOAD_STATE_KEY: False}
    assert ingester.acknowledged_offsets == [offset]


def test_initial_load_failure_stages_nothing(monkeypatch, tmp_path):
    ctx = FakeCtx(active_values=[])
    ctx.work_dir = tmp_path
    with pytest.raises(RuntimeError, match="boom-load"):
        _loaded(monkeypatch, tmp_path, ctx=ctx, loader_raises=True)
    assert ctx.staged == []


def test_initial_load_closes_the_snapshot_session(monkeypatch, tmp_path):
    ctx = FakeCtx(active_values=[])
    ctx.work_dir = tmp_path
    _, _, ingester = _loaded(monkeypatch, tmp_path, ctx=ctx)
    assert ingester.initial_load_session.closed is True


def test_initial_load_requires_a_tabsdata_owned_offset(monkeypatch, tmp_path):
    """An ingester whose offset the source owns hands back a callable
    instead of a state dict; there is nowhere to record the load, so the
    runner refuses rather than repeating the snapshot on every restart."""
    ctx = FakeCtx(active_values=[])
    ctx.work_dir = tmp_path
    monkeypatch.setattr(
        _runner,
        "InitialLoader",
        lambda **kw: type("L", (), {"load": lambda self, session: object()})(),
    )
    ingester = FakeIngester()
    ingester.stage_state_marker = lambda: None

    with pytest.raises(SqlCdcException):
        _runner._load_initially(
            ctx=ctx,
            ingester=ingester,
            cfg=_cfg(initial_load=True),
            large_column_policy=object(),
        )
    assert ctx.staged == []


def test_a_load_with_a_window_is_sent_but_not_published(monkeypatch, tmp_path):
    """The load is published with the window's changes, once they are read
    and deduplicated, so the load alone publishes nothing."""
    ctx = FakeCtx(active_values=[])
    ctx.work_dir = tmp_path
    end = object()
    result, offset, ingester = _loaded(monkeypatch, tmp_path, ctx=ctx, window_end=end)
    start, pending = result
    assert start is offset
    assert pending.manifest == ctx.pending
    assert [len(files) for files in pending.manifest] == [1]
    assert pending.window_end is end
    assert ctx.staged == []
    assert ingester.acknowledged_offsets == []


class _FakeRegistry:
    def version(self, table, ordinal):
        return ("version", table, ordinal)


def _with_window(monkeypatch, tmp_path, *, window, reduced=None):
    """Run the loop over a load of row 1 sent and pending, with the window
    read (its rows sent as one table) and the deduplication faked out."""
    disk = FakeDisk(manifest=[[]])
    buffer = FakeBuffer()
    _patch_sentinels(monkeypatch, disk, buffer)
    ctx = FakeCtx(active_values=[False], stream_state=None)
    ctx.work_dir = tmp_path
    end = object()
    pending = _runner._PendingLoad(manifest=sent(ctx, [1]), window_end=end)
    window_sent = None if window is None else sent(ctx, window)
    monkeypatch.setattr(_runner, "_load_initially", lambda **kw: (object(), pending))
    monkeypatch.setattr(_runner, "_read_window", lambda **kw: window_sent)
    deduplicated: list = []

    def deduplicate(**kw):
        # Read now: the window's tables are unstaged once deduplicated.
        deduplicated.append(
            kw
            | {"loaded": collected(kw["loaded"]), "changes": collected(kw["changes"])}
        )
        return reduced or {}

    monkeypatch.setattr(_runner, "deduplicate_initial_load_frames", deduplicate)
    ingester = FakeIngester()
    ingester.schema_registry = _FakeRegistry()
    ingester.stage_state_marker = {"cdc.offset": "end"}
    reached = object()
    ingester.base_offset = lambda: reached
    _stream(ctx, ingester, _cfg(initial_load=True))
    return ctx, ingester, deduplicated, reached


def test_the_load_is_staged_with_what_deduplication_keeps(monkeypatch, tmp_path):
    kept = Deduplicated(pl.LazyFrame({"id": [2]}), dropped=1)
    ctx, ingester, deduplicated, reached = _with_window(
        monkeypatch, tmp_path, window=[2, 1], reduced={"s.t": kept}
    )
    ((tables, state),) = ctx.staged
    assert ids(tables) == [[[1], [2]]]
    assert state == {"cdc.offset": "end", CDC_INITIAL_LOAD_STATE_KEY: False}
    # Where the window's read left the stream, which a window ending at a
    # binlog position, no resumable offset, needs.
    assert ingester.stage_state_arg is reached
    assert ingester.acknowledged_offsets == [reached]
    (call,) = deduplicated
    assert call["loaded"] == [[[1]]]
    assert call["changes"] == [[[2, 1]]]
    assert call["schemas"] == {"s.t": ("version", "s.t", 0)}
    assert len(list((tmp_path / "stage").glob("*.parquet"))) == 2


def test_a_window_deduplication_drops_nothing_from_is_published_as_sent(
    monkeypatch, tmp_path
):
    ctx, *_ = _with_window(monkeypatch, tmp_path, window=[2, 3])
    ((tables, _),) = ctx.staged
    assert ids(tables) == [[[1], [2, 3]]]


def test_a_window_whose_changes_all_drop_publishes_the_load_alone(
    monkeypatch, tmp_path
):
    dropped = Deduplicated(pl.LazyFrame({"id": []}, schema={"id": pl.Int64}), 2)
    ctx, *_ = _with_window(
        monkeypatch, tmp_path, window=[1, 1], reduced={"s.t": dropped}
    )
    ((tables, _),) = ctx.staged
    assert ids(tables) == [[[1]]]


def test_an_interrupted_window_read_stages_nothing(monkeypatch, tmp_path):
    ctx, ingester, deduplicated, _ = _with_window(monkeypatch, tmp_path, window=None)
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
    monkeypatch.setattr(_runner, "StageLedger", lambda **kw: disk)
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
        stop_signal=StopSignal(),
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


def test_the_stream_seeds_the_mark_from_the_initial_load(monkeypatch, tmp_path):
    """Capture resumes exactly where the snapshot ended, so the load's
    offset seeds the mark last. The previous offset is seeded before it,
    so what it recorded outlives the reload."""
    disk = FakeDisk(manifest=[[]])
    buffer = FakeBuffer()
    _patch_sentinels(monkeypatch, disk, buffer)
    load_offset = _Offset()
    monkeypatch.setattr(_runner, "_load_initially", lambda **kw: (load_offset, None))
    previous = object()
    ingester = FakeIngester(resume_value=previous)
    seeds: list = []
    ingester.seed_offset = seeds.append
    ctx = FakeCtx(active_values=[False], stream_state=None)
    ctx.work_dir = tmp_path

    _stream(ctx, ingester, _cfg(initial_load=True))

    assert seeds == [previous, load_offset]


def test_the_stream_skips_a_completed_initial_load(monkeypatch, tmp_path):
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

    _stream(ctx, ingester, _cfg(initial_load=True))

    assert calls == []
    assert ingester.seeded is resume


# --- CdcRunConfig defaults ----------------------------------------------


def test_cdc_run_config_uses_package_defaults():
    cfg = CdcRunConfig()
    assert cfg.buffer.max_rows == DEFAULT_BUFFER_MAX_ROWS
    assert cfg.buffer.max_bytes == DEFAULT_BUFFER_MAX_BYTES
    assert cfg.buffer.max_secs == DEFAULT_BUFFER_MAX_SECS
    assert cfg.poll.idle_secs == DEFAULT_POLL_IDLE_SECS
    assert cfg.max_large_text_length == DEFAULT_MAX_LARGE_TEXT_LENGTH
    assert cfg.initial_load is False
    assert cfg.fetch_size == DEFAULT_FETCH_SIZE
