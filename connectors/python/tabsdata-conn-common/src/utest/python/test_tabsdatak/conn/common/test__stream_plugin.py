#
# Copyright 2026 Tabsdata Inc.
#

from __future__ import annotations

import threading
from pathlib import Path
from typing import Self

import pytest

# noinspection PyProtectedMember
from tabsdatak.conn.common._stream import _plugin

# noinspection PyProtectedMember
from tabsdatak.conn.common._stream._config import StreamRunConfig

# noinspection PyProtectedMember
from tabsdatak.conn.common._stream._encoder import StreamEncoder

# noinspection PyProtectedMember
from tabsdatak.conn.common._stream._offset import StreamOffset

# noinspection PyProtectedMember
from tabsdatak.conn.common._stream._plugin import (
    PipelineStreamSrcPlugin,
    StreamRun,
    manifest_secs,
)

# noinspection PyProtectedMember
from tabsdatak.conn.common._stream._sentinels import BufferSentinel

# noinspection PyProtectedMember
from tabsdatak.conn.common._stream._signal import StopSignal
from tabsdatak.conn.common.error import ConnErrorCode, ConnStreamException


class FakeCtx:
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


class FakeOffset:
    def __init__(self, mark: str) -> None:
        self.mark = mark

    def write(self) -> str:
        return self.mark


class FakeBuffer:
    def __init__(self, *, due=False, moves_to=None):
        self.offset = FakeOffset("base")
        self.flushed = 0
        self.processed = 0
        self._due = due
        self._moves_to = moves_to

    def reset_timer(self) -> None:
        pass

    def add_all(self, records) -> int:
        self.processed += 1
        return len(list(records))

    def due(self) -> bool:
        return self._due

    def flush(self) -> None:
        self.flushed += 1
        if self._moves_to is not None:
            self.offset = FakeOffset(self._moves_to)


class FakeIngester:
    def __init__(self, *, resume_value=None, poll_raises=False):
        self._resume_value = resume_value
        self._poll_raises = poll_raises
        self.resume_args = None
        self.seeded = "UNSET"
        self.session = object()
        self.closed = False
        self.stage_state_arg = None
        self.stage_state_marker = object()
        self.acknowledged_offsets: list = []
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
        return self.session

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


class OwnedOffsetIngester(FakeIngester):
    def stage_state(self, offset):
        return {"offset": offset}


class Plugin(PipelineStreamSrcPlugin):
    def build_run_config(self, stream_src):
        return StreamRunConfig()

    def build_ingester(self, conn, stream_src):
        return FakeIngester()

    def slots(self, run):
        return ["s.t"]

    def build_encoder(self, run):
        return None


def cfg() -> StreamRunConfig:
    return StreamRunConfig(poll_idle_secs=0)


def patch_sentinels(monkeypatch, disk, buffer):
    monkeypatch.setattr(_plugin, "FileLedger", lambda **kw: disk)
    monkeypatch.setattr(_plugin, "DiskSentinel", lambda **kw: disk)
    monkeypatch.setattr(_plugin, "BufferSentinel", lambda **kw: buffer)


def preset_stop_signal(monkeypatch) -> None:
    preset = StopSignal()
    preset.set()
    monkeypatch.setattr(_plugin.StopSignal, "install", lambda: preset)


def stream(ctx, ingester, *, plugin=None) -> None:
    (plugin or Plugin()).stream(ctx=ctx, ingester=ingester, cfg=cfg())


def run_of(ctx, ingester) -> StreamRun:
    return Plugin().build_run(
        ctx=ctx, ingester=ingester, cfg=cfg(), stop_signal=StopSignal()
    )


def publish(ctx, disk, buffer, ingester) -> bool:
    return Plugin().publish(run_of(ctx, ingester), disk, disk, buffer)


def test_publish_empty_manifest_does_not_stage():
    ctx = FakeCtx(active_values=[])
    disk = FakeDisk(manifest=[[], []])

    assert publish(ctx, disk, FakeBuffer(), FakeIngester()) is False
    assert ctx.staged == []
    assert disk.cleared == 0


def test_publish_stages_manifest_and_commits_offset():
    ctx = FakeCtx(active_values=[])
    files = [[Path("t0.parquet")], []]
    disk = FakeDisk(manifest=files)
    buffer = FakeBuffer()
    ingester = FakeIngester()

    assert publish(ctx, disk, buffer, ingester) is True
    assert ctx.staged == [(files, ingester.stage_state_marker)]
    assert ingester.stage_state_arg is buffer.offset
    assert ingester.acknowledged_offsets == [buffer.offset]
    assert disk.cleared == 1


def test_publish_does_not_acknowledge_the_offset_when_stage_fails():
    ctx = FakeCtx(active_values=[])
    ctx.stage_raises = True
    disk = FakeDisk(manifest=[[Path("t0.parquet")]])
    ingester = FakeIngester()

    with pytest.raises(RuntimeError, match="boom-stage"):
        publish(ctx, disk, FakeBuffer(), ingester)

    assert ingester.acknowledged_offsets == []
    assert disk.cleared == 0


class Positions(StreamOffset):
    def __init__(self, applied=()):
        self.applied = list(applied)

    def update(self, other: Self) -> None:
        self.applied.extend(other.applied)

    def write(self) -> str:
        return ",".join(self.applied)

    @classmethod
    def read(cls, data: str) -> Self:
        return cls(data.split(","))


class SilentEncoder(StreamEncoder[str, Positions]):
    def slot(self, record: str) -> str:
        return "s.t"

    def size(self, record: str) -> int:
        return 1

    def offset(self, record: str) -> Positions:
        return Positions([record])

    def encode(self, slot: str, records: list[str]):
        return []


def test_a_stage_does_not_cover_records_still_in_memory():
    disk = FakeDisk(manifest=[[Path("t0.parquet")]])
    buffer = BufferSentinel(
        ledger=disk,
        encoder=SilentEncoder(),
        slots=["s.t"],
        max_rows=1_000,
        max_bytes=None,
        max_secs=10_000,
        offset=Positions(),
    )
    buffer.add("written")
    buffer.flush()
    buffer.add("in-memory")
    ingester = FakeIngester()

    assert publish(FakeCtx(active_values=[]), disk, buffer, ingester) is True
    assert ingester.stage_state_arg.applied == ["written"]
    assert ingester.acknowledged_offsets[0].applied == ["written"]


def test_stream_seeds_resume_offset_from_ctx(monkeypatch):
    patch_sentinels(monkeypatch, FakeDisk(manifest=[[]]), FakeBuffer())
    resumed = object()
    ingester = FakeIngester(resume_value=resumed)
    ctx = FakeCtx(active_values=[False], first_stage=False, stream_state={"x": 1})

    stream(ctx, ingester)

    assert ingester.resume_args == (False, {"x": 1})
    assert ingester.seeded is resumed
    assert ingester.closed is True


def test_stream_threshold_flush_stages_once(monkeypatch):
    disk = FakeDisk(manifest=[[Path("f")]], due_normal=True)
    patch_sentinels(monkeypatch, disk, FakeBuffer())
    ctx = FakeCtx(active_values=[True, False])

    stream(ctx, FakeIngester())

    assert len(ctx.staged) == 1


class WaitingSignal(StopSignal):
    def __init__(self) -> None:
        super().__init__()
        self.waits: list[float] = []

    def wait(self, timeout: float) -> bool:
        self.waits.append(timeout)
        return False


def test_the_loop_rests_only_after_a_poll_that_found_nothing(monkeypatch):
    class Trickle(FakeIngester):
        def __init__(self):
            super().__init__()
            self.batches = [["a", "b"], [], ["c"], []]

        def poll_changes(self, session):
            return self.batches.pop(0)

    disk = FakeDisk(manifest=[[]])
    patch_sentinels(monkeypatch, disk, FakeBuffer())
    signal = WaitingSignal()
    monkeypatch.setattr(_plugin.StopSignal, "install", lambda: signal)
    ctx = FakeCtx(active_values=[True, True, True, True, False])

    Plugin().stream(ctx=ctx, ingester=Trickle(), cfg=StreamRunConfig(poll_idle_secs=7))

    assert signal.waits == [7, 7]


def disk_arguments(monkeypatch, disk) -> dict:
    arguments: dict = {}

    def build(**kwargs):
        arguments.update(kwargs)
        return disk

    monkeypatch.setattr(_plugin, "DiskSentinel", build)
    return arguments


@pytest.mark.parametrize(
    ("trigger_max_secs", "flush_interval_mins", "expected"),
    [(60, 0, 60), (600, 0, 600), (600, 2, 120), (60, 10, 60), (5, 1, 5)],
)
def test_the_manifest_time_threshold_is_capped_by_the_flush_interval(
    trigger_max_secs, flush_interval_mins, expected
):
    assert manifest_secs(trigger_max_secs, flush_interval_mins * 60) == expected


def test_stream_hands_the_disk_sentinel_its_thresholds(monkeypatch):
    disk = FakeDisk(manifest=[[]])
    buffer = FakeBuffer()
    patch_sentinels(monkeypatch, disk, buffer)
    arguments = disk_arguments(monkeypatch, disk)
    ctx = FakeCtx(active_values=[False], flush_interval_mins=2)

    Plugin().stream(
        ctx=ctx,
        ingester=FakeIngester(),
        cfg=StreamRunConfig(
            trigger_max_rows=7,
            trigger_max_bytes=11,
            trigger_max_secs=600,
            poll_idle_secs=0,
        ),
    )

    assert arguments["buffer"] is buffer
    assert (
        arguments["max_rows"],
        arguments["max_bytes"],
        arguments["max_secs"],
    ) == (7, 11, 120)


def test_a_due_manifest_writes_the_buffer_out_before_it_stages(monkeypatch):
    class OrderedDisk(FakeDisk):
        def manifest(self):
            order.append("manifest")
            return super().manifest()

    class OrderedBuffer(FakeBuffer):
        def flush(self) -> None:
            order.append("flush")
            super().flush()

    order: list[str] = []
    disk = OrderedDisk(manifest=[[Path("f")]], due_normal=True)
    patch_sentinels(monkeypatch, disk, OrderedBuffer())
    ctx = FakeCtx(active_values=[True, False])

    stream(ctx, FakeIngester())

    assert order[:2] == ["flush", "manifest"]
    assert len(ctx.staged) >= 1


def test_stream_graceful_end_flushes_trailing_batch(monkeypatch):
    disk = FakeDisk(manifest=[[Path("f")]], due_force=True)
    buffer = FakeBuffer()
    patch_sentinels(monkeypatch, disk, buffer)
    ingester = FakeIngester()
    ctx = FakeCtx(active_values=[False])

    stream(ctx, ingester)

    assert buffer.flushed == 1
    assert len(ctx.staged) == 1
    assert ingester.closed is True


def test_stream_interrupted_skips_trailing_flush(monkeypatch):
    preset_stop_signal(monkeypatch)
    disk = FakeDisk(manifest=[[Path("f")]], due_force=True)
    buffer = FakeBuffer()
    patch_sentinels(monkeypatch, disk, buffer)
    ingester = FakeIngester()
    ctx = FakeCtx(active_values=[True, True])

    stream(ctx, ingester)

    assert buffer.flushed == 0
    assert ctx.staged == []
    assert ingester.closed is True


def test_stream_errored_skips_trailing_flush_but_closes(monkeypatch):
    disk = FakeDisk(manifest=[[Path("f")]], due_force=True)
    buffer = FakeBuffer()
    patch_sentinels(monkeypatch, disk, buffer)
    ingester = FakeIngester(poll_raises=True)
    ctx = FakeCtx(active_values=[True])

    with pytest.raises(RuntimeError, match="boom-poll"):
        stream(ctx, ingester)

    assert buffer.flushed == 0
    assert ctx.staged == []
    assert ingester.closed is True


def test_a_stream_stopped_before_it_starts_stages_nothing_but_closes(monkeypatch):
    class Refusing(Plugin):
        def before_stream(self, run):
            return False

    disk = FakeDisk(manifest=[[Path("f")]], due_force=True)
    buffer = FakeBuffer()
    patch_sentinels(monkeypatch, disk, buffer)
    ingester = FakeIngester()
    ctx = FakeCtx(active_values=[True])

    stream(ctx, ingester, plugin=Refusing())

    assert (buffer.flushed, ctx.staged, ingester.settled) == (0, [], 1)
    assert ingester.closed is True


def test_stream_hands_the_ingester_a_checkpoint_that_records_state(monkeypatch):
    patch_sentinels(monkeypatch, FakeDisk(manifest=[]), FakeBuffer())
    ingester = OwnedOffsetIngester()
    ctx = FakeCtx(
        active_values=[False],
        first_stage=False,
        stream_state={"offset": "staged"},
    )

    stream(ctx, ingester)
    ingester.lifecycle.checkpoint(FakeOffset("moved"))

    assert [state["offset"].mark for state in ctx.checkpoints] == ["moved"]
    assert ctx.staged == []


def test_a_flush_that_moves_the_offset_without_data_checkpoints_it(monkeypatch):
    ctx = FakeCtx(
        active_values=[True, False],
        first_stage=False,
        stream_state={"offset": "staged"},
    )
    disk = FakeDisk(manifest=[[]])
    buffer = FakeBuffer(due=True, moves_to="moved")
    patch_sentinels(monkeypatch, disk, buffer)

    stream(ctx, OwnedOffsetIngester())

    assert [state["offset"].mark for state in ctx.checkpoints] == ["moved"]
    assert ctx.staged == []


def test_a_flush_that_moves_nothing_checkpoints_nothing(monkeypatch):
    ctx = FakeCtx(
        active_values=[True, False],
        first_stage=False,
        stream_state={"offset": "staged"},
    )
    disk = FakeDisk(manifest=[[]])
    patch_sentinels(monkeypatch, disk, FakeBuffer(due=True))

    stream(ctx, OwnedOffsetIngester())

    assert ctx.checkpoints == []


def test_no_checkpoint_while_the_ledger_holds_unstaged_data(monkeypatch):
    ctx = FakeCtx(
        active_values=[True, False],
        first_stage=False,
        stream_state={"offset": "staged"},
    )
    disk = FakeDisk(manifest=[[Path("t0.parquet")]])
    patch_sentinels(monkeypatch, disk, FakeBuffer(due=True, moves_to="moved"))

    stream(ctx, OwnedOffsetIngester())

    assert ctx.checkpoints == []
    assert ctx.staged == []


def test_a_source_kept_checkpoint_invokes_the_state_callable():
    ctx = FakeCtx(active_values=[], first_stage=False, stream_state={})
    ingester = FakeIngester()
    acknowledged = []
    ingester.stage_state_marker = lambda: acknowledged.append("done")
    run = run_of(ctx, ingester)

    Plugin().checkpoint(run, FakeOffset("moved"))

    assert acknowledged == ["done"]
    assert ctx.checkpoints == []
    assert run.recorded == "moved"


def test_there_is_no_checkpoint_before_a_first_stage():
    ctx = FakeCtx(active_values=[], first_stage=True)
    ingester = OwnedOffsetIngester()
    Plugin().checkpoint(run_of(ctx, ingester), "moved")
    assert ctx.checkpoints == []


def test_a_checkpoint_without_an_offset_is_a_bug():
    ctx = FakeCtx(active_values=[], first_stage=False, stream_state={"x": 1})
    with pytest.raises(ConnStreamException) as raised:
        Plugin().checkpoint(run_of(ctx, OwnedOffsetIngester()), None)
    assert raised.value.api_error is ConnErrorCode.CONN_34
    assert ctx.checkpoints == []


def test_a_clean_end_settles_the_position_after_the_last_stage(monkeypatch):
    patch_sentinels(
        monkeypatch, FakeDisk(manifest=[[Path("f")]], due_force=True), FakeBuffer()
    )
    ingester = FakeIngester()
    ctx = FakeCtx(active_values=[False])

    stream(ctx, ingester)

    assert (len(ctx.staged), ingester.settled, ingester.closed) == (1, 1, True)


def test_a_signalled_stop_settles_the_position(monkeypatch):
    preset_stop_signal(monkeypatch)
    patch_sentinels(monkeypatch, FakeDisk(manifest=[]), FakeBuffer())
    ingester = FakeIngester()

    stream(FakeCtx(active_values=[True]), ingester)

    assert ingester.settled == 1


def test_an_errored_run_does_not_settle(monkeypatch):
    patch_sentinels(monkeypatch, FakeDisk(manifest=[]), FakeBuffer())
    ingester = FakeIngester(poll_raises=True)

    with pytest.raises(RuntimeError, match="boom-poll"):
        stream(FakeCtx(active_values=[True]), ingester)

    assert (ingester.settled, ingester.closed) == (0, True)


def test_a_failed_settle_still_closes_the_session(monkeypatch):
    patch_sentinels(monkeypatch, FakeDisk(manifest=[]), FakeBuffer())
    ingester = FakeIngester()
    ingester.settle = lambda: (_ for _ in ()).throw(RuntimeError("boom-settle"))

    stream(FakeCtx(active_values=[False]), ingester)

    assert ingester.closed is True


def test_the_stop_signal_off_the_main_thread_is_a_plain_unset_signal():
    result = {}

    def worker():
        stop_signal = StopSignal.install()
        result["stop_signal"] = stop_signal
        result["is_set"] = stop_signal.is_set()

    thread = threading.Thread(target=worker)
    thread.start()
    thread.join()

    assert isinstance(result["stop_signal"], StopSignal)
    assert result["is_set"] is False


def test_run_sets_the_configured_level_on_its_loggers(monkeypatch):
    import logging

    class Configured(Plugin):
        logging_namespace = "tabsdatak.conn.common.test_stream_namespace"

        def build_run_config(self, stream_src):
            return StreamRunConfig(logging_level=logging.DEBUG)

    monkeypatch.setattr(Configured, "stream", lambda self, **kw: None)
    names = [
        Configured.logging_namespace,
        _plugin.STREAM_NAMESPACE,
        Configured.__module__,
    ]
    previous = {name: logging.getLogger(name).level for name in names}
    try:
        Configured().run(ctx=None, conn=None, stream_src=None)
        assert {name: logging.getLogger(name).level for name in names} == {
            name: logging.DEBUG for name in names
        }
    finally:
        for name, level in previous.items():
            logging.getLogger(name).setLevel(level)
