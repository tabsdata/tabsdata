#
# Copyright 2026 Tabsdata Inc.
#

from __future__ import annotations

import logging
import time
from abc import ABC, abstractmethod
from collections.abc import Callable, Iterable, Sequence
from dataclasses import dataclass, field
from typing import Any

from tabsdatak.api import StreamSrc, StreamThresholds
from tabsdatak.conn.common._stream._config import StreamRunConfig
from tabsdatak.conn.common._stream._encoder import StreamEncoder
from tabsdatak.conn.common._stream._ingester import StreamIngester
from tabsdatak.conn.common._stream._ledger import FileLedger
from tabsdatak.conn.common._stream._lifecycle import StreamLifecycle
from tabsdatak.conn.common._stream._offset import StreamOffset
from tabsdatak.conn.common._stream._poll import SourcePoll
from tabsdatak.conn.common._stream._restart import StreamRestart
from tabsdatak.conn.common._stream._sentinels import BufferSentinel, DiskSentinel
from tabsdatak.conn.common._stream._signal import StopSignal
from tabsdatak.conn.common._stream.constants import MAX_RESTARTS_WITHOUT_STAGE
from tabsdatak.conn.common.error import ConnErrorCode
from tabsdatak.spi import Conn, StreamSrcPlugin, StreamSrcPluginCtx, StreamStateSpec

logger = logging.getLogger(__name__)

STREAM_NAMESPACE = "tabsdatak.conn.common._stream"


@dataclass(kw_only=True)
class StreamRun[R, O: StreamOffset]:
    ctx: StreamSrcPluginCtx
    ingester: StreamIngester[R, O]
    cfg: StreamRunConfig
    thresholds: StreamThresholds
    stop_signal: StopSignal
    session: Any = None
    slots: list[str] = field(default_factory=list)
    staged: O | None = None
    restarts: int = 0


def snapshot[O: StreamOffset](offset: O) -> O:
    return type(offset).read(offset.write())


class RunLifecycle[R, O: StreamOffset](StreamLifecycle[O]):
    def __init__(self, plugin: PipelineStreamSrcPlugin, run: StreamRun[R, O]):
        self.plugin = plugin
        self.run = run
        self.ledger: FileLedger | None = None
        self.disk: DiskSentinel | None = None
        self.buffer: BufferSentinel[R, O] | None = None

    def arm(
        self,
        ledger: FileLedger,
        disk: DiskSentinel,
        buffer: BufferSentinel[R, O],
    ) -> None:
        self.ledger = ledger
        self.disk = disk
        self.buffer = buffer

    def checkpoint(self, offset: O) -> bool:
        return self.plugin.checkpoint(self.run, offset)

    def settle(self) -> None:
        if self.buffer is None:
            return
        self.buffer.flush()
        if self.disk.due(force=True):
            self.plugin.publish(self.run, self.ledger, self.disk, self.buffer)
        else:
            self.plugin.record(self.run, self.ledger, self.buffer)


class PipelineStreamSrcPlugin[C: Conn, S: StreamSrc, R, O: StreamOffset](
    StreamSrcPlugin[C, S], ABC
):
    logging_namespace: str | None = None
    clock: Callable[[], float] = staticmethod(time.time)

    @abstractmethod
    def build_run_config(self, stream_src: S) -> StreamRunConfig: ...

    @abstractmethod
    def build_ingester(self, conn: C, stream_src: S) -> StreamIngester[R, O]: ...

    @abstractmethod
    def slots(self, run: StreamRun[R, O]) -> Sequence[str]: ...

    @abstractmethod
    def build_encoder(self, run: StreamRun[R, O]) -> StreamEncoder[R, O]: ...

    def build_run(
        self,
        *,
        ctx: StreamSrcPluginCtx,
        ingester: StreamIngester[R, O],
        cfg: StreamRunConfig,
        stop_signal: StopSignal,
    ) -> StreamRun[R, O]:
        return StreamRun(
            ctx=ctx,
            ingester=ingester,
            cfg=cfg,
            thresholds=self.thresholds(ctx, cfg),
            stop_signal=stop_signal,
        )

    @staticmethod
    def thresholds(ctx: StreamSrcPluginCtx, cfg: StreamRunConfig) -> StreamThresholds:
        """The thresholds this run flows by: the batch level from the
        function's definition, the buffer and poll levels from it when it
        names them and from the connector's settings otherwise."""
        defined = ctx.thresholds
        return StreamThresholds(
            batch=defined.batch,
            buffer=defined.buffer or cfg.buffer,
            poll=defined.poll or cfg.poll,
        )

    def seed(self, run: StreamRun[R, O]) -> None:
        run.ingester.seed_offset(
            run.ingester.resume_offset(
                first_stage=run.ctx.first_stage,
                stream_state=run.ctx.stream_state,
            )
        )

    def before_stream(self, run: StreamRun[R, O]) -> bool:
        return True

    def records(self, run: StreamRun[R, O], polled: Iterable[R]) -> Iterable[R]:
        return polled

    def stage_state(self, run: StreamRun[R, O], offset: O) -> StreamStateSpec:
        return run.ingester.stage_state(offset)

    def may_checkpoint(self, run: StreamRun[R, O]) -> bool:
        return not run.ctx.first_stage

    def run(
        self,
        ctx: StreamSrcPluginCtx,
        conn: C,
        stream_src: S,
    ) -> None:
        cfg = self.build_run_config(stream_src)
        if self.logging_namespace is not None:
            logging.getLogger(self.logging_namespace).setLevel(cfg.logging_level)
        logging.getLogger(STREAM_NAMESPACE).setLevel(cfg.logging_level)
        plugin_logger = logging.getLogger(type(self).__module__)
        plugin_logger.setLevel(cfg.logging_level)

        ingester = self.build_ingester(conn, stream_src)
        plugin_logger.info("Starting the stream with %s", type(ingester).__name__)
        try:
            self.stream(ctx=ctx, ingester=ingester, cfg=cfg)
        finally:
            plugin_logger.info("Stopped the stream")

    def stream(
        self,
        *,
        ctx: StreamSrcPluginCtx,
        ingester: StreamIngester[R, O],
        cfg: StreamRunConfig,
    ) -> None:
        ledger: FileLedger | None = None
        buffer_sentinel: BufferSentinel[R, O] | None = None
        disk_sentinel: DiskSentinel | None = None
        interrupted = False
        errored = False

        stop_signal = StopSignal.install()
        run = self.build_run(
            ctx=ctx, ingester=ingester, cfg=cfg, stop_signal=stop_signal
        )
        cfg = run.cfg
        try:
            self.seed(run)
            lifecycle = RunLifecycle(self, run)
            ingester.attach_lifecycle(lifecycle)
            run.session = ingester.open_session(ctx.work_dir)
            run.slots = list(self.slots(run))
            if not self.before_stream(run):
                interrupted = True
                return
            ledger = FileLedger(landing=ctx.work_dir, slots=run.slots)
            encoder = self.build_encoder(run)
            thresholds = run.thresholds
            buffer_sentinel = BufferSentinel(
                ledger=ledger,
                encoder=encoder,
                slots=run.slots,
                max_rows=thresholds.buffer.max_rows,
                max_bytes=thresholds.buffer.max_bytes,
                max_secs=thresholds.buffer.max_secs,
                offset=ingester.base_offset(),
                measure_bytes=thresholds.batch.max_bytes is not None,
                clock=self.clock,
            )
            disk_sentinel = DiskSentinel(
                ledger=ledger,
                buffer=buffer_sentinel,
                max_rows=thresholds.batch.max_rows,
                max_bytes=thresholds.batch.max_bytes,
                max_secs=thresholds.batch.max_secs,
                min_secs=thresholds.batch.min_secs,
                idle_secs=thresholds.batch.idle_secs,
                clock=self.clock,
            )

            lifecycle.arm(ledger, disk_sentinel, buffer_sentinel)
            run.staged = snapshot(buffer_sentinel.offset)

            buffer_sentinel.reset_timer()
            disk_sentinel.reset_timer()

            while not stop_signal.is_set() and ctx.active:
                try:
                    poll = SourcePoll(
                        ingester.poll_changes(run.session),
                        count=encoder.rows,
                    )
                    polled = buffer_sentinel.add_all(self.records(run, poll))
                except StreamRestart as restart:
                    self.restart(run, restart, ledger, disk_sentinel, buffer_sentinel)
                    continue
                poll.log()
                if buffer_sentinel.due():
                    buffer_sentinel.flush()
                    self.record(run, ledger, buffer_sentinel)
                if disk_sentinel.due():
                    buffer_sentinel.flush()
                    self.publish(run, ledger, disk_sentinel, buffer_sentinel)
                if polled == 0:
                    stop_signal.wait(timeout=run.thresholds.poll.idle_secs)
            if stop_signal.is_set():
                interrupted = True
                logger.info("Interrupted the stream; exiting the run loop")
        except Exception as exception:
            errored = True
            logger.error(f"Error in the stream run loop: {exception}")
            raise
        finally:
            if not interrupted and not errored:
                if buffer_sentinel is not None:
                    buffer_sentinel.flush()
                if disk_sentinel is not None and disk_sentinel.due(force=True):
                    self.publish(run, ledger, disk_sentinel, buffer_sentinel)
                elif buffer_sentinel is not None:
                    self.record(run, ledger, buffer_sentinel)
            if run.session is not None:
                if not errored:
                    self.settle_offset(run)
                ingester.close_session(run.session)

    def publish(
        self,
        run: StreamRun[R, O],
        ledger: FileLedger,
        disk_sentinel: DiskSentinel,
        buffer_sentinel: BufferSentinel[R, O],
    ) -> bool:
        tables_files = ledger.manifest()
        if not any(tables_files) and not disk_sentinel.idle_due():
            return False
        offset = buffer_sentinel.offset
        run.ctx.stage(tables_files, self.stage_state(run, offset))
        run.ingester.acknowledge_offset(offset)
        run.staged = snapshot(offset)
        run.restarts = 0
        ledger.clear()
        disk_sentinel.reset_timer()
        return True

    def restart(
        self,
        run: StreamRun[R, O],
        restart: StreamRestart,
        ledger: FileLedger,
        disk_sentinel: DiskSentinel,
        buffer_sentinel: BufferSentinel[R, O],
    ) -> None:
        run.restarts += 1
        if run.restarts > MAX_RESTARTS_WITHOUT_STAGE:
            raise ConnErrorCode.CONN_36.exception(
                cause=restart,
                restarts=run.restarts - 1,
                reason=restart.reason,
            )
        logger.warning(
            "Restarting the stream from the last staged offset, dropping the "
            "work pending since then, because %s",
            restart.reason,
        )
        buffer_sentinel.discard(snapshot(run.staged))
        ledger.discard()
        disk_sentinel.reset_timer()
        run.ingester.seed_offset(snapshot(run.staged))
        run.ingester.close_session(run.session)
        run.session = run.ingester.open_session(run.ctx.work_dir)

    def record(
        self,
        run: StreamRun[R, O],
        ledger: FileLedger,
        buffer_sentinel: BufferSentinel[R, O],
    ) -> None:
        if ledger.has_data:
            return
        offset = buffer_sentinel.offset
        if offset.write() == run.staged.write():
            return
        self.checkpoint(run, offset)

    def checkpoint(self, run: StreamRun[R, O], offset: O) -> bool:
        if offset is None:
            raise ConnErrorCode.CONN_34.exception(
                ingester=type(run.ingester).__name__, offset=offset
            )
        state = self.stage_state(run, offset)
        if isinstance(state, dict):
            if not self.may_checkpoint(run):
                return False
            run.ctx.checkpoint(state)
        else:
            state()
        run.staged = snapshot(offset)
        return True

    def settle_offset(self, run: StreamRun[R, O]) -> None:
        try:
            run.ingester.settle_offset()
        except Exception as exception:
            logger.warning(
                f"Could not record the final offset of the stream: {exception}"
            )
