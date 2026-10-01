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

from tabsdatak.api import StreamSrc
from tabsdatak.conn.common._stream._config import StreamRunConfig
from tabsdatak.conn.common._stream._encoder import StreamEncoder
from tabsdatak.conn.common._stream._ingester import StreamIngester
from tabsdatak.conn.common._stream._ledger import FileLedger
from tabsdatak.conn.common._stream._offset import StreamOffset
from tabsdatak.conn.common._stream._sentinels import BufferSentinel, DiskSentinel
from tabsdatak.conn.common._stream._signal import StopSignal
from tabsdatak.conn.common.error import ConnErrorCode
from tabsdatak.spi import Conn, StreamSrcPlugin, StreamSrcPluginCtx, StreamStateSpec

logger = logging.getLogger(__name__)

STREAM_NAMESPACE = "tabsdatak.conn.common._stream"


def manifest_secs(trigger_max_secs: float, flush_interval_secs: float) -> float:
    if flush_interval_secs > 0:
        return min(trigger_max_secs, flush_interval_secs)
    return trigger_max_secs


@dataclass(kw_only=True)
class StreamRun[R, O: StreamOffset]:
    ctx: StreamSrcPluginCtx
    ingester: StreamIngester[R, O]
    cfg: StreamRunConfig
    stop_signal: StopSignal
    session: Any = None
    slots: list[str] = field(default_factory=list)


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
        return StreamRun(ctx=ctx, ingester=ingester, cfg=cfg, stop_signal=stop_signal)

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
        return run.ingester.commit_state(offset)

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

        flush_interval_secs = max(ctx.flush_interval_mins, 0) * 60

        stop_signal = StopSignal.install()
        run = self.build_run(
            ctx=ctx, ingester=ingester, cfg=cfg, stop_signal=stop_signal
        )
        cfg = run.cfg
        try:
            self.seed(run)
            ingester.watch_position(lambda offset: self.checkpoint(run, offset))
            run.session = ingester.open_session(ctx.work_dir)
            run.slots = list(self.slots(run))
            if not self.before_stream(run):
                interrupted = True
                return
            ledger = FileLedger(landing=ctx.work_dir, slots=run.slots)
            buffer_sentinel = BufferSentinel(
                ledger=ledger,
                encoder=self.build_encoder(run),
                slots=run.slots,
                max_rows=cfg.buffer_max_rows,
                max_bytes=cfg.buffer_max_bytes,
                max_secs=cfg.buffer_max_secs,
                offset=ingester.base_offset(),
                measure_bytes=cfg.trigger_max_bytes is not None,
                clock=self.clock,
            )
            disk_sentinel = DiskSentinel(
                ledger=ledger,
                buffer=buffer_sentinel,
                max_rows=cfg.trigger_max_rows,
                max_bytes=cfg.trigger_max_bytes,
                max_secs=manifest_secs(cfg.trigger_max_secs, flush_interval_secs),
                clock=self.clock,
            )

            buffer_sentinel.reset_timer()
            disk_sentinel.reset_timer()

            while not stop_signal.is_set() and ctx.active:
                buffer_sentinel.add_all(
                    self.records(run, ingester.poll_changes(run.session)),
                )
                if buffer_sentinel.due():
                    buffer_sentinel.flush()
                if disk_sentinel.due():
                    buffer_sentinel.flush()
                    self.publish(run, ledger, disk_sentinel, buffer_sentinel)
                stop_signal.wait(timeout=cfg.poll_interval_seconds)
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
            if run.session is not None:
                if not errored:
                    self.settle(run)
                ingester.close_session(run.session)

    def publish(
        self,
        run: StreamRun[R, O],
        ledger: FileLedger,
        disk_sentinel: DiskSentinel,
        buffer_sentinel: BufferSentinel[R, O],
    ) -> bool:
        tables_files = ledger.manifest()
        if not any(tables_files):
            return False
        offset = buffer_sentinel.offset
        run.ctx.stage(tables_files, self.stage_state(run, offset))
        run.ingester.commit_offset(offset)
        ledger.clear()
        disk_sentinel.reset_timer()
        return True

    def checkpoint(self, run: StreamRun[R, O], offset: O) -> None:
        if offset is None:
            raise ConnErrorCode.CONN_34.exception(
                ingester=type(run.ingester).__name__, offset=offset
            )
        if not self.may_checkpoint(run):
            return
        state = self.stage_state(run, offset)
        if not isinstance(state, dict):
            raise ConnErrorCode.CONN_34.exception(
                ingester=type(run.ingester).__name__, offset=offset
            )
        run.ctx.checkpoint(state)

    def settle(self, run: StreamRun[R, O]) -> None:
        try:
            run.ingester.settle()
        except Exception as exception:
            logger.warning(
                f"Could not record the final position of the stream: {exception}"
            )
