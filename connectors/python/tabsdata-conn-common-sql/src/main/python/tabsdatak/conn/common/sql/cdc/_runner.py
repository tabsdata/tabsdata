#
# Copyright 2026 Tabs Data Inc.
#

"""Dialect-agnostic CDC stream-source plugin base.

Per-database connectors subclass :class:`CdcPlugin` and fill in two
hooks: :meth:`CdcPlugin.build_run_config` (translate the user-facing
``src_cfg`` into a :class:`CdcRunConfig`) and
:meth:`CdcPlugin.build_ingester` (construct the connector-specific
:class:`~tabsdatak.conn.common.sql.cdc._ingester.CdcIngester`, e.g.
``Wal2JsonIngester`` / ``BinlogIngester``). :meth:`CdcPlugin.run` owns
everything that is the same across connectors: log-level wiring, signal
handling, the buffer / disk sentinels, the poll loop, the two-phase
``stage`` / ``commit`` publish, offset commit + per-flow state
persistence, and downstream-trigger backpressure.

The ingester provides the database-specific surface
(``open_session`` / ``harvest_tables`` / ``base_offset`` /
``commit_offset`` / ``poll_changes`` / ``close_session``); the loop never
reaches into a particular replication protocol.
"""

from __future__ import annotations

import logging
import signal
import sys
import threading
from abc import ABC, abstractmethod
from dataclasses import dataclass, field

from tabsdatak.api import StreamSrc
from tabsdatak.conn.common.sql.cdc._ingester import CdcIngester
from tabsdatak.conn.common.sql.cdc._sentinel.buffer import BufferSentinel
from tabsdatak.conn.common.sql.cdc._sentinel.disk import DiskSentinel
from tabsdatak.conn.common.sql.cdc.constants import (
    DEFAULT_BUFFER_MAX_BYTES,
    DEFAULT_BUFFER_MAX_ROWS,
    DEFAULT_BUFFER_MAX_SECS,
    DEFAULT_POLL_INTERVAL_SECONDS,
    DEFAULT_TRIGGER_MAX_BYTES,
    DEFAULT_TRIGGER_MAX_ROWS,
    DEFAULT_TRIGGER_MAX_SECS,
)
from tabsdatak.conn.common.sql.cdc.typing import CdcFormat
from tabsdatak.spi import StageRef, StreamSrcPlugin, StreamSrcPluginCtx

logger = logging.getLogger(__name__)

# Connector loggers live under this namespace; :meth:`CdcPlugin.run` sets
# the level here so the plugin, sentinels and ingester all report at the
# configured verbosity.
_COMMON_SQL_NAMESPACE = "tabsdatak.conn.common.sql"


@dataclass(frozen=True, kw_only=True)
class CdcRunConfig:
    """Per-flow knobs for the CDC run loop.

    Built by the connector plugin (:meth:`CdcPlugin.build_run_config`)
    from the user-facing ``src_cfg`` (falling back to the package
    defaults in :mod:`tabsdatak.conn.common.sql.cdc.constants`).
    """

    cdc_format: CdcFormat = field(default_factory=CdcFormat)
    buffer_max_rows: int = DEFAULT_BUFFER_MAX_ROWS
    buffer_max_bytes: int | None = DEFAULT_BUFFER_MAX_BYTES
    buffer_max_secs: int = DEFAULT_BUFFER_MAX_SECS
    trigger_max_rows: int | None = DEFAULT_TRIGGER_MAX_ROWS
    trigger_max_bytes: int | None = DEFAULT_TRIGGER_MAX_BYTES
    trigger_max_secs: int = DEFAULT_TRIGGER_MAX_SECS
    poll_interval_seconds: int = DEFAULT_POLL_INTERVAL_SECONDS
    logging_level: int = logging.ERROR


def _setup_signal_handlers() -> threading.Event:
    """Install SIGINT / SIGTERM (and SIGBREAK on Windows) handlers that
    set a stop event, so the poll loop unwinds cleanly. No-op off the
    main thread (the runtime / tests drive shutdown via ``is_active``)."""
    stop_event = threading.Event()

    if threading.current_thread() is not threading.main_thread():
        return stop_event

    def handle_signal(signum, _frame):
        logger.info(f"Received termination signal ({signum}). Shutting down...")
        stop_event.set()

    signal.signal(signal.SIGINT, handle_signal)
    signal.signal(signal.SIGTERM, handle_signal)

    if sys.platform == "win32":
        signal.signal(signal.SIGBREAK, handle_signal)
        try:
            # noinspection PyPackageRequirements,PyUnresolvedReferences
            import win32api

            win32api.SetConsoleCtrlHandler(handle_signal, True)
        except ImportError:
            pass

    return stop_event


def _publish(
    ctx: StreamSrcPluginCtx,
    disk_sentinel: DiskSentinel,
    buffer_sentinel: BufferSentinel,
    ingester: CdcIngester,
) -> bool:
    """Stage + commit the accumulated batch through ``ctx`` and persist
    the resume offset.

    The disk sentinel hands us a positional per-output-table file-list
    manifest (a :data:`~tabsdatak.spi.StreamManifestSpec`); the runtime
    stages it (returning an opaque :class:`~tabsdatak.spi.StageRef`) and
    commits. The source-side offset is then advanced
    (``ingester.commit_offset``) and written to per-flow state so the
    next run resumes where this one left off.

    Returns whether downstream is back-pressuring (``True`` -> a later
    ``trigger_monitor`` must clear before publishing resumes).
    """
    manifest = disk_sentinel.build_manifest()
    if not any(manifest):
        return False
    stage_ref: StageRef = ctx.stage(manifest)
    ctx.commit(stage_ref)
    ingester.commit_offset(buffer_sentinel.offset)
    ctx.write_state(buffer_sentinel.offset.write())
    disk_sentinel.clear()
    # Notify downstream a batch is ready. A ``False`` return means
    # downstream is back-pressuring; pause publishing until
    # ``trigger_monitor`` clears (checked at the top of the loop).
    return not ctx.trigger()


def _run_loop(
    *,
    ctx: StreamSrcPluginCtx,
    ingester: CdcIngester,
    cfg: CdcRunConfig,
) -> None:
    """Drive ``ingester`` until ``ctx.is_active()`` flips to ``False`` or a
    termination signal arrives.

    Opens the ingester session, harvests the tracked-table schema, then
    polls for changes, buffering them in memory and spilling to the work
    dir; completed batches are staged + committed through ``ctx``. On a
    graceful end-of-stream the trailing partial batch is flushed before
    returning. Invoked by :meth:`CdcPlugin.run`.
    """
    buffer_sentinel: BufferSentinel | None = None
    disk_sentinel: DiskSentinel | None = None
    session = None
    trigger_pending = False
    interrupted = False
    errored = False

    stop_signal = _setup_signal_handlers()
    try:
        session = ingester.open_session()
        tracked_tables = ingester.harvest_tables(
            session=session,
            cdc_format=cfg.cdc_format,
        )
        disk_sentinel = DiskSentinel(
            landing=ctx.work_dir,
            max_rows=cfg.trigger_max_rows,
            max_bytes=cfg.trigger_max_bytes,
            max_secs=cfg.trigger_max_secs,
            tables=tracked_tables,
            cdc_format=cfg.cdc_format,
        )
        buffer_sentinel = BufferSentinel(
            disk_sentinel=disk_sentinel,
            max_rows=cfg.buffer_max_rows,
            max_bytes=cfg.buffer_max_bytes,
            max_secs=cfg.buffer_max_secs,
            tables=tracked_tables,
            offset=ingester.base_offset(),
        )

        buffer_sentinel.reset_timer()
        disk_sentinel.reset_timer()

        while not stop_signal.is_set() and ctx.is_active():
            if trigger_pending and ctx.trigger_monitor():
                trigger_pending = False
            buffer_sentinel.process_changes(
                changes=ingester.poll_changes(session),
            )
            buffer_sentinel.expire()
            if disk_sentinel.should_flush(force=False):
                trigger_pending = _publish(
                    ctx, disk_sentinel, buffer_sentinel, ingester
                )
            stop_signal.wait(timeout=cfg.poll_interval_seconds)
        if stop_signal.is_set():
            interrupted = True
            logger.info("cdc: interrupted; exiting loop.")
    except Exception as exception:
        errored = True
        logger.error(f"Error in CDC run loop: {exception}")
        raise
    finally:
        # On a graceful end-of-stream (``ctx.is_active()`` flipped to
        # ``False`` with no signal and no error) publish the trailing
        # partial batch before exiting. A signalled or errored exit
        # leaves the buffer for the next run to resume from the offset.
        if not interrupted and not errored:
            if buffer_sentinel is not None:
                buffer_sentinel.flush()
            if disk_sentinel is not None and disk_sentinel.should_flush(force=True):
                _publish(ctx, disk_sentinel, buffer_sentinel, ingester)
        if session is not None:
            ingester.close_session(session)


class CdcPlugin[SS: StreamSrc](StreamSrcPlugin[SS], ABC):
    """Abstract :class:`~tabsdatak.spi.StreamSrcPlugin` for CDC stream
    sources.

    Concrete per-database plugins implement :meth:`build_run_config` and
    :meth:`build_ingester`; :meth:`run` supplies the shared lifecycle
    (log-level wiring + the dialect-agnostic poll/stage/commit loop), so a
    connector's plugin carries no run-loop boilerplate.
    """

    @abstractmethod
    def build_run_config(self, stream_src: SS) -> CdcRunConfig:
        """Translate the user-facing ``src_cfg`` on ``stream_src`` into a
        :class:`CdcRunConfig` (buffer / trigger limits, poll interval,
        output format, logging level)."""

    @abstractmethod
    def build_ingester(self, stream_src: SS) -> CdcIngester:
        """Construct the connector-specific
        :class:`~tabsdatak.conn.common.sql.cdc._ingester.CdcIngester` from
        ``stream_src`` (connection, tables, start position, protocol
        knobs)."""

    def run(
        self,
        ctx: StreamSrcPluginCtx,
        stream_src: SS,
    ) -> None:
        cfg = self.build_run_config(stream_src)
        # Sync the shared common-sql machinery (sentinels / loop) and this
        # connector's own plugin logger with the configured verbosity.
        logging.getLogger(_COMMON_SQL_NAMESPACE).setLevel(cfg.logging_level)
        plugin_logger = logging.getLogger(type(self).__module__)
        plugin_logger.setLevel(cfg.logging_level)

        ingester = self.build_ingester(stream_src)
        plugin_logger.info("cdc: starting (%s)", type(ingester).__name__)
        try:
            _run_loop(ctx=ctx, ingester=ingester, cfg=cfg)
        finally:
            plugin_logger.info("cdc: stopped")
