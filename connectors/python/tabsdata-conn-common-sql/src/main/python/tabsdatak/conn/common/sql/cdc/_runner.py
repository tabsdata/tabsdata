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
handling, the buffer / disk sentinels, the poll loop (honouring
``ctx.flush_interval_mins``), and the single-shot ``ctx.stage`` publish
that commits each batch's resume offset via the ingester's
:meth:`~CdcIngester.commit_state`.

The ingester provides the database-specific surface
(``open_session`` / ``harvest_tables`` / ``base_offset`` /
``commit_offset`` / ``poll_changes`` / ``close_session``) and owns the
offset policy -- Tabsdata-persisted (``stream_state``) or external
(source-managed) -- via :meth:`~CdcIngester.resume_offset` /
:meth:`~CdcIngester.commit_state`. The loop never reaches into a
particular replication protocol.
"""

from __future__ import annotations

import logging
import signal
import sys
import threading
import time
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
from tabsdatak.spi import StreamSrcPlugin, StreamSrcPluginCtx

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
    main thread (the runtime / tests drive shutdown via ``ctx.active``)."""
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
    """Stage the accumulated batch through ``ctx`` and commit the offset.

    The disk sentinel hands us a positional per-output-table file-list
    (entry ``i`` -> table ``i``), which we hand to ``ctx.stage`` along
    with the ingester's :meth:`~CdcIngester.commit_state` -- the ingester
    decides whether Tabsdata persists the offset (read back via
    ``ctx.stream_state``) or the source commits it itself.

    Returns whether anything was staged (so the caller can reset its
    flush-interval clock only on a real flush).
    """
    tables_files = disk_sentinel.build_manifest()
    if not any(tables_files):
        return False
    # The ingester decides how the new offset is committed: Tabsdata-owned
    # ingesters persist it (``StoreStreamState``); external-owned ones
    # (e.g. a Postgres slot) commit on the source. ``ctx.stage`` runs that
    # commit when it publishes the batch.
    ctx.stage(tables_files, ingester.commit_state(buffer_sentinel.offset))
    disk_sentinel.clear()
    return True


def _run_loop(
    *,
    ctx: StreamSrcPluginCtx,
    ingester: CdcIngester,
    cfg: CdcRunConfig,
) -> None:
    """Drive ``ingester`` until ``ctx.active`` flips to ``False`` or a
    termination signal arrives.

    Opens the ingester session, harvests the tracked-table schema, then
    polls for changes, buffering them in memory and spilling to the work
    dir; a batch is staged through ``ctx`` when the disk sentinel's
    row/byte/time thresholds fire **or** ``ctx.flush_interval_mins`` has
    elapsed since the last stage. On a graceful end-of-stream the trailing
    partial batch is flushed before returning. Invoked by
    :meth:`CdcPlugin.run`.
    """
    buffer_sentinel: BufferSentinel | None = None
    disk_sentinel: DiskSentinel | None = None
    session = None
    interrupted = False
    errored = False

    flush_interval_secs = max(ctx.flush_interval_mins, 0) * 60

    stop_signal = _setup_signal_handlers()
    try:
        # Seed the resume position before the stream opens: from the
        # persisted ``stream_state`` (Tabsdata-owned offset) or left to
        # the source (external-owned / cold start). ``first_stage`` ->
        # seed from the configured ``start_from``.
        ingester.seed_offset(
            ingester.resume_offset(
                first_stage=ctx.first_stage,
                stream_state=ctx.stream_state,
            )
        )
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
        last_flush = time.time()

        while not stop_signal.is_set() and ctx.active:
            buffer_sentinel.process_changes(
                changes=ingester.poll_changes(session),
            )
            buffer_sentinel.expire()
            # Stage on the sentinel's own thresholds, or once the
            # runtime-imposed flush interval has elapsed (and we have
            # something buffered to stage).
            interval_due = (
                flush_interval_secs > 0
                and (time.time() - last_flush) >= flush_interval_secs
                and disk_sentinel.has_data
            )
            if disk_sentinel.should_flush(force=False) or interval_due:
                _publish(ctx, disk_sentinel, buffer_sentinel, ingester)
                last_flush = time.time()
            stop_signal.wait(timeout=cfg.poll_interval_seconds)
        if stop_signal.is_set():
            interrupted = True
            logger.info("cdc: interrupted; exiting loop.")
    except Exception as exception:
        errored = True
        logger.error(f"Error in CDC run loop: {exception}")
        raise
    finally:
        # On a graceful end-of-stream (``ctx.active`` flipped to ``False``
        # with no signal and no error) publish the trailing partial batch
        # before exiting. A signalled or errored exit leaves the buffer
        # for the next run to resume from the offset.
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
