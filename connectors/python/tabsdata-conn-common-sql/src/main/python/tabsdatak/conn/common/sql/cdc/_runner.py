#
# Copyright 2026 Tabsdata Inc.
#

"""Dialect-agnostic CDC stream-source plugin base.

Per-database connectors subclass :class:`CdcPlugin` and fill in two
hooks: :meth:`CdcPlugin.build_run_config` (translate the user-facing
``src_cfg`` into a :class:`CdcRunConfig`) and
:meth:`CdcPlugin.build_ingester` (construct the connector-specific
:class:`~tabsdatak.conn.common.sql.cdc._ingester.CdcIngester`, e.g.
``Wal2JsonIngester`` / ``BinlogIngester``). :meth:`CdcPlugin.run` owns
everything that is the same across connectors: log-level wiring, signal
handling, the buffer / disk sentinels, the large-column policy
(:class:`~tabsdatak.conn.common.sql.cdc._large.LargeColumnPolicy`) every
polled change passes through, the poll loop (honouring
``ctx.flush_interval_mins``), and the single-shot ``ctx.stage`` publish
that commits each batch's resume offset via the ingester's
:meth:`~CdcIngester.commit_state`.

The ingester provides the database-specific surface
(``open_session`` / ``harvest_tables`` / ``base_offset`` /
``commit_offset`` / ``poll_changes`` / ``close_session``) and owns the
offset policy -- Tabsdata-persisted (``stream_state``) or external
(source-managed) -- via :meth:`~CdcIngester.resume_offset` /
:meth:`~CdcIngester.commit_state`, plus the source-side acknowledgement
of a staged offset via :meth:`~CdcIngester.commit_offset`. The loop never
reaches into a particular replication protocol.
"""

from __future__ import annotations

import logging
import math
import shutil
import signal
import sys
import threading
import time
from abc import ABC, abstractmethod
from dataclasses import dataclass, field

from tabsdatak.api import StreamSrc
from tabsdatak.conn.common.sql.cdc._ingester import (
    CDC_INITIAL_LOAD_STATE_KEY,
    CdcIngester,
)
from tabsdatak.conn.common.sql.cdc._initial_load import InitialLoader
from tabsdatak.conn.common.sql.cdc._large import LargeColumnPolicy
from tabsdatak.conn.common.sql.cdc._model import CdcOffset
from tabsdatak.conn.common.sql.cdc._sentinel.buffer import BufferSentinel
from tabsdatak.conn.common.sql.cdc._sentinel.disk import DiskSentinel
from tabsdatak.conn.common.sql.cdc.constants import (
    DEFAULT_BUFFER_MAX_BYTES,
    DEFAULT_BUFFER_MAX_ROWS,
    DEFAULT_BUFFER_MAX_SECS,
    DEFAULT_CHUNK_SIZE,
    DEFAULT_FETCH_SIZE,
    DEFAULT_MAX_LARGE_TEXT_LENGTH,
    DEFAULT_POLL_INTERVAL_SECONDS,
    DEFAULT_TRIGGER_MAX_BYTES,
    DEFAULT_TRIGGER_MAX_ROWS,
    DEFAULT_TRIGGER_MAX_SECS,
)
from tabsdatak.conn.common.sql.cdc.error import SqlCdcErrorCode
from tabsdatak.conn.common.sql.cdc.typing import CdcFormat
from tabsdatak.spi import Conn, StreamSrcPlugin, StreamSrcPluginCtx

# Work-dir subdirectory the initial load's parquet files land in, wiped
# at the start of a load so a previous attempt leaves nothing behind.
_INITIAL_LOAD_DIR = "_cdc_initial_load"

NO_TIME_LIMIT = math.inf

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
    max_large_text_length: int = DEFAULT_MAX_LARGE_TEXT_LENGTH
    initial_load: bool = False
    fetch_size: int = DEFAULT_FETCH_SIZE
    chunk_size: int = DEFAULT_CHUNK_SIZE
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
    ``ctx.stream_state``) or the source commits it itself. Once the batch
    is staged, :meth:`~CdcIngester.commit_offset` lets the ingester
    acknowledge the now-durable offset to the source.

    Returns whether anything was staged (so the caller can reset its
    flush-interval clock only on a real flush).
    """
    tables_files = disk_sentinel.build_manifest()
    if not any(tables_files):
        return False
    offset = buffer_sentinel.offset
    ctx.stage(tables_files, ingester.commit_state(offset))
    ingester.commit_offset(offset)
    disk_sentinel.clear()
    return True


def _initial_load_pending(ctx: StreamSrcPluginCtx, cfg: CdcRunConfig) -> bool:
    """Whether this run owes an initial load: the source asks for one and
    no previous run has staged it. The flag is written as ``False``
    together with the load's own batch, so a run interrupted before that
    batch is staged starts the load again."""
    if not cfg.initial_load:
        return False
    state = ctx.stream_state or {}
    return state.get(CDC_INITIAL_LOAD_STATE_KEY, True)


def _load_initially(
    *,
    ctx: StreamSrcPluginCtx,
    ingester: CdcIngester,
    cfg: CdcRunConfig,
    large_column_policy: LargeColumnPolicy,
) -> CdcOffset:
    """Snapshot the tracked tables and stage them as a single batch.

    Returns the offset change capture resumes from. The whole load is one
    ``ctx.stage`` call, so it is all-or-nothing: an error anywhere leaves
    no manifest and no state, and the next run redoes it.
    """
    landing = ctx.work_dir / _INITIAL_LOAD_DIR
    if landing.exists():
        shutil.rmtree(landing)
    landing.mkdir(parents=True)

    session = ingester.open_initial_load()
    try:
        tracked_tables = ingester.collect_tables(
            session=session.engine,
            cdc_format=cfg.cdc_format,
        )
        disk_sentinel = DiskSentinel(
            landing=landing,
            max_rows=cfg.buffer_max_rows,
            max_bytes=cfg.buffer_max_bytes,
            max_secs=NO_TIME_LIMIT,
            tables=tracked_tables,
            cdc_format=cfg.cdc_format,
        )
        loader = InitialLoader(
            ingester=ingester,
            disk_sentinel=disk_sentinel,
            cdc_format=cfg.cdc_format,
            large_column_policy=large_column_policy,
            fetch_size=cfg.fetch_size,
            chunk_size=cfg.chunk_size,
        )
        offset = loader.load(session)
    finally:
        session.close()

    state = ingester.commit_state(offset)
    if not isinstance(state, dict):
        raise SqlCdcErrorCode.CDC_21.exception(ingester=type(ingester).__name__)
    ctx.stage(
        disk_sentinel.build_manifest(),
        {**state, CDC_INITIAL_LOAD_STATE_KEY: False},
    )
    ingester.commit_offset(offset)
    logger.info("Initial load staged")
    return offset


def _run_loop(
    *,
    ctx: StreamSrcPluginCtx,
    ingester: CdcIngester,
    cfg: CdcRunConfig,
) -> None:
    """Drive ``ingester`` until ``ctx.active`` flips to ``False`` or a
    termination signal arrives.

    Runs the initial load first when one is owed, then opens the ingester
    session, harvests the tracked-table schema and polls for changes,
    buffering them in memory and spilling to the work dir; a batch is
    staged through ``ctx`` when the disk sentinel's row/byte/time
    thresholds fire **or** ``ctx.flush_interval_mins`` has elapsed since
    the last stage. On a graceful end-of-stream the trailing partial batch
    is flushed before returning. Invoked by :meth:`CdcPlugin.run`.
    """
    buffer_sentinel: BufferSentinel | None = None
    disk_sentinel: DiskSentinel | None = None
    session = None
    interrupted = False
    errored = False

    flush_interval_secs = max(ctx.flush_interval_mins, 0) * 60

    large_column_policy = LargeColumnPolicy(
        max_text_length=cfg.max_large_text_length,
    )

    stop_signal = _setup_signal_handlers()
    try:
        if _initial_load_pending(ctx, cfg):
            # The load's own offset becomes the resume position, so the
            # stream opens exactly where the snapshot ends.
            ingester.seed_offset(
                _load_initially(
                    ctx=ctx,
                    ingester=ingester,
                    cfg=cfg,
                    large_column_policy=large_column_policy,
                )
            )
        else:
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
                changes=large_column_policy.apply(ingester.poll_changes(session)),
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


class CdcPlugin[C: Conn, SS: StreamSrc](StreamSrcPlugin[C, SS], ABC):
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
    def build_ingester(self, conn: C, stream_src: SS) -> CdcIngester:
        """Construct the connector-specific
        :class:`~tabsdatak.conn.common.sql.cdc._ingester.CdcIngester` from
        ``conn`` + ``stream_src`` (connection, tables, start position,
        protocol knobs)."""

    def run(
        self,
        ctx: StreamSrcPluginCtx,
        conn: C,
        stream_src: SS,
    ) -> None:
        cfg = self.build_run_config(stream_src)
        # Sync the shared common-sql machinery (sentinels / loop) and this
        # connector's own plugin logger with the configured verbosity.
        logging.getLogger(_COMMON_SQL_NAMESPACE).setLevel(cfg.logging_level)
        plugin_logger = logging.getLogger(type(self).__module__)
        plugin_logger.setLevel(cfg.logging_level)

        ingester = self.build_ingester(conn, stream_src)
        plugin_logger.info("cdc: starting (%s)", type(ingester).__name__)
        try:
            _run_loop(ctx=ctx, ingester=ingester, cfg=cfg)
        finally:
            plugin_logger.info("cdc: stopped")
