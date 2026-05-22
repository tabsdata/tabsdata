"""Dialect-agnostic CDC run loop.

Per-database ``StreamSrcPlugin.run`` implementations build a
concrete ``CdcIngester`` and pass it to ``run_cdc`` along with the
plugin's ``StreamSrcPluginCtx`` and a ``CdcRunConfig`` carrying
the runner's tuning knobs."""

from __future__ import annotations

import logging
import signal
import sys
import threading
from dataclasses import dataclass
from pathlib import Path
from typing import Any

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
from tabsdatak.spi import StreamSrcPluginCtx

logger = logging.getLogger(__name__)

# Context state key under which the runner persists the current
# offset between trx invocations. Concrete offsets serialise to a
# dict via ``CdcOffset.write()`` and load back via
# ``CdcOffset.read``.
OFFSET_STATE_KEY = "cdc.offset"


@dataclass(frozen=True)
class CdcRunConfig:
    """Runner tuning knobs. The per-database ``StreamSrc``'s
    ``src_cfg`` typically maps onto these; the plugin builds a
    ``CdcRunConfig`` from the user's ``src_cfg`` before calling
    ``run_cdc``."""

    cdc_format: CdcFormat = CdcFormat()
    buffer_max_rows: int = DEFAULT_BUFFER_MAX_ROWS
    buffer_max_bytes: int | None = DEFAULT_BUFFER_MAX_BYTES
    buffer_max_secs: int = DEFAULT_BUFFER_MAX_SECS
    trigger_max_rows: int | None = DEFAULT_TRIGGER_MAX_ROWS
    trigger_max_bytes: int | None = DEFAULT_TRIGGER_MAX_BYTES
    trigger_max_secs: int = DEFAULT_TRIGGER_MAX_SECS
    poll_interval_seconds: int = DEFAULT_POLL_INTERVAL_SECONDS
    # Connector plugins resolve this from their ``src_cfg`` (e.g.
    # ``mysql.cdc.logging_level``). ``run_cdc`` applies it to the
    # whole ``tabsdatak.conn.common.sql.cdc`` namespace so the
    # runner / sentinels / ingester INFO milestones surface at
    # the level the user picked.
    logging_level: int = logging.ERROR


def _setup_signal_handlers() -> threading.Event:
    """Install SIGINT / SIGTERM handlers that flip a stop ``Event``.
    Windows gets the ``win32api`` console-handler too where
    available -- silently skipped otherwise."""
    stop_event = threading.Event()

    def handle_signal(*_args: Any) -> None:
        stop_event.set()

    signal.signal(signal.SIGINT, handle_signal)
    signal.signal(signal.SIGTERM, handle_signal)
    if sys.platform == "win32":  # pragma: no cover - Windows path
        try:
            # noinspection PyPackageRequirements,PyUnresolvedReferences
            import win32api

            win32api.SetConsoleCtrlHandler(handle_signal, True)
        except ImportError:
            pass

    return stop_event


def _load_offset(ctx: StreamSrcPluginCtx, ingester: CdcIngester) -> Any:
    """Restore the offset persisted by the previous trx, or fall
    back to the ingester's ``base_offset`` when this is a cold
    start."""
    state = ctx.read_state()
    serialised = state.get(OFFSET_STATE_KEY) if state else None
    if serialised is None:
        return ingester.base_offset()
    base = ingester.base_offset()
    # ``CdcOffset.read`` is a classmethod; ask the concrete offset
    # type the ingester just produced.
    return type(base).read(serialised)


def _save_offset(ctx: StreamSrcPluginCtx, offset: Any) -> None:
    ctx.write_state({OFFSET_STATE_KEY: offset.write()})


def run_cdc(
    *,
    ctx: StreamSrcPluginCtx,
    ingester: CdcIngester,
    cfg: CdcRunConfig,
) -> None:
    """Common CDC run loop.

    1. Install signal handlers + open the ingester session.
    2. Harvest tables (catalog snapshot + schema registry seed).
    3. Build disk sentinel (working dir + thresholds) and buffer
       sentinel (offset + thresholds).
    4. While the ctx is active and no stop signal: retry pending
       trigger, poll the change stream, drain into the buffer,
       expire time-based flushes, flush the disk sentinel, commit
       the offset back to the source and persist it on the ctx.
    5. On shutdown: flush remaining buffers + disk and commit one
       last offset.

    The function returns normally on graceful stop (ctx inactive
    or signal raised); any uncaught exception propagates."""
    # Bring every logger under the cdc namespace (runner +
    # sentinels + ingester base) to the level the plugin picked
    # via ``cfg.logging_level``. Per-connector loggers (mysql /
    # postgres / db2) are set independently by their plugins.
    logging.getLogger("tabsdatak.conn.common.sql.cdc").setLevel(
        cfg.logging_level,
    )

    stop_signal = _setup_signal_handlers()
    session = None
    buffer_sentinel: BufferSentinel | None = None
    disk_sentinel: DiskSentinel | None = None
    try:
        logger.info(
            "run_cdc: starting (ingester=%s)",
            ingester.ingester_type,
        )
        session = ingester.open_session()
        tracked_tables = ingester.harvest_tables(
            session=session,
            cdc_format=cfg.cdc_format,
        )
        logger.info(
            "run_cdc: harvested %d table(s): %s",
            len(tracked_tables),
            sorted(tracked_tables.keys()),
        )

        disk_sentinel = DiskSentinel(
            ctx=ctx,
            landing=Path(ctx.work_dir),
            max_rows=cfg.trigger_max_rows,
            max_bytes=cfg.trigger_max_bytes,
            max_secs=cfg.trigger_max_secs,
            tables=tracked_tables,
            cdc_format=cfg.cdc_format,
        )

        # Resume from the persisted offset if any -- the runner's
        # buffer sentinel uses this to seed its running offset.
        base_offset = _load_offset(ctx, ingester)
        buffer_sentinel = BufferSentinel(
            disk_sentinel=disk_sentinel,
            max_rows=cfg.buffer_max_rows,
            max_bytes=cfg.buffer_max_bytes,
            max_secs=cfg.buffer_max_secs,
            tables=tracked_tables,
            offset=base_offset,
        )

        buffer_sentinel.reset_timer()
        disk_sentinel.reset_timer()
        logger.info("run_cdc: entering poll loop")

        while not stop_signal.is_set() and ctx.is_active():
            disk_sentinel.retry_trigger()
            buffer_sentinel.process_changes(
                changes=ingester.poll_changes(session),
            )
            buffer_sentinel.expire()
            if disk_sentinel.flush(force=False):
                ingester.commit_offset(buffer_sentinel.offset)
                _save_offset(ctx, buffer_sentinel.offset)
                logger.info(
                    "run_cdc: flushed + committed offset",
                )
            stop_signal.wait(timeout=cfg.poll_interval_seconds)

        if stop_signal.is_set():
            logger.info("run_cdc: interrupted; exiting loop")
        else:
            logger.info("run_cdc: ctx inactive; exiting loop")
    finally:
        # Drain in-flight rows + parquet files on shutdown so we
        # don't leave a half-batch unstaged. Errors here are
        # logged but the original exception (if any) is preferred.
        try:
            if buffer_sentinel is not None and buffer_sentinel.has_data:
                buffer_sentinel.flush()
            if disk_sentinel is not None:
                if disk_sentinel.flush(force=True):
                    if buffer_sentinel is not None:
                        ingester.commit_offset(buffer_sentinel.offset)
                        _save_offset(ctx, buffer_sentinel.offset)
        except Exception:
            logger.exception("run_cdc: shutdown flush failed")
        if session is not None:
            ingester.close_session(session)
        logger.info("run_cdc: stopped")
