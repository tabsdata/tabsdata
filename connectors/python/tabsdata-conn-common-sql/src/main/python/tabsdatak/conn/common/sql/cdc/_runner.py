#
# Copyright 2026 Tabsdata Inc.
#

"""Dialect-agnostic CDC stream-source plugin base.

Per-database connectors subclass :class:`CdcPlugin` and fill in two
hooks: :meth:`CdcPlugin.build_run_config` (translate the user-facing
``src_cfg`` into a :class:`CdcRunConfig`) and
:meth:`CdcPlugin.build_ingester` (construct the connector-specific
:class:`~tabsdatak.conn.common.sql.cdc._ingester.CdcIngester`, e.g.
``Wal2JsonIngester`` / ``BinlogIngester``). The run loop itself is the
shared :class:`~tabsdatak.conn.common._stream._plugin.PipelineStreamSrcPlugin`
one; :class:`CdcPlugin` fills in its CDC hooks: the initial load and its
window, the tracked tables as output slots, the
:class:`~tabsdatak.conn.common.sql.cdc._encoder.CdcEncoder`, the
large-column policy
(:class:`~tabsdatak.conn.common.sql.cdc._large.LargeColumnPolicy`) every
polled change passes through, and the initial-load flag kept in the
stream state.

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
from abc import ABC, abstractmethod
from collections.abc import Iterable, Sequence
from dataclasses import dataclass, field
from pathlib import Path

from tabsdatak.api import StreamSrc

# noinspection PyProtectedMember
from tabsdatak.conn.common._stream._config import StreamRunConfig

# noinspection PyProtectedMember
from tabsdatak.conn.common._stream._ledger import FileLedger

# noinspection PyProtectedMember
from tabsdatak.conn.common._stream._plugin import PipelineStreamSrcPlugin, StreamRun

# noinspection PyProtectedMember
from tabsdatak.conn.common._stream._sentinels import BufferSentinel, ChunkSentinel

# noinspection PyProtectedMember
from tabsdatak.conn.common._stream._signal import StopSignal
from tabsdatak.conn.common.sql.cdc._deduplicator import (
    deduplicate_initial_load_files,
)
from tabsdatak.conn.common.sql.cdc._encoder import CdcEncoder
from tabsdatak.conn.common.sql.cdc._ingester import (
    CDC_INITIAL_LOAD_STATE_KEY,
    CdcIngester,
)
from tabsdatak.conn.common.sql.cdc._initial_load import InitialLoader
from tabsdatak.conn.common.sql.cdc._large import LargeColumnPolicy
from tabsdatak.conn.common.sql.cdc._model import CdcChange, CdcOffset
from tabsdatak.conn.common.sql.cdc.constants import (
    DEFAULT_CHUNK_SIZE,
    DEFAULT_FETCH_SIZE,
    DEFAULT_INITIAL_LOAD_MAX_WINDOW_CHANGES,
    DEFAULT_MAX_LARGE_TEXT_LENGTH,
)
from tabsdatak.conn.common.sql.cdc.error import SqlCdcErrorCode
from tabsdatak.conn.common.sql.cdc.typing import CdcFormat
from tabsdatak.spi import Conn, StreamSrcPluginCtx, StreamStateSpec

# Work-dir subdirectories the initial load's parquet files land in: the
# load's rows, the changes committed while it read them, and those of the
# changes deduplication keeps. Wiped at the start of a load so a previous
# attempt leaves nothing behind.
_INITIAL_LOAD_DIR = "_cdc_initial_load"
_INITIAL_LOAD_WINDOW_DIR = "_cdc_initial_load_window"
_INITIAL_LOAD_KEPT_DIR = "_cdc_initial_load_kept"

NO_TIME_LIMIT = math.inf

logger = logging.getLogger(__name__)

# Connector loggers live under this namespace; :meth:`CdcPlugin.run` sets
# the level here so the plugin, sentinels and ingester all report at the
# configured verbosity.
_COMMON_SQL_NAMESPACE = "tabsdatak.conn.common.sql"


@dataclass(frozen=True, kw_only=True)
class CdcRunConfig(StreamRunConfig):
    """Per-flow knobs for the CDC run loop.

    Built by the connector plugin (:meth:`CdcPlugin.build_run_config`)
    from the user-facing ``src_cfg`` (falling back to the package
    defaults in :mod:`tabsdatak.conn.common.sql.cdc.constants` and
    :mod:`tabsdatak.conn.common._stream.constants`).
    """

    cdc_format: CdcFormat = field(default_factory=CdcFormat)
    max_large_text_length: int = DEFAULT_MAX_LARGE_TEXT_LENGTH
    initial_load: bool = False
    fetch_size: int = DEFAULT_FETCH_SIZE
    chunk_size: int = DEFAULT_CHUNK_SIZE
    initial_load_max_window_changes: int = DEFAULT_INITIAL_LOAD_MAX_WINDOW_CHANGES


def _initial_load_pending(ctx: StreamSrcPluginCtx, cfg: CdcRunConfig) -> bool:
    """Whether this run owes an initial load: the source asks for one and
    no previous run has staged it. The flag is written as ``False``
    together with the load's own batch, so a run interrupted before that
    batch is staged starts the load again."""
    if not cfg.initial_load:
        return False
    state = ctx.stream_state or {}
    return state.get(CDC_INITIAL_LOAD_STATE_KEY, True)


@dataclass(frozen=True)
class _PendingLoad:
    """A load read but not staged yet: its files, and where the changes
    committed while it ran end."""

    manifest: list[list[Path]]
    window_end: CdcOffset


def _fresh(folder: Path) -> Path:
    if folder.exists():
        shutil.rmtree(folder)
    folder.mkdir(parents=True)
    return folder


def _load_initially(
    *,
    ctx: StreamSrcPluginCtx,
    ingester: CdcIngester,
    cfg: CdcRunConfig,
    large_column_policy: LargeColumnPolicy,
) -> tuple[CdcOffset, _PendingLoad | None]:
    """Snapshot the tracked tables, and stage them as a single batch when
    the snapshot is a point in time.

    Returns the offset change capture resumes from and, for a load whose
    session reports a window, the load left unstaged: the run loop reads
    the window's changes and stages both together
    (:func:`_stage_initial_load`). Either way the load is one ``ctx.stage``
    call, so it is all-or-nothing: an error anywhere leaves no manifest and
    no state, and the next run redoes it.
    """
    landing = _fresh(ctx.work_dir / _INITIAL_LOAD_DIR)

    session = ingester.open_initial_load()
    try:
        tracked_tables = ingester.collect_tables(
            session=session.engine,
            cdc_format=cfg.cdc_format,
        )
        ledger = FileLedger(landing=landing, slots=tracked_tables)
        chunk_sentinel = ChunkSentinel(
            ledger=ledger,
            max_rows=cfg.buffer_max_rows,
            max_bytes=cfg.buffer_max_bytes,
        )
        loader = InitialLoader(
            ingester=ingester,
            chunk_sentinel=chunk_sentinel,
            cdc_format=cfg.cdc_format,
            large_column_policy=large_column_policy,
            fetch_size=cfg.fetch_size,
            chunk_size=cfg.chunk_size,
        )
        offset = loader.load(session)
        window_end = session.window_end
    finally:
        session.close()

    if window_end is not None:
        return offset, _PendingLoad(manifest=ledger.manifest(), window_end=window_end)
    _stage_load_manifest(ctx, ingester, ledger.manifest(), offset)
    logger.info("Initial load staged")
    return offset, None


def _stage_load_manifest(
    ctx: StreamSrcPluginCtx,
    ingester: CdcIngester,
    manifest: list[list[Path]],
    offset: CdcOffset,
) -> None:
    state = ingester.commit_state(offset)
    if not isinstance(state, dict):
        raise SqlCdcErrorCode.CDC_21.exception(ingester=type(ingester).__name__)
    ctx.stage(manifest, {**state, CDC_INITIAL_LOAD_STATE_KEY: False})
    ingester.commit_offset(offset)


def _read_window(
    *,
    ctx: StreamSrcPluginCtx,
    ingester: CdcIngester,
    session,
    tracked_tables: dict,
    cfg: CdcRunConfig,
    large_column_policy: LargeColumnPolicy,
    window_end: CdcOffset,
    stop_signal: StopSignal,
) -> list[list[Path]] | None:
    """The changes committed while the load read the tables, as a manifest:
    read by the ingester's own poll, bounded at `window_end`, and written as
    the stream writes them. ``None`` when a signal stops the read."""
    ledger = FileLedger(
        landing=_fresh(ctx.work_dir / _INITIAL_LOAD_WINDOW_DIR),
        slots=tracked_tables,
    )
    buffer_sentinel = BufferSentinel(
        ledger=ledger,
        encoder=CdcEncoder(cfg.cdc_format, tracked_tables),
        slots=tracked_tables,
        max_rows=cfg.buffer_max_rows,
        max_bytes=cfg.buffer_max_bytes,
        max_secs=NO_TIME_LIMIT,
        offset=ingester.base_offset(),
    )
    limit = cfg.initial_load_max_window_changes
    read = 0
    ingester.bound_stream(window_end)
    while not ingester.stream_bound_reached():
        if stop_signal.is_set():
            return None
        for change in large_column_policy.apply(ingester.poll_changes(session)):
            read += 1
            if read > limit:
                raise SqlCdcErrorCode.CDC_29.exception(limit=limit)
            buffer_sentinel.add(change)
    buffer_sentinel.flush()
    logger.info("Initial load window read: %d change(s)", read)
    return ledger.manifest()


def _stage_initial_load(
    *,
    ctx: StreamSrcPluginCtx,
    ingester: CdcIngester,
    tracked_tables: dict,
    cfg: CdcRunConfig,
    pending: _PendingLoad,
    window: list[list[Path]],
) -> None:
    """Stage the load with the window's changes that still apply to it, at
    the window's end, as one batch."""
    registry = ingester.schema_registry
    kept = deduplicate_initial_load_files(
        loaded=pending.manifest,
        changes=window,
        tables=tracked_tables,
        schemas={table: registry.version(table, 0) for table in tracked_tables},
        cdc_format=cfg.cdc_format,
        landing=_fresh(ctx.work_dir / _INITIAL_LOAD_KEPT_DIR),
    )
    manifest = [
        loaded + changes for loaded, changes in zip(pending.manifest, kept, strict=True)
    ]
    _stage_load_manifest(ctx, ingester, manifest, pending.window_end)
    logger.info(
        "Initial load staged with %d deduplicated change file(s)",
        sum(len(files) for files in kept),
    )


@dataclass(kw_only=True)
class CdcRun(StreamRun[CdcChange, CdcOffset]):
    cfg: CdcRunConfig
    ingester: CdcIngester
    large_column_policy: LargeColumnPolicy
    tracked_tables: dict = field(default_factory=dict)
    pending_load: _PendingLoad | None = None


class CdcPlugin[C: Conn, SS: StreamSrc](
    PipelineStreamSrcPlugin[C, SS, CdcChange, CdcOffset], ABC
):
    """Abstract :class:`~tabsdatak.spi.StreamSrcPlugin` for CDC stream
    sources.

    Concrete per-database plugins implement :meth:`build_run_config` and
    :meth:`build_ingester`; the shared run loop and the CDC hooks here
    supply the rest, so a connector's plugin carries no run-loop
    boilerplate.
    """

    logging_namespace = _COMMON_SQL_NAMESPACE

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

    def build_run(
        self,
        *,
        ctx: StreamSrcPluginCtx,
        ingester: CdcIngester,
        cfg: CdcRunConfig,
        stop_signal: StopSignal,
    ) -> CdcRun:
        return CdcRun(
            ctx=ctx,
            ingester=ingester,
            cfg=cfg,
            stop_signal=stop_signal,
            large_column_policy=LargeColumnPolicy(
                max_text_length=cfg.max_large_text_length,
            ),
        )

    def seed(self, run: CdcRun) -> None:
        if not _initial_load_pending(run.ctx, run.cfg):
            super().seed(run)
            return
        # A reload replaces the persisted offset, so the previous one is
        # seeded first: what it recorded about the source it was read
        # from outlives the reload. The load's own offset then becomes
        # the resume position, so the stream opens exactly where the
        # snapshot ends.
        super().seed(run)
        offset, run.pending_load = _load_initially(
            ctx=run.ctx,
            ingester=run.ingester,
            cfg=run.cfg,
            large_column_policy=run.large_column_policy,
        )
        run.ingester.seed_offset(offset)

    def slots(self, run: CdcRun) -> Sequence[str]:
        run.tracked_tables = run.ingester.harvest_tables(
            session=run.session,
            cdc_format=run.cfg.cdc_format,
        )
        return list(run.tracked_tables)

    def before_stream(self, run: CdcRun) -> bool:
        if run.pending_load is None:
            return True
        window = _read_window(
            ctx=run.ctx,
            ingester=run.ingester,
            session=run.session,
            tracked_tables=run.tracked_tables,
            cfg=run.cfg,
            large_column_policy=run.large_column_policy,
            window_end=run.pending_load.window_end,
            stop_signal=run.stop_signal,
        )
        if window is None:
            logger.info("cdc: interrupted while reading the initial load window.")
            return False
        _stage_initial_load(
            ctx=run.ctx,
            ingester=run.ingester,
            tracked_tables=run.tracked_tables,
            cfg=run.cfg,
            pending=run.pending_load,
            window=window,
        )
        return True

    def build_encoder(self, run: CdcRun) -> CdcEncoder:
        return CdcEncoder(run.cfg.cdc_format, run.tracked_tables)

    def records(self, run: CdcRun, polled: Iterable[CdcChange]) -> Iterable[CdcChange]:
        return run.large_column_policy.apply(polled)

    def stage_state(self, run: CdcRun, offset: CdcOffset) -> StreamStateSpec:
        state = super().stage_state(run, offset)
        if isinstance(state, dict):
            # The new state replaces the persisted one wholesale, so one carrying
            # only the offset would drop the record of an initial load already
            # done and make the next run redo it.
            done = (run.ctx.stream_state or {}).get(CDC_INITIAL_LOAD_STATE_KEY)
            if done is not None:
                state = {**state, CDC_INITIAL_LOAD_STATE_KEY: done}
        return state

    def may_checkpoint(self, run: CdcRun) -> bool:
        return super().may_checkpoint(run) and not _initial_load_pending(
            run.ctx, run.cfg
        )
