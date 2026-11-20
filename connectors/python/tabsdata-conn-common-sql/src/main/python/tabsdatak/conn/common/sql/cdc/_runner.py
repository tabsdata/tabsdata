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
``poll_changes`` / ``close_session``) and declares who keeps the offset
through its ``offsets`` keeper -- Tabsdata (``stream_state``) or the
source -- which the loop reaches through ``stage_state`` /
``resume_offset`` / ``acknowledge_offset``. The loop never reaches into
a particular replication protocol.
"""

from __future__ import annotations

import logging
import math
from abc import ABC, abstractmethod
from collections.abc import Iterable, Sequence
from dataclasses import dataclass, field

import polars as pl

from tabsdatak.api import StreamSrc

# noinspection PyProtectedMember
from tabsdatak.conn.common._schemas import KeptSchemas
from tabsdatak.conn.common._stream._config import StreamRunConfig

# noinspection PyProtectedMember
from tabsdatak.conn.common._stream._ledger import StageLedger

# noinspection PyProtectedMember
from tabsdatak.conn.common._stream._plugin import PipelineStreamSrcPlugin, StreamRun

# noinspection PyProtectedMember
from tabsdatak.conn.common._stream._poll import SourcePoll

# noinspection PyProtectedMember
from tabsdatak.conn.common._stream._sentinels import BufferSentinel, ChunkSentinel

# noinspection PyProtectedMember
from tabsdatak.conn.common._stream._signal import StopSignal
from tabsdatak.conn.common.sql.cdc._deduplicator import (
    deduplicate_initial_load_frames,
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
from tabsdatak.spi import (
    Conn,
    LazyFrameInput,
    StageFileStats,
    StreamSrcPluginCtx,
    StreamStateSpec,
)

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
    """A load sent but not published yet: its tables, per output slot, and
    where the changes committed while it ran end."""

    manifest: list[list[StageFileStats]]
    window_end: CdcOffset


def _load_initially(
    *,
    ctx: StreamSrcPluginCtx,
    ingester: CdcIngester,
    cfg: CdcRunConfig,
    large_column_policy: LargeColumnPolicy,
    schemas: KeptSchemas,
) -> tuple[CdcOffset, _PendingLoad | None]:
    """Snapshot the tracked tables, and stage them as a single batch when
    the snapshot is a point in time.

    Returns the offset change capture resumes from and, for a load whose
    session reports a window, the load left unstaged: the run loop reads
    the window's changes and stages both together
    (:func:`_stage_initial_load`). Either way the load is published by one
    ``ctx.stage`` call, so it is all-or-nothing: an error anywhere leaves no
    manifest and no state, what was sent is unstaged, and the next run
    redoes it.
    """
    session = ingester.open_initial_load()
    try:
        tracked_tables = ingester.collect_tables(
            session=session.engine,
            cdc_format=cfg.cdc_format,
        )
        ledger = StageLedger(ctx=ctx, slots=tracked_tables, schemas=schemas)
        chunk_sentinel = ChunkSentinel(
            ledger=ledger,
            max_rows=cfg.buffer.max_rows,
            max_bytes=cfg.buffer.max_bytes,
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
    _publish_load(ctx, ingester, offset, schemas)
    logger.info("Initial load staged")
    return offset, None


def _publish_load(
    ctx: StreamSrcPluginCtx,
    ingester: CdcIngester,
    offset: CdcOffset,
    schemas: KeptSchemas,
) -> None:
    state = ingester.stage_state(offset)
    if not isinstance(state, dict):
        raise SqlCdcErrorCode.CDC_21.exception(ingester=type(ingester).__name__)
    ctx.stage(None, schemas.carried({**state, CDC_INITIAL_LOAD_STATE_KEY: False}))
    ingester.acknowledge_offset(offset)


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
    schemas: KeptSchemas,
) -> list[list[StageFileStats]] | None:
    """The changes committed while the load read the tables, per output
    slot: read by the ingester's own poll, bounded at `window_end`, and sent
    as the stream sends them. ``None`` when a signal stops the read."""
    ledger = StageLedger(ctx=ctx, slots=tracked_tables, schemas=schemas)
    buffer_sentinel = BufferSentinel(
        ledger=ledger,
        encoder=CdcEncoder(cfg.cdc_format, tracked_tables),
        slots=tracked_tables,
        max_rows=cfg.buffer.max_rows,
        max_bytes=cfg.buffer.max_bytes,
        max_secs=NO_TIME_LIMIT,
        offset=ingester.base_offset(),
    )
    limit = cfg.initial_load_max_window_changes
    read = 0
    ingester.bound_stream(window_end)
    while not ingester.stream_bound_reached():
        if stop_signal.is_set():
            return None
        poll = SourcePoll(ingester.poll_changes(session))
        for change in large_column_policy.apply(poll):
            read += 1
            if read > limit:
                raise SqlCdcErrorCode.CDC_29.exception(limit=limit)
            buffer_sentinel.add(change)
        poll.log()
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
    window: list[list[StageFileStats]],
    schemas: KeptSchemas,
) -> None:
    """Publish the load with the window's changes that still apply to it, at
    the window's end, as one batch. The window is read back from the stage to
    deduplicate it: a table it drops nothing from keeps its window as sent,
    and for every other one the kept changes are sent and its window is
    unstaged."""
    registry = ingester.schema_registry
    reduced = deduplicate_initial_load_frames(
        loaded=[[ctx.scan(stats) for stats in files] for files in pending.manifest],
        changes=[[ctx.scan(stats) for stats in files] for files in window],
        tables=tracked_tables,
        schemas={table: registry.version(table, 0) for table in tracked_tables},
        cdc_format=cfg.cdc_format,
    )
    ledger = StageLedger(ctx=ctx, slots=tracked_tables, schemas=schemas)
    replaced: list[StageFileStats] = []
    for table, files in zip(tracked_tables, window, strict=True):
        deduplicated = reduced.get(table)
        if deduplicated is None:
            continue
        if deduplicated.dropped < sum(stats.rows for stats in files):
            ledger.stage(table, deduplicated.changes)
        replaced.extend(files)
    # Only once the kept changes are sent: they are read from these.
    ctx.unstage(replaced)
    # Where the window's read left the stream, not the window's end itself:
    # an end that is no resumable offset (a binlog position) bounds the read,
    # and the offset is what the read reached up to it.
    _publish_load(ctx, ingester, ingester.base_offset(), schemas)
    logger.info(
        "Initial load staged, with the changes of %d table(s) deduplicated",
        len(reduced),
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
            thresholds=self.thresholds(ctx, cfg),
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
            schemas=run.schemas,
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
            schemas=run.schemas,
        )
        if window is None:
            logger.info("Interrupted while reading the initial load window")
            return False
        _stage_initial_load(
            ctx=run.ctx,
            ingester=run.ingester,
            tracked_tables=run.tracked_tables,
            cfg=run.cfg,
            pending=run.pending_load,
            window=window,
            schemas=run.schemas,
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

    def empty(self, run: CdcRun, slot: str) -> LazyFrameInput:
        schema = run.ingester.schema_registry.latest(slot).pa_schema
        return LazyFrameInput(pl.DataFrame(schema.empty_table()).lazy())

    def may_checkpoint(self, run: CdcRun) -> bool:
        return super().may_checkpoint(run) and not _initial_load_pending(
            run.ctx, run.cfg
        )
