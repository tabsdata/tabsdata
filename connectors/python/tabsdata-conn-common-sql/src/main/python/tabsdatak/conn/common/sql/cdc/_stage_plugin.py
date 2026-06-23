#
# Copyright 2026 Tabs Data Inc.
#

"""CDC-aware stream-stage publisher.

A CDC :class:`~tabsdatak.spi.StreamSrcPlugin` (the long-running ``run``
side) only *stages* change batches into Tabsdata's storage staging area,
recording each in a manifest. The paired hidden batch (publisher) source
ingests those staged files; by default that is the generic
:class:`~tabsdatak.spi.StreamStageSrcPlugin`, which passes the ingested
files through unchanged.

:class:`CdcStageSrcPlugin` is the CDC publisher. For every output slot it:

* concatenates the slot's staged parquet files into one frame;
* orders them by change segment
  (:data:`~tabsdatak.conn.common.sql.cdc.constants.CDC_META_SEG`) then
  ordinal (:data:`~tabsdatak.conn.common.sql.cdc.constants.CDC_META_ORD`),
  ``ord`` being the order of a change *within* its ``seg``;
* de-duplicates on that ``(seg, ord)`` pair;
* drops every row at or before the ``(seg, ord)`` published by the previous
  run -- a lexicographic compare, so the unpublished tail of a segment that
  was split across runs is still emitted (the cursor is tracked per slot in
  the per-flow scratch store); when no prior pair exists, no rows are
  dropped;
* persists the highest ``(seg, ord)`` seen so the next run resumes strictly
  after it.

All of the above is done per slot, keyed by the slot's positional index in
the outer list.

Bind it to a flow by pointing a CDC :class:`~tabsdatak.spi.StreamSrcDef`'s
``publisher_src`` at :class:`CdcStageSrc` (instead of the default
:class:`~tabsdatak.spi.StreamStageSrc`). The binding is registered via the
:data:`CDC_STAGE_SRC` entry point.
"""

from __future__ import annotations

import logging

from tabsdatak.conn.common.sql.cdc.constants import CDC_META_ORD, CDC_META_SEG
from tabsdatak.conn.common.sql.cdc.error import SqlCdcErrorCode
from tabsdatak.spi import (
    SrcDef,
    SrcPluginCtx,
    StreamStageSrc,
    StreamStageSrcPlugin,
    TableFileSpec,
    TableMode,
    _StreamStageSrcConn,
)

logger = logging.getLogger(__name__)

# Per-slot key under the per-flow scratch store holding the highest CDC
# ``(seg, ord)`` pair published so far. Suffixed with the slot's positional
# index so a multi-table CDC flow tracks each output slot (table)
# independently.
_TABLE_PREFIX = "cdc-table"


def _table_key(index: int) -> str:
    return f"{_TABLE_PREFIX}.{index}"


class CdcStageSrc(StreamStageSrc):
    """Config type for the CDC stream-stage publisher binding."""


class CdcStageSrcPlugin(StreamStageSrcPlugin):
    """Stream-stage publisher that de-duplicates and offset-filters CDC rows.

    Overrides :meth:`process_files`; the manifest scan + ingestion handled
    by the base ``read_in`` is left untouched (see module docstring)."""

    def process_files(
        self,
        ctx: SrcPluginCtx,
        files: list[list[TableFileSpec]],
    ) -> list[list[TableFileSpec]]:
        """De-duplicate and ``(seg, ord)``-filter each slot's CDC rows.

        ``files`` is positional by output slot (the same shape the base
        ``read_in`` returns). Each slot's parquet files are concatenated,
        sorted and de-duplicated on ``(seg, ord)``, filtered against the
        per-slot ``(seg, ord)`` cursor in ``ctx`` (none stored -> no
        filtering), and written to a single parquet under ``ctx.work_dir``;
        the new high-water ``(seg, ord)`` is persisted via
        ``ctx.store_attr``. An empty input slot yields an empty output
        slot. Returns the per-slot file lists to publish (see module
        docstring for the full contract)."""
        import polars as pl

        out_dir = ctx.work_dir / "_cdc_stage"
        out_dir.mkdir(parents=True, exist_ok=True)

        results: list[list[TableFileSpec]] = []
        for index, slot_files in enumerate(files):
            if not slot_files:
                # No files staged for this slot this run; nothing to emit.
                results.append([])
                continue

            lazy_frames = [pl.scan_parquet(path) for path in slot_files]
            # Stays lazy end to end: the dedup is a window aggregation rather
            # than ``unique(keep="first")`` so it does not depend on engine
            # row order (which the parallel/streaming engine does not
            # preserve), and the result is streamed to disk by
            # ``sink_parquet`` below, never materialised whole.
            #
            # ``concat`` keeps file order, so a global row index over it is a
            # true arrival order; within each ``(seg, ord)`` group the row
            # with the minimum arrival is the earliest-staged one. Selecting
            # it by ``min().over`` (not ``keep="first"``) makes the surviving
            # duplicate deterministic regardless of how the engine schedules
            # the scan.
            deduped = (
                pl.concat(lazy_frames, how="vertical_relaxed")
                .with_row_index("arrival")
                .filter(
                    pl.col("arrival")
                    == pl.col("arrival").min().over([CDC_META_SEG, CDC_META_ORD])
                )
                .drop("arrival")
                .sort([CDC_META_SEG, CDC_META_ORD])
            )

            prior = ctx.get_attr(_table_key(index))
            if prior is not None:
                prior_seg, prior_ord = prior
                # ord is the order within seg, so a segment may be split
                # across runs: keep rows strictly after the published
                # ``(seg, ord)`` -- a lexicographic ``> (prior_seg,
                # prior_ord)``. Filtering on seg alone would wrongly drop
                # the unpublished tail of a partially published segment.
                emitted = deduped.filter(
                    (pl.col(CDC_META_SEG) > prior_seg)
                    | (
                        (pl.col(CDC_META_SEG) == prior_seg)
                        & (pl.col(CDC_META_ORD) > prior_ord)
                    )
                )
            else:
                emitted = deduped

            out_path = out_dir / f"slot{index:04d}.parquet"
            try:
                # High-water ``(seg, ord)`` over the de-duplicated batch.
                # ``ord`` is scoped within ``seg``, so the cursor is the
                # largest ``ord`` among the rows at the largest ``seg`` --
                # computed explicitly rather than read positionally off the
                # sorted frame, which the engine does not guarantee to leave
                # in sort order. Streamed into a one-row frame; the batch
                # itself is never collected. Taken from the pre-filter frame
                # so an already-seen batch can never lower the stored cursor.
                high_water = (
                    deduped.filter(
                        pl.col(CDC_META_SEG) == pl.col(CDC_META_SEG).max()
                    )
                    .select(
                        pl.col(CDC_META_SEG).max().alias(CDC_META_SEG),
                        pl.col(CDC_META_ORD).max().alias(CDC_META_ORD),
                    )
                    .collect(engine="streaming")
                )
                emitted.sink_parquet(out_path)
            except (pl.exceptions.PolarsError, OSError) as e:
                raise SqlCdcErrorCode.CDC_15.exception(
                    cause=e, index=index, reason=str(e)
                )

            if high_water.height:
                # seg/ord are decimal128, so ``row`` hands them back as
                # ``Decimal`` -- stored as-is (the scratch store accepts
                # ``Decimal``) to keep the cursor exact across the full
                # decimal128(38, 0) range. ``prior`` was stored the same way,
                # so ``max`` compares two ``[Decimal, Decimal]`` pairs.
                seg, ordinal = high_water.row(0)
                batch_max = [seg, ordinal]
                new_cursor = batch_max if prior is None else max(prior, batch_max)
                ctx.store_attr(_table_key(index), new_cursor)

            results.append([out_path])

        logger.info("cdc-stage: processed %d slot(s)", len(results))
        return results


# Entry-point binding for the CDC stream-stage publisher. Like the generic
# ``STREAM_STAGE_SRC`` the manifest decides the output-slot count at read
# time, so ``cardinality`` returns the ``-1`` sentinel (slot-count check
# disabled) and there is no explorer. ``ARRAY`` mode because an output slot
# may carry zero files (fully filtered) or one (the collapsed batch).
CDC_STAGE_SRC = SrcDef(
    conn=_StreamStageSrcConn,
    src_type="cdcStage",
    src_version="v1",
    src=CdcStageSrc,
    table_mode=TableMode.ARRAY,
    cardinality=lambda s: -1,
    plugin=CdcStageSrcPlugin,
    explorer=None,
)
