#
# Copyright 2026 Tabsdata Inc.
#

"""CDC-aware stream-stage publisher.

A CDC :class:`~tabsdatak.spi.StreamSrcPlugin` (the long-running ``run``
side) only *stages* change batches into Tabsdata's storage staging area,
recording each in a manifest. The paired hidden batch (publisher) source
ingests those staged files; by default that is the generic
:class:`~tabsdatak.spi.StreamStageSrcPlugin`, which passes the ingested
files through unchanged.

:class:`CdcStageSrcPlugin` is the CDC publisher. For every output slot it:

* concatenates the slot's staged tables into one frame, lazily, where they
  are staged;
* orders them by change segment
  (:data:`~tabsdatak.conn.common.sql.cdc.constants.CDC_META_SEG`) then
  ordinal (:data:`~tabsdatak.conn.common.sql.cdc.constants.CDC_META_ORD`),
  ``ord`` being the order of a change *within* its ``seg``;
* de-duplicates on that ``(seg, ord)`` pair;
* drops every row at or before the ``(seg, ord)`` published by the previous
  run -- a lexicographic compare, so the unpublished tail of a segment that
  was split across runs is still emitted (the cursor is tracked per slot in
  the per-flow attribute store); when no prior pair exists, no rows are
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
from collections.abc import Iterable
from decimal import Decimal
from typing import TYPE_CHECKING

from tabsdatak.conn.common.sql.cdc._initial_load import INITIAL_LOAD_SEGMENT
from tabsdatak.conn.common.sql.cdc.constants import CDC_META_ORD, CDC_META_SEG
from tabsdatak.conn.common.sql.cdc.error import SqlCdcErrorCode
from tabsdatak.spi import (
    FrameMetadata,
    LazyFrameInput,
    SrcDef,
    SrcPluginCtx,
    StreamStageSrc,
    StreamStageSrcPlugin,
    TableMode,
    _StreamStageSrcConn,
)

if TYPE_CHECKING:
    import polars as pl

logger = logging.getLogger(__name__)

# Per-slot keys under the per-flow attribute store holding the highest CDC
# ``(seg, ord)`` pair published so far. The pair is two scalar
# attributes (seg and ord stored separately, since the attribute store holds
# only scalars). Both are suffixed with the slot's positional index so a
# multi-table CDC flow tracks each output slot (table) independently.
_TABLE_PREFIX = "cdc-table"


def _seg_key(index: int) -> str:
    return f"{_TABLE_PREFIX}.{index}.seg"


def _ord_key(index: int) -> str:
    return f"{_TABLE_PREFIX}.{index}.ord"


def _key_order(frames: list[pl.LazyFrame]) -> list[int] | None:
    """The order in which to concatenate the staged files of one table so
    that the result is already free of duplicate keys and sorted by key,
    or ``None`` when no such order exists.

    Terms:

    * ``seg`` (segment): the committed transaction a CDC row belongs to,
      as a number that grows in commit order; ``-1`` for every row of an
      initial load.
    * ``ord`` (ordinal): the position of the row inside its segment.
    * key: the pair ``(seg, ord)``, compared segment first, then ordinal.

    The order exists when:

    * no key in any file is null;
    * within each file, every key is strictly greater than the one in the
      row before it, so no key repeats and none goes backwards;
    * ordered by their first key, each file starts after the previous file
      ends, so no two files share a range of keys;
    * at least one file has a row.

    Files without rows go last, as they add nothing. An initial load always
    qualifies: its segment is ``-1`` throughout and its ordinal counts up,
    file after file.

    Each file is read once, only its key columns, streamed into one row of
    facts: rows, nulls, ascending, first key and last key. The comparison
    with the previous row is added as a column before those facts are
    taken, on purpose: computed among them, polars holds the whole key
    columns in memory instead of streaming them.

    Returns:
        The positions of ``frames`` in that order, or ``None``.
    """
    import polars as pl

    segment, ordinal = pl.col(CDC_META_SEG), pl.col(CDC_META_ORD)
    ascending = (
        (segment > segment.shift())
        | ((segment == segment.shift()) & (ordinal > ordinal.shift()))
        | (segment.shift().is_null() & ordinal.shift().is_null())
    ).alias("ascending")
    spans: list[tuple[tuple, tuple, int]] = []
    empty: list[int] = []
    for position, frame in enumerate(frames):
        rows, ascends, nulls, first, last = (
            frame
            .select(segment, ordinal)
            .with_columns(ascending)
            .select(
                pl.len(),
                pl.col("ascending").all(),
                (segment.null_count() + ordinal.null_count()).alias("nulls"),
                pl.struct(segment, ordinal).first().alias("first"),
                pl.struct(segment, ordinal).last().alias("last"),
            )
            .collect(engine="streaming")
            .row(0)
        )
        if rows == 0:
            empty.append(position)
            continue
        if nulls or not ascends:
            return None
        spans.append((
            (first[CDC_META_SEG], first[CDC_META_ORD]),
            (last[CDC_META_SEG], last[CDC_META_ORD]),
            position,
        ))
    if not spans:
        return None
    spans.sort()
    for (_, previous_last, _), (next_first, _, _) in zip(spans, spans[1:]):
        if next_first <= previous_last:
            return None
    return [position for _, _, position in spans] + empty


def _known(table: LazyFrameInput) -> bool:
    return not isinstance(table.metadata, FrameMetadata) or table.metadata.known


class CdcStageSrc(StreamStageSrc):
    """Config type for the CDC stream-stage publisher binding."""


class CdcStageSrcPlugin(StreamStageSrcPlugin):
    """Stream-stage publisher that de-duplicates and offset-filters CDC rows.

    Overrides :meth:`process_files`; the manifest scan + ingestion handled
    by the base ``read_in`` is left untouched (see module docstring)."""

    def process_files(
        self,
        ctx: SrcPluginCtx,
        files: list[Iterable[LazyFrameInput]],
    ) -> list[Iterable[LazyFrameInput]]:
        """De-duplicate and ``(seg, ord)``-filter each slot's CDC rows.

        ``files`` is positional by output slot (the same shape the base
        ``read_in`` returns). Each slot's tables are concatenated, sorted
        and de-duplicated on ``(seg, ord)``, and filtered against the
        per-slot ``(seg, ord)`` cursor in ``ctx`` (none stored -> no
        filtering), all lazily, as one table; the new high-water
        ``(seg, ord)`` is persisted via ``ctx.set_attr``. A slot of an
        empty table of unknown columns passes through as it is. Returns
        the per-slot tables to publish (see module docstring for the full
        contract).

        Removing duplicates and sorting cost a pass that holds the whole
        batch's keys. When :func:`_key_order` finds the staged files
        already hold every key once and in order, as an initial load
        always does, both are skipped: the files are concatenated in key
        order, which is exactly what they would have produced."""
        import polars as pl

        results: list[Iterable[LazyFrameInput]] = []
        for index, slot in enumerate(files):
            tables = list(slot)
            if not tables:
                raise SqlCdcErrorCode.CDC_32.exception(index=index)
            if not any(_known(table) for table in tables):
                results.append(tables)
                continue

            lazy_frames = [table.frame for table in tables]
            try:
                order = _key_order(lazy_frames)
            except (pl.exceptions.PolarsError, OSError) as e:
                raise SqlCdcErrorCode.CDC_15.exception(
                    cause=e, index=index, reason=str(e)
                )
            if order is not None:
                # Fast path: no key repeats and the files, in this order,
                # already hold the rows sorted by key. Removing duplicates
                # would drop nothing and sorting would just place the files
                # one after another, so they are concatenated in that order.
                deduped = pl.concat(
                    [lazy_frames[position] for position in order],
                    how="vertical_relaxed",
                )
            else:
                # Stays lazy end to end: the dedup is a window aggregation
                # rather than ``unique(keep="first")`` so it does not depend
                # on engine row order (which the parallel/streaming engine
                # does not preserve), and the result is returned as a frame
                # the runtime streams to its landing, never materialised
                # whole.
                #
                # ``concat`` keeps file order, so a global row index over it
                # is a true arrival order; within each ``(seg, ord)`` group
                # the row with the minimum arrival is the earliest-staged
                # one. Selecting it by ``min().over`` (not ``keep="first"``)
                # makes the surviving duplicate deterministic regardless of
                # how the engine schedules the scan.
                deduped = (
                    pl
                    .concat(lazy_frames, how="vertical_relaxed")
                    .with_row_index("arrival")
                    .filter(
                        pl.col("arrival")
                        == pl.col("arrival").min().over([CDC_META_SEG, CDC_META_ORD])
                    )
                    .drop("arrival")
                    .sort([CDC_META_SEG, CDC_META_ORD])
                )

            # The cursor is stored as two scalar attributes (seg and
            # ord), each the ``str`` of a decimal128(38,0) value -- the
            # attribute store holds scalars, and ``str`` keeps the pair exact
            # across the full range. They are written together, so both are
            # present or both absent; restore to ``Decimal`` for comparison.
            prior_seg_str = ctx.get_attr(_seg_key(index))
            prior_ord_str = ctx.get_attr(_ord_key(index))
            prior: tuple[Decimal, Decimal] | None = None
            if prior_seg_str is not None and prior_ord_str is not None:
                prior = (Decimal(str(prior_seg_str)), Decimal(str(prior_ord_str)))
            if prior is not None:
                prior_seg, prior_ord = prior
                # ord is the order within seg, so a segment may be split
                # across runs: keep rows strictly after the published
                # ``(seg, ord)`` -- a lexicographic ``> (prior_seg,
                # prior_ord)``. Filtering on seg alone would wrongly drop
                # the unpublished tail of a partially published segment.
                #
                # Segment ``INITIAL_LOAD_SEGMENT`` is the exception: it is
                # reserved for the initial load and sits below every real
                # position, so the cursor never applies to it and a reload
                # is published even on a slot that already streamed.
                emitted = deduped.filter(
                    (pl.col(CDC_META_SEG) == INITIAL_LOAD_SEGMENT)
                    | (pl.col(CDC_META_SEG) > prior_seg)
                    | (
                        (pl.col(CDC_META_SEG) == prior_seg)
                        & (pl.col(CDC_META_ORD) > prior_ord)
                    )
                )
            else:
                emitted = deduped

            try:
                # The high-water mark: the largest key ``(seg, ord)`` in the
                # batch, which becomes the cursor. It is the largest ``ord``
                # among the rows with the largest ``seg``, computed rather
                # than read off the last sorted row, since the engine does
                # not promise to keep sort order. Streamed into one row.
                #
                # * Taken before the cursor filter, so a batch of rows
                #   already published never lowers the cursor.
                # * Taken over the staged rows themselves, not the
                #   deduplicated ones: removing duplicates keeps one row of
                #   every key, so the largest key is the same, without
                #   running the dedup and the sort a second time.
                high_water = (
                    pl
                    .concat(lazy_frames, how="vertical_relaxed")
                    .select(CDC_META_SEG, CDC_META_ORD)
                    .filter(pl.col(CDC_META_SEG) == pl.col(CDC_META_SEG).max())
                    .select(
                        pl.col(CDC_META_SEG).max().alias(CDC_META_SEG),
                        pl.col(CDC_META_ORD).max().alias(CDC_META_ORD),
                    )
                    .collect(engine="streaming")
                )
            except (pl.exceptions.PolarsError, OSError) as e:
                raise SqlCdcErrorCode.CDC_15.exception(
                    cause=e, index=index, reason=str(e)
                )

            # A batch whose staged files hold no row has no largest key:
            # the one row then holds nulls, and the cursor stays as it is.
            if high_water.height and None not in high_water.row(0):
                # seg/ord come from decimal128(38,0) columns, so ``row``
                # hands them back as ``Decimal``. ``max`` compares the
                # ``(seg, ord)`` tuples lexicographically (``prior`` was
                # restored to ``Decimal`` above, so the compare is exact).
                # Persist each as ``str`` -- the attribute store holds scalars
                # and ``str`` preserves the full decimal128(38,0) range.
                seg, ordinal = high_water.row(0)
                batch_max = (seg, ordinal)
                new_seg, new_ord = batch_max if prior is None else max(prior, batch_max)
                ctx.set_attr(_seg_key(index), str(new_seg))
                ctx.set_attr(_ord_key(index), str(new_ord))

            results.append([LazyFrameInput(emitted)])

        logger.info("cdc-stage: processed %d slot(s)", len(results))
        return results


# Entry-point binding for the CDC stream-stage publisher. Like the generic
# ``STREAM_STAGE_SRC`` the manifest decides the output-slot count at read
# time, so ``cardinality`` returns the ``-1`` sentinel (slot-count check
# disabled) and there is no explorer. ``ARRAY`` mode, each output slot
# carrying one table: the collapsed batch, or an empty one.
CDC_STAGE_SRC = SrcDef(
    conn=_StreamStageSrcConn,
    type_=".cdc-stage",
    system="Tabsdata",
    src_version="v1",
    src=CdcStageSrc,
    table_mode=TableMode.ARRAY,
    cardinality=lambda s: -1,
    plugin=CdcStageSrcPlugin,
    explorer=None,
    icon=None,
)
