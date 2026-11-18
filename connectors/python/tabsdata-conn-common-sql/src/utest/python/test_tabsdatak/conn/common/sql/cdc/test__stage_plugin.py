#
#  Copyright 2026 Tabsdata Inc.
#

"""Unit tests for
:class:`~tabsdatak.conn.common.sql.cdc._stage_plugin.CdcStageSrcPlugin`
covering, per output slot: concat + sort + de-dup on ``(seg, ord)``,
``(seg, ord)`` cursor filtering (none vs set, including resuming inside a
segment split across runs), the monotonic high-water store, empty-slot
handling, and per-index independence. ``ord`` is the order of a change
within its ``seg``, so the cursor is the full ``(seg, ord)`` pair. The
``SrcPluginCtx`` surface the plugin touches (``work_dir`` / ``get_attr`` /
``set_attr``) is faked; no transporter or manifest is involved.

The cursor is persisted as two scalar attributes -- ``seg`` and
``ord`` stored separately, each the ``str`` of the decimal128(38, 0)
value -- since the attribute store holds only scalars."""

from __future__ import annotations

from decimal import Decimal
from pathlib import Path

import polars as pl
import pytest

from tabsdatak.conn.common.sql.cdc._stage_plugin import (
    CDC_STAGE_SRC,
    CdcStageSrc,
    CdcStageSrcPlugin,
    _key_order,
    _ord_key,
    _seg_key,
)
from tabsdatak.conn.common.sql.cdc.constants import CDC_META_ORD, CDC_META_SEG
from tabsdatak.conn.common.sql.cdc.error import SqlCdcException
from tabsdatak.spi import FrameMetadata, LazyFrameInput


class FakeCtx:
    """Minimal stand-in for the ``SrcPluginCtx`` surface ``process_files``
    consumes: a working directory (``work_dir``) plus the per-flow attribute store."""

    def __init__(self, work_dir: Path, attrs: dict | None = None):
        self._work_dir = work_dir
        self.attrs: dict = dict(attrs or {})

    @property
    def work_dir(self) -> Path:
        return self._work_dir

    def get_attr(self, attr):
        return self.attrs.get(attr)

    def set_attr(self, attr, val):
        self.attrs[attr] = val


def _write(path: Path, rows: list[tuple[int, int, str]]) -> LazyFrameInput:
    """Write a tiny parquet with (seg, ord, payload) rows.

    seg/ord are cast to ``decimal128(38, 0)`` -- the dtype the CDC ingester
    actually emits, and the only dtype the plugin sees in production. Returns
    the :class:`LazyFrameInput` over it that ``process_files`` consumes, as the
    stage hands it a staged table."""
    frame = pl.DataFrame({
        CDC_META_SEG: [r[0] for r in rows],
        CDC_META_ORD: [r[1] for r in rows],
        "payload": [r[2] for r in rows],
    }).with_columns(
        pl.col(CDC_META_SEG).cast(pl.Decimal(38, 0)),
        pl.col(CDC_META_ORD).cast(pl.Decimal(38, 0)),
    )
    frame.write_parquet(path)
    return LazyFrameInput(pl.scan_parquet(path))


def _read(table: LazyFrameInput) -> list[tuple[int, int]]:
    df = table.frame.collect()
    return list(zip(df.get_column(CDC_META_SEG), df.get_column(CDC_META_ORD)))


def _cursor(ctx: FakeCtx, index: int = 0) -> tuple:
    """The stored ``(seg, ord)`` cursor for ``index`` -- two scalar ``str``
    attributes, or ``(None, None)`` when nothing was stored."""
    return (ctx.get_attr(_seg_key(index)), ctx.get_attr(_ord_key(index)))


def _seed(seg: str, ordinal: str, index: int = 0) -> dict:
    """Seed a FakeCtx attribute store with a prior ``(seg, ord)`` cursor."""
    return {_seg_key(index): seg, _ord_key(index): ordinal}


def test_no_prior_cursor_sorts_dedups_and_stores_max(tmp_path):
    f1 = _write(tmp_path / "a.parquet", [(3, 1, "c"), (1, 1, "a")])
    f2 = _write(tmp_path / "b.parquet", [(2, 1, "b"), (1, 1, "a-dup")])
    ctx = FakeCtx(tmp_path / "work")

    out = CdcStageSrcPlugin().process_files(ctx, [[f1, f2]])

    assert len(out) == 1 and len(out[0]) == 1
    # Sorted by (seg, ord); the (1,1) duplicate collapsed to one row.
    assert _read(out[0][0]) == [(1, 1), (2, 1), (3, 1)]
    # No prior cursor -> nothing filtered, high-water (seg, ord) stored.
    assert _cursor(ctx) == ("3", "1")


def test_prior_cursor_filters_at_or_before_and_advances(tmp_path):
    f = _write(
        tmp_path / "a.parquet",
        [(1, 1, "a"), (2, 1, "b"), (3, 1, "c"), (4, 1, "d")],
    )
    ctx = FakeCtx(tmp_path / "work", _seed("2", "1"))

    out = CdcStageSrcPlugin().process_files(ctx, [[f]])

    # (seg, ord) <= (2, 1) dropped; only 3 and 4 survive.
    assert _read(out[0][0]) == [(3, 1), (4, 1)]
    assert _cursor(ctx) == ("4", "1")


def test_resumes_inside_a_segment_split_across_runs(tmp_path):
    # Prior run published seg 5 through ord 2. This batch re-stages seg 5's
    # ords 1-2 and adds the unpublished tail (5, 3) plus a new seg 6.
    f = _write(
        tmp_path / "a.parquet",
        [(5, 1, "a"), (5, 2, "b"), (5, 3, "c"), (6, 1, "d")],
    )
    ctx = FakeCtx(tmp_path / "work", _seed("5", "2"))

    out = CdcStageSrcPlugin().process_files(ctx, [[f]])

    # Lexicographic (seg, ord) > (5, 2): (5,1)/(5,2) drop, (5,3) survives.
    # A seg-only `seg > 5` filter would wrongly drop (5, 3) here.
    assert _read(out[0][0]) == [(5, 3), (6, 1)]
    assert _cursor(ctx) == ("6", "1")


def test_ord_breaks_segment_ties_and_dedup_is_on_pair(tmp_path):
    # Same segment, different ordinal -> distinct rows, ordered by ordinal.
    f = _write(
        tmp_path / "a.parquet",
        [(5, 2, "second"), (5, 1, "first"), (5, 1, "first-dup")],
    )
    ctx = FakeCtx(tmp_path / "work")

    out = CdcStageSrcPlugin().process_files(ctx, [[f]])

    assert _read(out[0][0]) == [(5, 1), (5, 2)]
    # Among the two (5, 1) duplicates the earliest-arriving row is kept:
    # the stable sort + unique(keep="first") retains "first", not "first-dup".
    df = out[0][0].frame.collect()
    kept = df.filter(
        (pl.col(CDC_META_SEG) == 5) & (pl.col(CDC_META_ORD) == 1)
    ).get_column("payload")
    assert kept.to_list() == ["first"]
    assert _cursor(ctx) == ("5", "2")


def test_dedup_and_cursor_filter_combine_in_one_run(tmp_path):
    # A duplicate (3,1) split across two files, with a prior cursor at (2,1):
    # dedup-collapse, keep-first, and the cursor filter all apply together.
    f1 = _write(tmp_path / "a.parquet", [(1, 1, "a"), (3, 1, "c"), (4, 1, "d")])
    f2 = _write(tmp_path / "b.parquet", [(2, 1, "b"), (3, 1, "c-dup")])
    ctx = FakeCtx(tmp_path / "work", _seed("2", "1"))

    out = CdcStageSrcPlugin().process_files(ctx, [[f1, f2]])

    # (1,1)/(2,1) <= (2,1) filtered; the (3,1) duplicate collapsed; (4,1) new.
    assert _read(out[0][0]) == [(3, 1), (4, 1)]
    # The earliest-arriving (3,1) survives the dedup ("c", not "c-dup").
    df = out[0][0].frame.collect()
    kept = df.filter(
        (pl.col(CDC_META_SEG) == 3) & (pl.col(CDC_META_ORD) == 1)
    ).get_column("payload")
    assert kept.to_list() == ["c"]
    # High-water is the deduped batch max (4, 1), not lowered by the filter.
    assert _cursor(ctx) == ("4", "1")


def test_cursor_stored_as_scalar_strings(tmp_path):
    # seg/ord are decimal128(38, 0); the cursor is persisted as two scalar
    # ``str`` attributes and the decimal dtype survives the parquet
    # round trip of the staged rows themselves.
    f1 = _write(tmp_path / "a.parquet", [(1, 1, "a"), (3, 1, "c"), (4, 1, "d")])
    f2 = _write(tmp_path / "b.parquet", [(2, 1, "b"), (3, 1, "c-dup")])
    ctx = FakeCtx(tmp_path / "work", _seed("2", "1"))

    out = CdcStageSrcPlugin().process_files(ctx, [[f1, f2]])

    assert _read(out[0][0]) == [(3, 1), (4, 1)]
    df = out[0][0].frame.collect()
    assert df.schema[CDC_META_SEG] == pl.Decimal(38, 0)
    cursor = _cursor(ctx)
    assert cursor == ("4", "1")
    assert all(isinstance(value, str) for value in cursor)


def test_cursor_preserves_value_beyond_int64(tmp_path):
    # A seg past 2**63 - 1: storing the cursor as a ``str`` keeps the full
    # decimal128(38, 0) range exact, with no int/float down-cast ceiling.
    big_seg = 2**63 + 7
    f = _write(tmp_path / "a.parquet", [(big_seg, 1, "x")])
    ctx = FakeCtx(tmp_path / "work")

    CdcStageSrcPlugin().process_files(ctx, [[f]])

    assert _cursor(ctx) == (str(big_seg), "1")


def test_a_slot_without_tables_is_an_error(tmp_path):
    with pytest.raises(SqlCdcException, match="CDC_32"):
        CdcStageSrcPlugin().process_files(FakeCtx(tmp_path / "work"), [[]])


def test_a_slot_of_an_unknown_table_passes_through_and_no_state(tmp_path):
    ctx = FakeCtx(tmp_path / "work")
    unknown = LazyFrameInput(pl.LazyFrame(), FrameMetadata(known=False))

    [[table]] = CdcStageSrcPlugin().process_files(ctx, [[unknown]])

    assert table is unknown
    assert _cursor(ctx) == (None, None)


def test_a_slot_of_an_empty_staged_table_yields_it_typed_and_no_state(tmp_path):
    ctx = FakeCtx(tmp_path / "work")
    empty = _write(tmp_path / "a.parquet", [])

    [[table]] = CdcStageSrcPlugin().process_files(ctx, [[empty]])

    assert table.frame.collect().height == 0
    assert table.frame.collect_schema() == empty.frame.collect_schema()
    assert table.metadata is None
    assert _cursor(ctx) == (None, None)


def test_stale_batch_does_not_lower_cursor(tmp_path):
    # Every row already seen (<= prior (10, 1)): output empty, cursor unchanged.
    f = _write(tmp_path / "a.parquet", [(7, 1, "a"), (9, 1, "b")])
    ctx = FakeCtx(tmp_path / "work", _seed("10", "1"))

    out = CdcStageSrcPlugin().process_files(ctx, [[f]])

    assert _read(out[0][0]) == []
    assert _cursor(ctx) == ("10", "1")


def test_slots_tracked_independently_by_index(tmp_path):
    f0 = _write(tmp_path / "s0.parquet", [(1, 1, "x"), (2, 1, "y")])
    f1 = _write(tmp_path / "s1.parquet", [(10, 1, "p"), (20, 1, "q")])
    ctx = FakeCtx(tmp_path / "work", _seed("10", "1", index=1))

    out = CdcStageSrcPlugin().process_files(ctx, [[f0], [f1]])

    assert _read(out[0][0]) == [(1, 1), (2, 1)]  # slot 0: no prior
    assert _read(out[1][0]) == [(20, 1)]  # slot 1: prior (10,1) filters <= it
    assert _cursor(ctx, 0) == ("2", "1")
    assert _cursor(ctx, 1) == ("20", "1")


def test_initial_load_rows_publish_despite_a_prior_cursor(tmp_path):
    """Segment -1 is reserved for the initial load and cannot be a real
    position, so a reload publishes in full on a slot that already streamed;
    the stale change at segment 400 is still filtered."""
    files = [
        _write(
            tmp_path / "a.parquet",
            [
                (-1, 0, "load"),
                (-1, 1, "row-1"),
                (-1, 2, "row-2"),
                (400, 1, "already-published"),
                (600, 3, "new"),
            ],
        )
    ]
    ctx = FakeCtx(tmp_path, _seed("500", "9"))

    out = CdcStageSrcPlugin().process_files(ctx, [files])

    assert _read(out[0][0]) == [(-1, 0), (-1, 1), (-1, 2), (600, 3)]
    assert _cursor(ctx) == ("600", "3")


def test_initial_load_only_batch_advances_the_cursor(tmp_path):
    """A load with no changes after it still leaves a usable cursor, so the
    next run filters from the load rather than from nothing."""
    files = [_write(tmp_path / "a.parquet", [(-1, 0, "load"), (-1, 1, "row-1")])]
    ctx = FakeCtx(tmp_path)

    out = CdcStageSrcPlugin().process_files(ctx, [files])

    assert _read(out[0][0]) == [(-1, 0), (-1, 1)]
    assert _cursor(ctx) == ("-1", "1")


def test_changes_after_an_initial_load_filter_normally(tmp_path):
    """Once the load has been published, ordinary changes above the load segment
    obey the cursor as they always did."""
    ctx = FakeCtx(tmp_path, _seed("-1", "1"))
    files = [_write(tmp_path / "b.parquet", [(-1, 1, "replayed"), (7, 1, "new")])]

    out = CdcStageSrcPlugin().process_files(ctx, [files])

    # The load row republishes (a reload is always emitted), the real
    # change publishes because it is above the cursor.
    assert _read(out[0][0]) == [(-1, 1), (7, 1)]
    assert _cursor(ctx) == ("7", "1")


def test_entry_point_binding_shape():
    assert CDC_STAGE_SRC.type_ == ".cdc-stage"
    assert CDC_STAGE_SRC.src is CdcStageSrc
    assert CDC_STAGE_SRC.plugin is CdcStageSrcPlugin
    assert CDC_STAGE_SRC.explorer is None
    # Manifest decides slot count -> sentinel disables the framework check
    # (the lambda ignores its argument).
    assert CDC_STAGE_SRC.cardinality(None) == -1
    # CdcStageSrc must be a StreamStageSrc so the SrcDef validator accepts
    # the inherited read_in signature.
    from tabsdatak.spi import StreamStageSrc

    assert issubclass(CdcStageSrc, StreamStageSrc)


# The fast path: when the staged files already hold every key once and in
# key order, ``process_files`` concatenates them instead of removing
# duplicates and sorting. Terms, as in ``_key_order``: ``seg`` is the
# committed transaction a row belongs to (``-1`` for an initial load),
# ``ord`` its position inside that transaction, and the key is the pair
# ``(seg, ord)``, compared segment first.


def _reference(frames: list[pl.LazyFrame], prior: tuple | None) -> pl.DataFrame:
    """What ``process_files`` produced before the fast path: duplicates
    removed keeping the earliest-staged row, sorted by key, then filtered by
    the cursor."""
    rows = (
        pl
        .concat(frames, how="vertical_relaxed")
        .with_row_index("arrival")
        .filter(
            pl.col("arrival")
            == pl.col("arrival").min().over([CDC_META_SEG, CDC_META_ORD])
        )
        .drop("arrival")
        .sort([CDC_META_SEG, CDC_META_ORD])
    )
    if prior is not None:
        seg, ordinal = prior
        rows = rows.filter(
            (pl.col(CDC_META_SEG) == -1)
            | (pl.col(CDC_META_SEG) > seg)
            | ((pl.col(CDC_META_SEG) == seg) & (pl.col(CDC_META_ORD) > ordinal))
        )
    return rows.collect()


SCENARIOS = {
    "initial load in order": (
        [[(-1, 0, "load"), (-1, 1, "a")], [(-1, 2, "b"), (-1, 3, "c")]],
        [0, 1],
    ),
    "files staged out of key order": (
        [[(7, 0, "c"), (7, 1, "d")], [(5, 0, "a"), (6, 3, "b")]],
        [1, 0],
    ),
    "a load then its changes": (
        [[(-1, 0, "load"), (-1, 1, "a")], [(40, 0, "x"), (41, 2, "y")]],
        [0, 1],
    ),
    "an empty file among them": (
        [[(1, 0, "a")], [], [(2, 0, "b")]],
        [0, 2, 1],
    ),
    "a key repeated across files": (
        [[(1, 0, "a"), (2, 0, "b")], [(2, 0, "b-dup"), (3, 0, "c")]],
        None,
    ),
    "a key repeated in a file": (
        [[(1, 0, "a"), (1, 0, "a-dup"), (2, 0, "b")]],
        None,
    ),
    "a key going backwards in a file": (
        [[(1, 0, "a"), (3, 0, "c"), (2, 0, "b")]],
        None,
    ),
    "ordinals going backwards in a segment": (
        [[(5, 2, "b"), (5, 1, "a")]],
        None,
    ),
    "overlapping files without a shared key": (
        [[(1, 0, "a"), (3, 0, "c")], [(2, 0, "b"), (4, 0, "d")]],
        None,
    ),
    "only empty files": ([[], []], None),
}


def _staged(tmp_path: Path, files: list[list[tuple]]) -> list[LazyFrameInput]:
    return [
        _write(tmp_path / f"{position}.parquet", rows)
        for position, rows in enumerate(files)
    ]


@pytest.mark.parametrize("name", SCENARIOS)
def test_key_order_finds_the_order_only_when_no_key_repeats_or_overlaps(tmp_path, name):
    files, expected = SCENARIOS[name]
    tables = _staged(tmp_path, files)
    assert _key_order([table.frame for table in tables]) == expected


def test_key_order_refuses_a_null_key(tmp_path):
    path = tmp_path / "null.parquet"
    pl.DataFrame({
        CDC_META_SEG: [1, None],
        CDC_META_ORD: [0, 1],
        "payload": ["a", "b"],
    }).with_columns(
        pl.col(CDC_META_SEG).cast(pl.Decimal(38, 0)),
        pl.col(CDC_META_ORD).cast(pl.Decimal(38, 0)),
    ).write_parquet(path)
    assert _key_order([pl.scan_parquet(path)]) is None


def test_key_order_sees_a_repeat_at_a_row_group_boundary(tmp_path):
    # The comparison with the previous row must hold across the chunks the
    # file is streamed in, not only inside each.
    rows = 250_000
    keys = pl.int_range(0, rows, eager=True)
    repeated = keys.scatter(100_000, 99_999)
    path = tmp_path / "boundary.parquet"
    pl.DataFrame({
        CDC_META_SEG: pl.repeat(-1, rows, eager=True),
        CDC_META_ORD: repeated,
    }).with_columns(
        pl.col(CDC_META_SEG).cast(pl.Decimal(38, 0)),
        pl.col(CDC_META_ORD).cast(pl.Decimal(38, 0)),
    ).write_parquet(path, row_group_size=100_000)
    assert _key_order([pl.scan_parquet(path)]) is None


@pytest.mark.parametrize("prior", [None, ("6", "3"), ("-1", "1")])
@pytest.mark.parametrize("name", SCENARIOS)
def test_rows_cursor_and_order_match_the_dedup_and_sort(tmp_path, name, prior):
    files, _ = SCENARIOS[name]
    tables = _staged(tmp_path, files)
    seeded = {} if prior is None else _seed(*prior)
    ctx = FakeCtx(tmp_path / "work", seeded)

    out = CdcStageSrcPlugin().process_files(ctx, [tables])

    expected = _reference(
        [table.frame for table in tables],
        None if prior is None else tuple(Decimal(value) for value in prior),
    )
    assert out[0][0].frame.collect().equals(expected)
    reference = FakeCtx(tmp_path / "reference", seeded)
    _reference_cursor(reference, [table.frame for table in tables], prior)
    assert _cursor(ctx) == _cursor(reference)


def _reference_cursor(ctx: FakeCtx, frames: list[pl.LazyFrame], prior) -> None:
    """The cursor ``process_files`` stores: the largest key of the
    deduplicated rows, never below the prior cursor, and left as it is when
    the batch has no key."""
    high_water = (
        _reference(frames, None)
        .filter(pl.col(CDC_META_SEG) == pl.col(CDC_META_SEG).max())
        .select(pl.col(CDC_META_SEG).max(), pl.col(CDC_META_ORD).max())
    )
    if not high_water.height or None in high_water.row(0):
        return
    batch = high_water.row(0)
    if prior is not None:
        batch = max(tuple(Decimal(value) for value in prior), batch)
    ctx.set_attr(_seg_key(0), str(batch[0]))
    ctx.set_attr(_ord_key(0), str(batch[1]))


def test_a_batch_of_empty_files_leaves_the_cursor_as_it_is(tmp_path):
    # Its staged files hold no row, so it has no largest key: no cursor is
    # stored where there was none, and a prior one is kept, not compared
    # with a missing key.
    tables = _staged(tmp_path, [[], []])

    fresh = FakeCtx(tmp_path / "fresh")
    out = CdcStageSrcPlugin().process_files(fresh, [tables])
    assert _read(out[0][0]) == []
    assert _cursor(fresh) == (None, None)

    resumed = FakeCtx(tmp_path / "resumed", _seed("6", "3"))
    out = CdcStageSrcPlugin().process_files(resumed, [tables])
    assert _read(out[0][0]) == []
    assert _cursor(resumed) == ("6", "3")
