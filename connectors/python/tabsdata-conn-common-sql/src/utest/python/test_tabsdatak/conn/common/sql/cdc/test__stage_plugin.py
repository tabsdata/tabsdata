#
#  Copyright 2026 Tabs Data Inc.
#

"""Unit tests for
:class:`~tabsdatak.conn.common.sql.cdc._stage_plugin.CdcStageSrcPlugin`
covering, per output slot: concat + sort + de-dup on ``(seg, ord)``,
``(seg, ord)`` cursor filtering (none vs set, including resuming inside a
segment split across runs), the monotonic high-water store, empty-slot
handling, and per-index independence. ``ord`` is the order of a change
within its ``seg``, so the cursor is the full ``(seg, ord)`` pair. The
``SrcPluginCtx`` surface the plugin touches (``work_dir`` / ``get_attr`` /
``store_attr``) is faked; no transporter or manifest is involved."""

from __future__ import annotations

from pathlib import Path

import polars as pl

from tabsdatak.conn.common.sql.cdc._stage_plugin import (
    CDC_STAGE_SRC,
    CdcStageSrc,
    CdcStageSrcPlugin,
    _table_key,
)
from tabsdatak.conn.common.sql.cdc.constants import CDC_META_ORD, CDC_META_SEG


class FakeCtx:
    """Minimal stand-in for the ``SrcPluginCtx`` surface ``process_files``
    consumes: a scratch ``work_dir`` plus the per-flow attr store."""

    def __init__(self, work_dir: Path, attrs: dict | None = None):
        self._work_dir = work_dir
        self.attrs: dict = dict(attrs or {})

    @property
    def work_dir(self) -> Path:
        return self._work_dir

    def get_attr(self, attr):
        return self.attrs.get(attr)

    def store_attr(self, attr, val):
        self.attrs[attr] = val


def _write(path: Path, rows: list[tuple[int, int, str]]) -> Path:
    """Write a tiny parquet with (seg, ord, payload) rows."""
    pl.DataFrame({
        CDC_META_SEG: [r[0] for r in rows],
        CDC_META_ORD: [r[1] for r in rows],
        "payload": [r[2] for r in rows],
    }).write_parquet(path)
    return path


def _read(path: Path) -> list[tuple[int, int]]:
    df = pl.read_parquet(path)
    return list(zip(df.get_column(CDC_META_SEG), df.get_column(CDC_META_ORD)))


def test_no_prior_cursor_sorts_dedups_and_stores_max(tmp_path):
    f1 = _write(tmp_path / "a.parquet", [(3, 1, "c"), (1, 1, "a")])
    f2 = _write(tmp_path / "b.parquet", [(2, 1, "b"), (1, 1, "a-dup")])
    ctx = FakeCtx(tmp_path / "work")

    out = CdcStageSrcPlugin().process_files(ctx, [[f1, f2]])

    assert len(out) == 1 and len(out[0]) == 1
    # Sorted by (seg, ord); the (1,1) duplicate collapsed to one row.
    assert _read(out[0][0]) == [(1, 1), (2, 1), (3, 1)]
    # No prior cursor -> nothing filtered, high-water (seg, ord) stored.
    assert ctx.get_attr(_table_key(0)) == [3, 1]


def test_prior_cursor_filters_at_or_before_and_advances(tmp_path):
    f = _write(
        tmp_path / "a.parquet",
        [(1, 1, "a"), (2, 1, "b"), (3, 1, "c"), (4, 1, "d")],
    )
    ctx = FakeCtx(tmp_path / "work", {_table_key(0): [2, 1]})

    out = CdcStageSrcPlugin().process_files(ctx, [[f]])

    # (seg, ord) <= (2, 1) dropped; only 3 and 4 survive.
    assert _read(out[0][0]) == [(3, 1), (4, 1)]
    assert ctx.get_attr(_table_key(0)) == [4, 1]


def test_resumes_inside_a_segment_split_across_runs(tmp_path):
    # Prior run published seg 5 through ord 2. This batch re-stages seg 5's
    # ords 1-2 and adds the unpublished tail (5, 3) plus a new seg 6.
    f = _write(
        tmp_path / "a.parquet",
        [(5, 1, "a"), (5, 2, "b"), (5, 3, "c"), (6, 1, "d")],
    )
    ctx = FakeCtx(tmp_path / "work", {_table_key(0): [5, 2]})

    out = CdcStageSrcPlugin().process_files(ctx, [[f]])

    # Lexicographic (seg, ord) > (5, 2): (5,1)/(5,2) drop, (5,3) survives.
    # A seg-only `seg > 5` filter would wrongly drop (5, 3) here.
    assert _read(out[0][0]) == [(5, 3), (6, 1)]
    assert ctx.get_attr(_table_key(0)) == [6, 1]


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
    df = pl.read_parquet(out[0][0])
    kept = df.filter(
        (pl.col(CDC_META_SEG) == 5) & (pl.col(CDC_META_ORD) == 1)
    ).get_column("payload")
    assert kept.to_list() == ["first"]
    assert ctx.get_attr(_table_key(0)) == [5, 2]


def test_dedup_and_cursor_filter_combine_in_one_run(tmp_path):
    # A duplicate (3,1) split across two files, with a prior cursor at (2,1):
    # dedup-collapse, keep-first, and the cursor filter all apply together.
    f1 = _write(tmp_path / "a.parquet", [(1, 1, "a"), (3, 1, "c"), (4, 1, "d")])
    f2 = _write(tmp_path / "b.parquet", [(2, 1, "b"), (3, 1, "c-dup")])
    ctx = FakeCtx(tmp_path / "work", {_table_key(0): [2, 1]})

    out = CdcStageSrcPlugin().process_files(ctx, [[f1, f2]])

    # (1,1)/(2,1) <= (2,1) filtered; the (3,1) duplicate collapsed; (4,1) new.
    assert _read(out[0][0]) == [(3, 1), (4, 1)]
    # The earliest-arriving (3,1) survives the dedup ("c", not "c-dup").
    df = pl.read_parquet(out[0][0])
    kept = df.filter(
        (pl.col(CDC_META_SEG) == 3) & (pl.col(CDC_META_ORD) == 1)
    ).get_column("payload")
    assert kept.to_list() == ["c"]
    # High-water is the deduped batch max (4, 1), not lowered by the filter.
    assert ctx.get_attr(_table_key(0)) == [4, 1]


def test_empty_slot_yields_empty_list_and_no_state(tmp_path):
    ctx = FakeCtx(tmp_path / "work")

    out = CdcStageSrcPlugin().process_files(ctx, [[]])

    assert out == [[]]
    assert ctx.get_attr(_table_key(0)) is None


def test_stale_batch_does_not_lower_cursor(tmp_path):
    # Every row already seen (<= prior (10, 1)): output empty, cursor unchanged.
    f = _write(tmp_path / "a.parquet", [(7, 1, "a"), (9, 1, "b")])
    ctx = FakeCtx(tmp_path / "work", {_table_key(0): [10, 1]})

    out = CdcStageSrcPlugin().process_files(ctx, [[f]])

    assert _read(out[0][0]) == []
    assert ctx.get_attr(_table_key(0)) == [10, 1]


def test_slots_tracked_independently_by_index(tmp_path):
    f0 = _write(tmp_path / "s0.parquet", [(1, 1, "x"), (2, 1, "y")])
    f1 = _write(tmp_path / "s1.parquet", [(10, 1, "p"), (20, 1, "q")])
    ctx = FakeCtx(tmp_path / "work", {_table_key(1): [10, 1]})

    out = CdcStageSrcPlugin().process_files(ctx, [[f0], [f1]])

    assert _read(out[0][0]) == [(1, 1), (2, 1)]  # slot 0: no prior
    assert _read(out[1][0]) == [(20, 1)]  # slot 1: prior (10,1) filters <= it
    assert ctx.get_attr(_table_key(0)) == [2, 1]
    assert ctx.get_attr(_table_key(1)) == [20, 1]


def test_entry_point_binding_shape():
    assert CDC_STAGE_SRC.src_type == "cdcStage"
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
