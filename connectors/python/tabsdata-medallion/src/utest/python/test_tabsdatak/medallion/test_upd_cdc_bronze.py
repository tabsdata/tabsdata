#
# Copyright 2026 Tabsdata Inc.
#

from datetime import datetime, timezone

import pandas as pd
import pytest

import tabsdatak.tableframe.datatypes as td_dtypes
from tabsdatak.tableframe import TableFrame
from tabsdatak._medallion.bronze.upd_cdc import (
    OP_DELETE,
    OP_INSERT,
    OP_TOMBSTONE,
    OP_UPDATE,
    build_upd_cdc_bronze,
)
from tabsdatak._medallion.constants import (
    MEDALLION_SYSTEM_COLUMN_BRONZE_TS,
    MEDALLION_SYSTEM_COLUMN_OP,
    MEDALLION_SYSTEM_COLUMN_PK,
    MEDALLION_SYSTEM_COLUMN_SOURCE_TS,
)

pytestmark = pytest.mark.medallion

# ── Metadata column names ────────────────────────────────────────────────────
# Output metadata columns produced by build_upd_cdc_bronze.
M_OP = MEDALLION_SYSTEM_COLUMN_OP
M_SOURCE_TS = MEDALLION_SYSTEM_COLUMN_SOURCE_TS
M_BRONZE_TS = MEDALLION_SYSTEM_COLUMN_BRONZE_TS
M_PK = MEDALLION_SYSTEM_COLUMN_PK

# Composite primary key used in these tests.
PK = [
    {"tenant_id": td_dtypes.String},
    {"entity_id": td_dtypes.Int64},
]
EXPECTED_PK_COLS = ["tenant_id", "entity_id"]

# Source-side column names that the function reads from upd / full rows.
UPDATED_TS_COL = "updated_at"
IS_DELETE_COL = "is_deleted"

# UTC-aware sample timestamps for the source-side `updated_at` column.
TS_PAST = datetime(2024, 1, 1, 12, 0, 0, tzinfo=timezone.utc)
TS_MID = datetime(2024, 1, 2, 12, 0, 0, tzinfo=timezone.utc)
TS_FUTURE = datetime(2024, 1, 3, 12, 0, 0, tzinfo=timezone.utc)


# ── Helpers ──────────────────────────────────────────────────────────────────


def make_upd_batch(rows: list[tuple]) -> TableFrame:
    """Build an update-batch (or full-snapshot) TableFrame.

    rows is a list of (tenant_id, entity_id, name, updated_at, is_deleted) tuples.
    Schema mirrors a typical update-stream / soft-delete source table:
      tenant_id   VARCHAR    PK part 1
      entity_id   BIGINT     PK part 2
      name        VARCHAR    payload
      updated_at  TIMESTAMP  source-side mutation time (UTC-aware)
      is_deleted  BOOLEAN    soft-delete flag
    """
    assert rows, "make_upd_batch needs at least one row"
    return TableFrame.from_dict(
        {
            "tenant_id": [r[0] for r in rows],
            "entity_id": [r[1] for r in rows],
            "name": [r[2] for r in rows],
            UPDATED_TS_COL: [r[3] for r in rows],
            IS_DELETE_COL: [r[4] for r in rows],
        }
    ).cast(
        {
            "tenant_id": td_dtypes.String,
            "entity_id": td_dtypes.Int64,
            "name": td_dtypes.String,
            IS_DELETE_COL: td_dtypes.Boolean,
        }
    )


def make_prior_bronze(rows: list[tuple]) -> TableFrame:
    """Build a prior-bronze TableFrame.

    rows is a list of (tenant_id, entity_id, op) tuples. The function only
    reads pk_cols + @td.medallion.op from prior_bronze, so the minimal shape covers
    every path. T sentinels carry null PK values — use (None, None, OP_TOMBSTONE).
    """
    return TableFrame.from_dict(
        {
            "tenant_id": [r[0] for r in rows],
            "entity_id": [r[1] for r in rows],
            MEDALLION_SYSTEM_COLUMN_OP: [r[2] for r in rows],
        }
    ).cast(
        {
            "tenant_id": td_dtypes.String,
            "entity_id": td_dtypes.Int64,
            MEDALLION_SYSTEM_COLUMN_OP: td_dtypes.String,
        }
    )


def user_cols(tf: TableFrame) -> list[str]:
    """Drop tabsdata system columns ($td.*) so we can assert on user columns."""
    return [c for c in tf.columns if not c.startswith("$td.")]


def to_rows(tf: TableFrame) -> list[dict]:
    """Return rows as list-of-dicts with system columns stripped."""
    pdf = tf._to_pandas()
    keep = [c for c in pdf.columns if not c.startswith("$td.")]
    return pdf[keep].to_dict(orient="records")


def call(full_commits, upd_commits, prior_bronze) -> TableFrame:
    return build_upd_cdc_bronze(
        full_commits=full_commits,
        upd_commits=upd_commits,
        prior_bronze=prior_bronze,
        pk=PK,
        updated_ts_col=UPDATED_TS_COL,
        is_delete_col=IS_DELETE_COL,
    )


def _assert_utc_aware(ts, label: str) -> None:
    """@td.medallion.* timestamps must be tz-aware with zero UTC offset.

    A regression to naive timestamps trips ``tzinfo is None``; a regression
    to a non-UTC offset trips the offset comparison.
    """
    assert ts is not None, f"{label} missing"
    assert ts.tzinfo is not None, f"{label} should be UTC-aware, got naive: {ts!r}"
    assert ts.utcoffset() == timezone.utc.utcoffset(None), (
        f"{label} should be in UTC; got offset {ts.utcoffset()!r}"
    )


# ─────────────────────────────────────────────────────────────────────────────
# Backfill branch — full_commits non-empty
# ─────────────────────────────────────────────────────────────────────────────


def test_backfill_emits_erase_then_inserts():
    """Branch: full_commits non-empty → erase sentinel + I rows."""
    full = make_upd_batch(
        [
            ("t1", 1, "alice", TS_PAST, False),
            ("t1", 2, "bob", TS_PAST, False),
        ]
    )
    out = call(full_commits=[full], upd_commits=[], prior_bronze=None)

    rows = to_rows(out)
    assert sorted(r[M_OP] for r in rows) == sorted([OP_INSERT, OP_INSERT, OP_TOMBSTONE])
    seed = {(r["tenant_id"], int(r["entity_id"])) for r in rows if r[M_OP] == OP_INSERT}
    assert seed == {("t1", 1), ("t1", 2)}


def test_backfill_drops_soft_deleted_rows():
    """Backfill: rows where is_delete=True are filtered out of seed."""
    full = make_upd_batch(
        [
            ("t1", 1, "alice", TS_PAST, False),
            ("t1", 2, "bob", TS_PAST, True),  # soft-deleted → dropped
            ("t1", 3, "carol", TS_PAST, False),
        ]
    )
    out = call(full_commits=[full], upd_commits=[], prior_bronze=None)

    rows = to_rows(out)
    # 1 erase + 2 inserts; the soft-deleted row never makes it past the filter.
    assert sorted(r[M_OP] for r in rows) == sorted([OP_INSERT, OP_INSERT, OP_TOMBSTONE])
    seed = {(r["tenant_id"], int(r["entity_id"])) for r in rows if r[M_OP] == OP_INSERT}
    assert seed == {("t1", 1), ("t1", 3)}


def test_backfill_erase_row_has_null_pk():
    """Backfill erase sentinel: PK columns are null."""
    full = make_upd_batch([("t1", 1, "alice", TS_PAST, False)])
    out = call(full_commits=[full], upd_commits=[], prior_bronze=None)

    rows = to_rows(out)
    erase = next(r for r in rows if r[M_OP] == OP_TOMBSTONE)
    assert pd.isna(erase["tenant_id"])
    assert pd.isna(erase["entity_id"])


def test_backfill_uses_only_latest_commit():
    """Backfill: only the last entry in full_commits is used as the seed."""
    old = make_upd_batch([("t1", 1, "OLD", TS_PAST, False)])
    new = make_upd_batch([("t1", 2, "NEW", TS_MID, False)])
    out = call(full_commits=[old, new], upd_commits=[], prior_bronze=None)

    rows = to_rows(out)
    seed = {(r["tenant_id"], int(r["entity_id"])) for r in rows if r[M_OP] == OP_INSERT}
    # `new`, not `old` — the older snapshot is superseded.
    assert seed == {("t1", 2)}


def test_backfill_ignores_prior_bronze():
    """Backfill: prior_bronze is not consulted when full_commits is non-empty.

    A populated prior_bronze that — if consulted — would force the row in
    the seed to be classified as U (since PK 1 is "seen"). The output is
    still the bootstrap [T, I(1)].
    """
    full = make_upd_batch([("t1", 1, "alice", TS_PAST, False)])
    prior_bronze = make_prior_bronze([("t1", 1, OP_INSERT)])
    out = call(full_commits=[full], upd_commits=[], prior_bronze=prior_bronze)

    rows = to_rows(out)
    assert sorted(r[M_OP] for r in rows) == sorted([OP_INSERT, OP_TOMBSTONE])


def test_backfill_pk_meta_column():
    """Backfill: @td.medallion.pk lists the composite PK on every row."""
    full = make_upd_batch([("t1", 1, "alice", TS_PAST, False)])
    out = call(full_commits=[full], upd_commits=[], prior_bronze=None)

    assert M_PK in user_cols(out)
    for r in to_rows(out):
        assert list(r[M_PK]) == EXPECTED_PK_COLS


def test_backfill_timestamps_utc_aware():
    """Backfill: @td.medallion.source_ts and @td.medallion.bronze_ts are UTC-aware on every row."""
    full = make_upd_batch([("t1", 1, "alice", TS_PAST, False)])
    out = call(full_commits=[full], upd_commits=[], prior_bronze=None)

    for r in to_rows(out):
        _assert_utc_aware(r[M_SOURCE_TS], f"{M_SOURCE_TS} (op={r[M_OP]!r})")
        _assert_utc_aware(r[M_BRONZE_TS], f"{M_BRONZE_TS} (op={r[M_OP]!r})")


def test_backfill_payload_flows_through():
    """Backfill: source value columns are preserved on each seed row."""
    full = make_upd_batch([("t1", 7, "alice", TS_PAST, False)])
    out = call(full_commits=[full], upd_commits=[], prior_bronze=None)

    seed = next(r for r in to_rows(out) if r[M_OP] == OP_INSERT)
    assert seed["tenant_id"] == "t1"
    assert int(seed["entity_id"]) == 7
    assert seed["name"] == "alice"


# ─────────────────────────────────────────────────────────────────────────────
# Backfill branch — both full_commits AND upd_commits non-empty
# (snapshot lands together with an incremental batch in one transformer fire)
# ─────────────────────────────────────────────────────────────────────────────


def test_backfill_with_upd_folds_in_events_after_snapshot():
    """Backfill + upd: upd events with updated_ts > max(full.updated_ts) are folded in.

    The cutoff is ``full.updated_ts_col.max()``. Events at or before that
    boundary are already represented in the snapshot, so the helper drops
    them. Events strictly after the boundary are classified against the
    seed's PK set.
    """
    # full snapshot has updated_at values up through TS_PAST.
    full = make_upd_batch(
        [
            ("t1", 1, "alice", TS_PAST, False),
            ("t1", 2, "bob", TS_PAST, False),
        ]
    )
    # upd has events both at-or-before and strictly-after the cutoff.
    upd = make_upd_batch(
        [
            ("t1", 1, "stale-alice", TS_PAST, False),  # at cutoff → dropped
            ("t1", 1, "renamed-alice", TS_MID, False),  # > cutoff, seen → U
            ("t1", 99, "new-entity", TS_FUTURE, False),  # > cutoff, unseen → I
        ]
    )
    out = call(full_commits=[full], upd_commits=[upd], prior_bronze=None)

    rows = to_rows(out)
    # 1 erase + 2 seed I + 1 U (entity 1) + 1 I (entity 99) = 5 rows.
    assert len(rows) == 5
    ops_by_label = {}
    for r in rows:
        if r[M_OP] == OP_TOMBSTONE:
            continue
        # Distinguish seed I rows (stamped at run_ts) from folded-in upd
        # rows (stamped at the source-side updated_ts) by ts type.
        entity = int(r["entity_id"])
        if r[M_OP] == OP_INSERT and entity == 99:
            ops_by_label["upd-insert-entity-99"] = r[M_OP]
        elif r[M_OP] == OP_UPDATE and entity == 1:
            ops_by_label["upd-update-entity-1"] = r[M_OP]
        elif r[M_OP] == OP_INSERT:
            ops_by_label.setdefault("seed", []).append(entity)
    assert ops_by_label["upd-insert-entity-99"] == OP_INSERT
    assert ops_by_label["upd-update-entity-1"] == OP_UPDATE
    assert sorted(ops_by_label["seed"]) == [1, 2]


def test_backfill_with_upd_filters_events_at_or_before_cutoff():
    """Backfill + upd: events with updated_ts == max(full.updated_ts) are dropped.

    The cutoff comparison is strictly ``>``, so events at the exact boundary
    are treated as already-in-snapshot and excluded.
    """
    full = make_upd_batch([("t1", 1, "alice", TS_MID, False)])
    upd = make_upd_batch(
        [
            ("t1", 2, "stale", TS_PAST, False),  # before cutoff → dropped
            ("t1", 3, "boundary", TS_MID, False),  # at cutoff → dropped
            ("t1", 4, "fresh", TS_FUTURE, False),  # after cutoff → kept (I)
        ]
    )
    out = call(full_commits=[full], upd_commits=[upd], prior_bronze=None)

    rows = to_rows(out)
    # 1 erase + 1 seed I (entity 1) + 1 upd I (entity 4) = 3 rows.
    assert len(rows) == 3
    # Erase T row has null entity_id (NaN in pandas); skip it explicitly.
    folded = [
        r
        for r in rows
        if not pd.isna(r["entity_id"]) and int(r["entity_id"]) == 4
    ]
    assert len(folded) == 1
    assert folded[0][M_OP] == OP_INSERT


def test_backfill_with_upd_seen_pk_with_delete_becomes_delete():
    """Backfill + upd: seen PK in upd_after with is_delete=True → D event."""
    full = make_upd_batch([("t1", 1, "alice", TS_PAST, False)])
    upd = make_upd_batch(
        [
            ("t1", 1, "alice", TS_MID, True),  # seen + delete → D
        ]
    )
    out = call(full_commits=[full], upd_commits=[upd], prior_bronze=None)

    rows = to_rows(out)
    # Entity 1 appears twice: seed I (from full) and folded D (from upd_after).
    # Assert both are present via row counts since a dict would collapse them.
    assert sum(1 for r in rows if r[M_OP] == OP_INSERT and int(r["entity_id"]) == 1) == 1
    assert sum(1 for r in rows if r[M_OP] == OP_DELETE and int(r["entity_id"]) == 1) == 1


def test_backfill_with_upd_soft_deleted_seed_pk_is_unseen():
    """Backfill + upd: a PK soft-deleted in the snapshot is NOT in `seen`.

    The snapshot row gets filtered out of seed, so when the same PK shows
    up in upd_after, it's treated as an unseen PK → I event (an un-delete
    or re-insert from bronze's POV).
    """
    full = make_upd_batch(
        [
            ("t1", 1, "alice", TS_PAST, True),  # soft-deleted in snapshot
        ]
    )
    upd = make_upd_batch(
        [
            ("t1", 1, "alice-back", TS_MID, False),  # un-deleted post-snapshot
        ]
    )
    out = call(full_commits=[full], upd_commits=[upd], prior_bronze=None)

    rows = to_rows(out)
    # Seed is empty (the only row was soft-deleted), so 1 erase + 1 folded I.
    assert len(rows) == 2
    folded = [r for r in rows if r[M_OP] != OP_TOMBSTONE]
    assert len(folded) == 1
    assert folded[0][M_OP] == OP_INSERT
    assert folded[0]["name"] == "alice-back"


def test_backfill_with_upd_empty_upd_short_circuits():
    """Backfill + upd_commits=[]: short-circuits to [erase, seed] only.

    Equivalent to the no-upd path; the extra fold-in logic is skipped.
    """
    full = make_upd_batch([("t1", 1, "alice", TS_PAST, False)])
    out = call(full_commits=[full], upd_commits=[], prior_bronze=None)

    rows = to_rows(out)
    assert sorted(r[M_OP] for r in rows) == sorted([OP_INSERT, OP_TOMBSTONE])


def test_backfill_with_upd_uses_full_max_ts_not_survivors_max_ts():
    """Backfill + upd: cutoff is max(full.updated_ts_col), including soft-deleted rows.

    If a soft-deleted source row has a later updated_ts than any survivor,
    it still raises the cutoff — so upd events between the survivor max
    and the soft-deleted max are dropped (already represented in the
    snapshot). Using survivors' max would let them through erroneously.
    """
    full = make_upd_batch(
        [
            ("t1", 1, "alice", TS_PAST, False),  # survivor, ts=TS_PAST
            ("t1", 2, "deleted", TS_MID, True),  # soft-deleted, ts=TS_MID
        ]
    )
    upd = make_upd_batch(
        [
            # ts between survivor max (TS_PAST) and full max (TS_MID).
            # Should be dropped because full's max is TS_MID, not TS_PAST.
            ("t1", 3, "between", TS_PAST, False),
        ]
    )
    out = call(full_commits=[full], upd_commits=[upd], prior_bronze=None)

    rows = to_rows(out)
    # Only erase + seed I (entity 1) — the upd row at ts=TS_PAST is at-or-
    # before the cutoff (TS_MID) and gets dropped.
    assert sorted(r[M_OP] for r in rows) == sorted([OP_INSERT, OP_TOMBSTONE])
    seed_entities = {int(r["entity_id"]) for r in rows if r[M_OP] == OP_INSERT}
    assert seed_entities == {1}


# ─────────────────────────────────────────────────────────────────────────────
# Incremental branch, prior_bronze=None — every row → I, soft-deletes dropped
# ─────────────────────────────────────────────────────────────────────────────


def test_incremental_no_prior_bronze_all_inserts():
    """Branch: prior_bronze=None → every upd row → I event."""
    upd = make_upd_batch(
        [
            ("t1", 1, "alice", TS_PAST, False),
            ("t1", 2, "bob", TS_MID, False),
        ]
    )
    out = call(full_commits=[], upd_commits=[upd], prior_bronze=None)

    rows = to_rows(out)
    assert len(rows) == 2
    assert all(r[M_OP] == OP_INSERT for r in rows)
    assert {(r["tenant_id"], int(r["entity_id"])) for r in rows} == {
        ("t1", 1),
        ("t1", 2),
    }


def test_incremental_no_prior_bronze_drops_soft_deletes():
    """No prior_bronze: is_delete=True rows are filtered out."""
    upd = make_upd_batch(
        [
            ("t1", 1, "alice", TS_PAST, False),
            ("t1", 2, "bob", TS_MID, True),  # filtered
            ("t1", 3, "carol", TS_FUTURE, False),
        ]
    )
    out = call(full_commits=[], upd_commits=[upd], prior_bronze=None)

    rows = to_rows(out)
    assert len(rows) == 2
    assert all(r[M_OP] == OP_INSERT for r in rows)
    assert {(r["tenant_id"], int(r["entity_id"])) for r in rows} == {
        ("t1", 1),
        ("t1", 3),
    }


def test_incremental_no_prior_bronze_diagonal_concat():
    """No prior_bronze: multiple upd batches are diagonal-concatenated."""
    batch_a = make_upd_batch([("t1", 1, "alice", TS_PAST, False)])
    batch_b = make_upd_batch([("t1", 2, "bob", TS_MID, False)])
    out = call(full_commits=[], upd_commits=[batch_a, batch_b], prior_bronze=None)

    rows = to_rows(out)
    assert len(rows) == 2
    assert {(r["tenant_id"], int(r["entity_id"])) for r in rows} == {
        ("t1", 1),
        ("t1", 2),
    }


def test_incremental_no_prior_bronze_pk_meta_column():
    """No prior_bronze: @td.medallion.pk on every row."""
    upd = make_upd_batch([("t1", 1, "alice", TS_PAST, False)])
    out = call(full_commits=[], upd_commits=[upd], prior_bronze=None)

    assert M_PK in user_cols(out)
    for r in to_rows(out):
        assert list(r[M_PK]) == EXPECTED_PK_COLS


def test_incremental_no_prior_bronze_timestamps_utc_aware():
    """No prior_bronze: @td.medallion.source_ts and @td.medallion.bronze_ts are UTC-aware."""
    upd = make_upd_batch([("t1", 1, "alice", TS_PAST, False)])
    out = call(full_commits=[], upd_commits=[upd], prior_bronze=None)

    for r in to_rows(out):
        _assert_utc_aware(r[M_SOURCE_TS], M_SOURCE_TS)
        _assert_utc_aware(r[M_BRONZE_TS], M_BRONZE_TS)


# ─────────────────────────────────────────────────────────────────────────────
# Incremental branch, prior_bronze populated — I/U/D classification
# ─────────────────────────────────────────────────────────────────────────────


def test_incremental_unseen_pk_becomes_insert():
    """prior_bronze present: PK not in prior_bronze → I event."""
    prior_bronze = make_prior_bronze([("t1", 1, OP_INSERT)])
    upd = make_upd_batch([("t1", 99, "new", TS_MID, False)])
    out = call(full_commits=[], upd_commits=[upd], prior_bronze=prior_bronze)

    rows = to_rows(out)
    assert len(rows) == 1
    assert rows[0][M_OP] == OP_INSERT
    assert int(rows[0]["entity_id"]) == 99


def test_incremental_seen_pk_no_delete_becomes_update():
    """prior_bronze present: PK in prior_bronze + is_delete=False → U."""
    prior_bronze = make_prior_bronze([("t1", 1, OP_INSERT)])
    upd = make_upd_batch([("t1", 1, "renamed", TS_MID, False)])
    out = call(full_commits=[], upd_commits=[upd], prior_bronze=prior_bronze)

    rows = to_rows(out)
    assert len(rows) == 1
    assert rows[0][M_OP] == OP_UPDATE
    assert rows[0]["name"] == "renamed"


def test_incremental_seen_pk_with_delete_becomes_delete():
    """prior_bronze present: PK in prior_bronze + is_delete=True → D."""
    prior_bronze = make_prior_bronze([("t1", 1, OP_INSERT)])
    upd = make_upd_batch([("t1", 1, "alice", TS_MID, True)])
    out = call(full_commits=[], upd_commits=[upd], prior_bronze=prior_bronze)

    rows = to_rows(out)
    assert len(rows) == 1
    assert rows[0][M_OP] == OP_DELETE


def test_incremental_unseen_pk_with_delete_becomes_delete():
    """prior_bronze present: is_delete fires first in the classifier, so
    an unseen PK with is_delete=True is emitted as D — regardless of
    whether bronze has ever seen the PK.

    Note this still differs from the no-prior_bronze branch, where
    soft-deleted rows for unseen PKs are filtered out entirely (no D
    emitted). The inconsistency between the two branches is known.
    """
    prior_bronze = make_prior_bronze([("t1", 1, OP_INSERT)])
    upd = make_upd_batch([("t1", 99, "ghost", TS_MID, True)])
    out = call(full_commits=[], upd_commits=[upd], prior_bronze=prior_bronze)

    rows = to_rows(out)
    assert len(rows) == 1
    assert rows[0][M_OP] == OP_DELETE


def test_incremental_mixed_iud():
    """prior_bronze present: one upd batch → I + U + D classification mix."""
    prior_bronze = make_prior_bronze(
        [
            ("t1", 1, OP_INSERT),
            ("t1", 2, OP_INSERT),
        ]
    )
    upd = make_upd_batch(
        [
            ("t1", 1, "renamed", TS_MID, False),  # seen + !delete → U
            ("t1", 2, "alice", TS_MID, True),  # seen + delete → D
            ("t1", 3, "new", TS_MID, False),  # unseen → I
        ]
    )
    out = call(full_commits=[], upd_commits=[upd], prior_bronze=prior_bronze)

    rows = to_rows(out)
    ops_by_entity = {int(r["entity_id"]): r[M_OP] for r in rows}
    assert ops_by_entity == {1: OP_UPDATE, 2: OP_DELETE, 3: OP_INSERT}


def test_incremental_t_sentinel_excluded_from_seen():
    """prior_bronze present: T sentinels (with null PKs) are excluded from `seen`.

    Without the T filter, the null-PK row in prior_bronze could pollute the
    `seen` set. This test exercises the filter by including a T sentinel in
    prior_bronze and verifying classification of a new PK row.
    """
    prior_bronze = make_prior_bronze(
        [
            (None, None, OP_TOMBSTONE),  # erase sentinel, must be excluded from seen
            ("t1", 1, OP_INSERT),
        ]
    )
    upd = make_upd_batch([("t1", 99, "new", TS_MID, False)])
    out = call(full_commits=[], upd_commits=[upd], prior_bronze=prior_bronze)

    rows = to_rows(out)
    assert len(rows) == 1
    assert rows[0][M_OP] == OP_INSERT  # PK 99 not in seen → I


def test_incremental_seen_helper_col_dropped():
    """prior_bronze present: the internal `_seen` column is dropped from output."""
    prior_bronze = make_prior_bronze([("t1", 1, OP_INSERT)])
    upd = make_upd_batch([("t1", 1, "renamed", TS_MID, False)])
    out = call(full_commits=[], upd_commits=[upd], prior_bronze=prior_bronze)

    assert "_seen" not in user_cols(out)


def test_incremental_source_ts_mirrors_updated_ts_col():
    """prior_bronze present: @td.medallion.source_ts mirrors the per-row updated_ts_col value."""
    prior_bronze = make_prior_bronze([("t1", 1, OP_INSERT)])
    upd = make_upd_batch(
        [
            ("t1", 1, "renamed", TS_PAST, False),  # seen → U, ts=TS_PAST
            ("t1", 99, "new", TS_FUTURE, False),  # unseen → I, ts=TS_FUTURE
        ]
    )
    out = call(full_commits=[], upd_commits=[upd], prior_bronze=prior_bronze)

    by_entity = {int(r["entity_id"]): r for r in to_rows(out)}
    _assert_utc_aware(by_entity[1][M_SOURCE_TS], "entity 1 @td.medallion.source_ts")
    _assert_utc_aware(by_entity[99][M_SOURCE_TS], "entity 99 @td.medallion.source_ts")
    # Per-row mirror: source_ts == the row's updated_at, not run_ts.
    assert by_entity[1][M_SOURCE_TS] == TS_PAST
    assert by_entity[99][M_SOURCE_TS] == TS_FUTURE


def test_incremental_pk_meta_column_with_prior_bronze():
    """prior_bronze present: @td.medallion.pk on every output row."""
    prior_bronze = make_prior_bronze([("t1", 1, OP_INSERT)])
    upd = make_upd_batch([("t1", 1, "alice", TS_MID, False)])
    out = call(full_commits=[], upd_commits=[upd], prior_bronze=prior_bronze)

    assert M_PK in user_cols(out)
    for r in to_rows(out):
        assert list(r[M_PK]) == EXPECTED_PK_COLS


def test_incremental_bronze_ts_utc_aware_with_prior_bronze():
    """prior_bronze present: @td.medallion.bronze_ts is UTC-aware (run_ts stamp)."""
    prior_bronze = make_prior_bronze([("t1", 1, OP_INSERT)])
    upd = make_upd_batch([("t1", 1, "alice", TS_MID, False)])
    out = call(full_commits=[], upd_commits=[upd], prior_bronze=prior_bronze)

    for r in to_rows(out):
        _assert_utc_aware(r[M_BRONZE_TS], f"{M_BRONZE_TS} (op={r[M_OP]!r})")


def test_incremental_diagonal_concat_with_prior_bronze():
    """prior_bronze present: multiple upd batches are diagonal-concatenated."""
    prior_bronze = make_prior_bronze([("t1", 1, OP_INSERT)])
    batch_a = make_upd_batch([("t1", 1, "renamed", TS_PAST, False)])
    batch_b = make_upd_batch([("t1", 99, "new", TS_MID, False)])
    out = call(
        full_commits=[],
        upd_commits=[batch_a, batch_b],
        prior_bronze=prior_bronze,
    )

    rows = to_rows(out)
    ops_by_entity = {int(r["entity_id"]): r[M_OP] for r in rows}
    assert ops_by_entity == {1: OP_UPDATE, 99: OP_INSERT}


def test_incremental_prior_bronze_duplicate_pks_collapsed():
    """prior_bronze present: duplicate PKs in prior_bronze collapse via .unique() in seen.

    The seen set is built via ``prior_bronze.select(pk_cols).unique()``, so a
    PK that appears in prior_bronze as both I and a later U is treated as a
    single 'seen' PK. Without the unique(), the left-join would multiply
    upd rows by the number of prior occurrences.
    """
    prior_bronze = make_prior_bronze(
        [
            ("t1", 1, OP_INSERT),
            ("t1", 1, OP_UPDATE),  # same PK, second op in bronze history
        ]
    )
    upd = make_upd_batch([("t1", 1, "renamed-again", TS_FUTURE, False)])
    out = call(full_commits=[], upd_commits=[upd], prior_bronze=prior_bronze)

    rows = to_rows(out)
    assert len(rows) == 1  # no row-explosion
    assert rows[0][M_OP] == OP_UPDATE


def test_incremental_value_columns_flow_through_with_prior_bronze():
    """prior_bronze present: source value columns are preserved on emitted rows."""
    prior_bronze = make_prior_bronze([("t1", 1, OP_INSERT)])
    upd = make_upd_batch([("t1", 1, "renamed", TS_MID, False)])
    out = call(full_commits=[], upd_commits=[upd], prior_bronze=prior_bronze)

    rows = to_rows(out)
    assert len(rows) == 1
    assert rows[0]["name"] == "renamed"


# ─────────────────────────────────────────────────────────────────────────────
# Incremental branch — no update data at all → None
# ─────────────────────────────────────────────────────────────────────────────


@pytest.mark.parametrize(
    "upd_commits",
    [
        pytest.param([], id="empty_list"),
        pytest.param(None, id="none"),
        pytest.param([None, None], id="all_none_entries"),
    ],
)
@pytest.mark.parametrize("with_prior", [False, True], ids=["no_prior", "with_prior"])
def test_incremental_no_update_data_returns_none(upd_commits, with_prior):
    """Incremental branch with no update data → `None`. With `full_commits`
    empty (so we're on the incremental path) and `upd_commits` that
    concatenates to nothing — an empty list, `None`, or all-`None` entries —
    `concat` yields `None` and the helper returns `None`. This early return
    fires BEFORE the `prior_bronze` handling, so it returns `None` whether
    or not a prior bronze exists (the 'nothing landed this fire' path)."""
    prior_bronze = make_prior_bronze([("t1", 1, OP_INSERT)]) if with_prior else None
    out = call(full_commits=[], upd_commits=upd_commits, prior_bronze=prior_bronze)
    assert out is None
