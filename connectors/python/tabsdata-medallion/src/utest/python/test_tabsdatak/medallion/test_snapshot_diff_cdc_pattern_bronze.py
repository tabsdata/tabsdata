#
# Copyright 2026 Tabsdata Inc.
#

from datetime import timezone

import pytest

import tabsdatak.tableframe.datatypes as td_dtypes
from tabsdatak.tableframe import TableFrame
from tabsdatak.tableframe.functions import col
from tabsdatak._medallion.bronze.snapshot_diff_cdc import (
from tabsdatak._medallion.constants import (
    MEDALLION_SYSTEM_COLUMN_BRONZE_TS,
    MEDALLION_SYSTEM_COLUMN_OP,
    MEDALLION_SYSTEM_COLUMN_PK,
    MEDALLION_SYSTEM_COLUMN_SOURCE_TS,
)
    OP_DELETE,
    OP_INSERT,
    OP_TOMBSTONE,
    OP_UPDATE,
    build_snapshot_diff_cdc_bronze,
)

pytestmark = pytest.mark.medallion

# ── Metadata column names ────────────────────────────────────────────────────
# Output metadata columns produced by build_snapshot_diff_cdc_bronze.
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


# ── Helpers ──────────────────────────────────────────────────────────────────


def make_snapshot(rows: list[tuple]) -> TableFrame:
    """Build a snapshot TableFrame.

    rows is a list of (tenant_id, entity_id, name, status, amount) tuples.
    Schema matches a small but realistic upstream table:
      tenant_id  VARCHAR  PK part 1
      entity_id  BIGINT   PK part 2
      name       VARCHAR
      status     VARCHAR  (nullable — exercises ne_missing branch)
      amount     DOUBLE
    """
    assert rows, "use empty_snapshot() to build an empty frame"
    # Explicit cast pins dtypes so a column of all-None values doesn't degrade
    # to ``Null`` (Polars's default inference), which would break the downstream
    # diagonal concat that vstacks against non-None rows from the other side.
    return TableFrame.from_dict(
        {
            "tenant_id": [r[0] for r in rows],
            "entity_id": [r[1] for r in rows],
            "name": [r[2] for r in rows],
            "status": [r[3] for r in rows],
            "amount": [r[4] for r in rows],
        }
    ).cast(
        {
            "tenant_id": td_dtypes.String,
            "entity_id": td_dtypes.Int64,
            "name": td_dtypes.String,
            "status": td_dtypes.String,
            "amount": td_dtypes.Float64,
        }
    )


def empty_snapshot() -> TableFrame:
    """Build an empty snapshot TableFrame preserving the make_snapshot schema.

    Constructing via ``from_dict`` with empty lists triggers null-typed
    dtype inference, which trips ``head.schema[c]`` lookups downstream.
    The filter trick yields a frame with the right column dtypes and
    zero rows.
    """
    return make_snapshot([("__placeholder__", 0, "x", "x", 0.0)]).filter(
        col("tenant_id") == "__NEVER__"
    )


def make_new_nonempty() -> list[TableFrame]:
    """A non-empty ``new`` list — exercises the incremental branch.

    ``new`` is now ``list[TableFrame]`` (one entry per version created since the
    last transformer run); the helper checks ``len(new) == 0`` for the refresh
    trigger.
    """
    return [TableFrame.from_dict({"version": [1]})]


def empty_new() -> list[TableFrame]:
    """An empty ``new`` list — exercises the refresh-trigger branch."""
    return []


def user_cols(tf: TableFrame) -> list[str]:
    """Drop tabsdata system columns ($td.*) so we can assert on user columns."""
    return [c for c in tf.columns if not c.startswith("$td.")]


def to_rows(tf: TableFrame) -> list[dict]:
    """Return rows as list-of-dicts with system columns stripped."""
    pdf = tf._to_pandas()
    keep = [c for c in pdf.columns if not c.startswith("$td.")]
    return pdf[keep].to_dict(orient="records")


def call(prev, head, new) -> TableFrame:
    return build_snapshot_diff_cdc_bronze(prev=prev, head=head, new=new, pk=PK)


# ── Guard ────────────────────────────────────────────────────────────────────


def test_head_none_raises():
    """Branch: head=None → ValueError, regardless of prev/new."""
    with pytest.raises(ValueError, match="head"):
        build_snapshot_diff_cdc_bronze(
            prev=None, head=None, new=make_new_nonempty(), pk=PK
        )


# ── Bootstrap branch (prev is None / empty, or new is empty) ─────────────────


def test_prev_none_emits_bootstrap():
    """Branch: prev=None → erase sentinel + seed rows from head."""
    head = make_snapshot(
        [
            ("t1", 1, "name-0", "active", 100.0),
            ("t1", 2, "name-1", "active", 200.0),
        ]
    )
    out = call(prev=None, head=head, new=make_new_nonempty())

    rows = to_rows(out)
    # One erase + 2 seed rows.
    assert len(rows) == 3
    assert sorted(r[M_OP] for r in rows) == sorted([OP_INSERT, OP_INSERT, OP_TOMBSTONE])

    seed_keys = {
        (r["tenant_id"], int(r["entity_id"])) for r in rows if r[M_OP] == OP_INSERT
    }
    assert seed_keys == {("t1", 1), ("t1", 2)}


def test_prev_empty_emits_bootstrap():
    """Branch: prev.is_empty() → bootstrap path."""
    prev = empty_snapshot()
    head = make_snapshot([("t1", 1, "name-0", "active", 100.0)])
    out = call(prev=prev, head=head, new=make_new_nonempty())

    rows = to_rows(out)
    assert len(rows) == 2  # erase + 1 seed
    assert sorted(r[M_OP] for r in rows) == sorted([OP_INSERT, OP_TOMBSTONE])


def test_new_empty_refresh_emits_bootstrap():
    """Branch: new.is_empty() (refresh trigger) → bootstrap path even when prev has rows.

    The seed comes from `head`, NOT `prev` — the refresh re-bootstraps from
    the latest snapshot.
    """
    prev = make_snapshot([("t1", 1, "old-name", "active", 100.0)])
    head = make_snapshot([("t1", 2, "new-name", "active", 200.0)])
    out = call(prev=prev, head=head, new=empty_new())

    rows = to_rows(out)
    assert len(rows) == 2  # erase + 1 seed (from head, not prev)
    assert sorted(r[M_OP] for r in rows) == sorted([OP_INSERT, OP_TOMBSTONE])

    seed_row = [r for r in rows if r[M_OP] == OP_INSERT][0]
    assert seed_row["tenant_id"] == "t1"
    assert int(seed_row["entity_id"]) == 2  # head's entity, not prev's
    assert seed_row["name"] == "new-name"


def test_bootstrap_erase_row_has_null_pk_and_value_cols():
    """Bootstrap erase sentinel: PK columns AND value columns are null."""
    import pandas as pd

    head = make_snapshot([("t1", 1, "name-0", "active", 100.0)])
    out = call(prev=None, head=head, new=make_new_nonempty())

    rows = to_rows(out)
    erase = next(r for r in rows if r[M_OP] == OP_TOMBSTONE)
    # PK columns null.
    assert pd.isna(erase["tenant_id"])
    assert pd.isna(erase["entity_id"])
    # Value columns null.
    assert pd.isna(erase["name"])
    assert pd.isna(erase["status"])
    assert pd.isna(erase["amount"])


def test_bootstrap_value_columns_flow_through_to_seed():
    """Bootstrap seed: every value column from head appears on the seed row."""
    head = make_snapshot([("t1", 7, "alice", "pending", 42.0)])
    out = call(prev=None, head=head, new=make_new_nonempty())

    cols = user_cols(out)
    for expected in ["tenant_id", "entity_id", "name", "status", "amount"]:
        assert expected in cols

    rows = to_rows(out)
    seed = next(r for r in rows if r[M_OP] == OP_INSERT)
    assert seed["tenant_id"] == "t1"
    assert int(seed["entity_id"]) == 7
    assert seed["name"] == "alice"
    assert seed["status"] == "pending"
    assert float(seed["amount"]) == 42.0


# ── Incremental branch ──────────────────────────────────────────────────────


def test_incremental_inserts_only():
    """Incremental: rows in head not in prev → I events."""
    prev = make_snapshot([("t1", 1, "name-0", "active", 100.0)])
    head = make_snapshot(
        [
            ("t1", 1, "name-0", "active", 100.0),  # unchanged
            ("t1", 2, "name-1", "active", 200.0),  # new
        ]
    )
    out = call(prev=prev, head=head, new=make_new_nonempty())

    rows = to_rows(out)
    assert len(rows) == 1
    assert rows[0][M_OP] == OP_INSERT
    assert int(rows[0]["entity_id"]) == 2
    assert rows[0]["name"] == "name-1"


def test_incremental_deletes_only():
    """Incremental: rows in prev not in head → D events."""
    prev = make_snapshot(
        [
            ("t1", 1, "name-0", "active", 100.0),
            ("t1", 2, "name-1", "active", 200.0),
        ]
    )
    head = make_snapshot([("t1", 1, "name-0", "active", 100.0)])
    out = call(prev=prev, head=head, new=make_new_nonempty())

    rows = to_rows(out)
    assert len(rows) == 1
    assert rows[0][M_OP] == OP_DELETE
    assert int(rows[0]["entity_id"]) == 2


def test_incremental_updates_only():
    """Incremental: rows in both with differing value cols → U events."""
    prev = make_snapshot([("t1", 1, "name-0", "active", 100.0)])
    head = make_snapshot([("t1", 1, "name-CHANGED", "active", 100.0)])
    out = call(prev=prev, head=head, new=make_new_nonempty())

    rows = to_rows(out)
    assert len(rows) == 1
    assert rows[0][M_OP] == OP_UPDATE
    assert rows[0]["name"] == "name-CHANGED"


def test_incremental_no_changes_emits_nothing():
    """Incremental: identical prev & head → empty output."""
    snapshot = [("t1", 1, "name-0", "active", 100.0)]
    out = call(
        prev=make_snapshot(snapshot),
        head=make_snapshot(snapshot),
        new=make_new_nonempty(),
    )

    assert len(to_rows(out)) == 0


def test_incremental_mixed_iud():
    """Incremental: I + U + D in one diff (covers all three concat branches)."""
    prev = make_snapshot(
        [
            ("t1", 1, "name-0", "active", 100.0),  # kept, no change
            ("t1", 2, "name-1", "active", 200.0),  # will be updated
            ("t1", 3, "name-2", "active", 300.0),  # will be deleted
        ]
    )
    head = make_snapshot(
        [
            ("t1", 1, "name-0", "active", 100.0),  # unchanged
            ("t1", 2, "name-1", "active", 250.0),  # amount changed → U
            ("t1", 4, "name-3", "active", 400.0),  # new → I
        ]
    )
    out = call(prev=prev, head=head, new=make_new_nonempty())

    rows = to_rows(out)
    assert len(rows) == 3
    ops_by_entity = {int(r["entity_id"]): r[M_OP] for r in rows}
    assert ops_by_entity == {2: OP_UPDATE, 3: OP_DELETE, 4: OP_INSERT}


# ── ne_missing (null-safe diff) semantics ────────────────────────────────────


def test_incremental_null_to_value_captured_as_update():
    """NULL → non-NULL value column transition → U event (ne_missing semantics)."""
    prev = make_snapshot([("t1", 1, "name-0", None, 100.0)])
    head = make_snapshot([("t1", 1, "name-0", "active", 100.0)])
    out = call(prev=prev, head=head, new=make_new_nonempty())

    rows = to_rows(out)
    assert len(rows) == 1
    assert rows[0][M_OP] == OP_UPDATE
    assert rows[0]["status"] == "active"


def test_incremental_value_to_null_captured_as_update():
    """non-NULL → NULL value column transition → U event (ne_missing semantics)."""
    import pandas as pd

    prev = make_snapshot([("t1", 1, "name-0", "active", 100.0)])
    head = make_snapshot([("t1", 1, "name-0", None, 100.0)])
    out = call(prev=prev, head=head, new=make_new_nonempty())

    rows = to_rows(out)
    assert len(rows) == 1
    assert rows[0][M_OP] == OP_UPDATE
    assert pd.isna(rows[0]["status"])


def test_incremental_null_to_null_not_an_update():
    """NULL == NULL under ne_missing → NOT flagged as an update."""
    prev = make_snapshot([("t1", 1, "name-0", None, 100.0)])
    head = make_snapshot([("t1", 1, "name-0", None, 100.0)])
    out = call(prev=prev, head=head, new=make_new_nonempty())

    assert len(to_rows(out)) == 0


# ── @td.medallion.pk + timestamp meta columns ──────────────────────────────────────────


def test_pk_meta_column_on_bootstrap():
    """@td.medallion.pk column lists the composite PK on every bootstrap row."""
    head = make_snapshot([("t1", 1, "name-0", "active", 100.0)])
    out = call(prev=None, head=head, new=make_new_nonempty())

    assert M_PK in user_cols(out)
    rows = to_rows(out)
    for r in rows:
        assert list(r[M_PK]) == EXPECTED_PK_COLS


def test_pk_meta_column_on_incremental():
    """@td.medallion.pk column lists the composite PK on every incremental row (I/U/D)."""
    prev = make_snapshot(
        [
            ("t1", 1, "name-0", "active", 100.0),  # → D
            ("t1", 2, "name-1", "active", 200.0),  # → U (amount changes)
        ]
    )
    head = make_snapshot(
        [
            ("t1", 2, "name-1", "active", 250.0),
            ("t1", 3, "name-2", "active", 300.0),  # → I
        ]
    )
    out = call(prev=prev, head=head, new=make_new_nonempty())

    assert M_PK in user_cols(out)
    rows = to_rows(out)
    assert len(rows) == 3  # I + U + D — covers every branch.
    for r in rows:
        assert (
            list(r[M_PK]) == EXPECTED_PK_COLS
        ), f"row with op={r[M_OP]!r} has pk={r[M_PK]!r}, expected {EXPECTED_PK_COLS!r}"


def _assert_utc_aware(ts, label: str) -> None:
    """``build_snapshot_diff_cdc_bronze`` stamps run_ts via
    ``datetime.now(tz=timezone.utc)``; pandas preserves the UTC offset
    through ``to_pandas()``, so output cells must be tz-aware with zero
    offset. A regression to naive timestamps trips ``tzinfo is None``;
    a regression to a non-UTC offset trips the offset comparison.
    """
    assert ts is not None, f"{label} missing"
    assert ts.tzinfo is not None, f"{label} should be UTC-aware, got naive: {ts!r}"
    assert ts.utcoffset() == timezone.utc.utcoffset(
        None
    ), f"{label} should be in UTC; got offset {ts.utcoffset()!r}"


def test_timestamp_meta_columns_present_on_bootstrap():
    """@td.medallion.source_ts and @td.medallion.bronze_ts present and UTC-aware on bootstrap rows."""
    head = make_snapshot([("t1", 1, "name-0", "active", 100.0)])
    out = call(prev=None, head=head, new=make_new_nonempty())

    cols = user_cols(out)
    assert M_SOURCE_TS in cols
    assert M_BRONZE_TS in cols
    rows = to_rows(out)
    for r in rows:
        _assert_utc_aware(r[M_SOURCE_TS], f"{M_SOURCE_TS} (op={r[M_OP]!r})")
        _assert_utc_aware(r[M_BRONZE_TS], f"{M_BRONZE_TS} (op={r[M_OP]!r})")


def test_timestamp_meta_columns_present_on_incremental():
    """@td.medallion.source_ts and @td.medallion.bronze_ts present and UTC-aware on incremental rows."""
    prev = make_snapshot([("t1", 1, "name-0", "active", 100.0)])
    head = make_snapshot([("t1", 2, "name-1", "active", 200.0)])
    out = call(prev=prev, head=head, new=make_new_nonempty())

    cols = user_cols(out)
    assert M_SOURCE_TS in cols
    assert M_BRONZE_TS in cols
    rows = to_rows(out)
    for r in rows:
        _assert_utc_aware(r[M_SOURCE_TS], f"{M_SOURCE_TS} (op={r[M_OP]!r})")
        _assert_utc_aware(r[M_BRONZE_TS], f"{M_BRONZE_TS} (op={r[M_OP]!r})")
