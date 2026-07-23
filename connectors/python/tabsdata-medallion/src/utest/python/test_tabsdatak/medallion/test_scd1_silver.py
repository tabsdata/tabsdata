#
# Copyright 2026 Tabsdata Inc.
#

from datetime import datetime, timezone

import pytest

import tabsdatak.tableframe.datatypes as td_dtypes
from tabsdatak.tableframe import TableFrame
from tabsdatak.tableframe.functions import col
from tabsdatak._medallion.bronze.cdc import (
    OP_DELETE,
    OP_INSERT,
    OP_TOMBSTONE,
    OP_UPDATE,
)
from tabsdatak._medallion.silver.scd import build_scd1_silver

pytestmark = pytest.mark.medallion

# ── Bronze metadata columns ──────────────────────────────────────────────────
M_OP = "$m.op"
M_SOURCE_TS = "$m.source_ts"
M_BRONZE_TS = "$m.bronze_ts"
M_PK = "$m.pk"

# Composite PK used in these tests.
PK = [
    {"tenant_id": td_dtypes.String},
    {"entity_id": td_dtypes.Int64},
]
EXPECTED_PK_COLS = ["tenant_id", "entity_id"]

# UTC-aware sample timestamps.
TS_1 = datetime(2024, 1, 1, 12, 0, 0, tzinfo=timezone.utc)
TS_2 = datetime(2024, 1, 2, 12, 0, 0, tzinfo=timezone.utc)
TS_3 = datetime(2024, 1, 3, 12, 0, 0, tzinfo=timezone.utc)
TS_4 = datetime(2024, 1, 4, 12, 0, 0, tzinfo=timezone.utc)
TS_PRIOR = datetime(2023, 12, 1, 0, 0, 0, tzinfo=timezone.utc)
BRONZE_TS = datetime(2024, 6, 1, 0, 0, 0, tzinfo=timezone.utc)


# ── Helpers ──────────────────────────────────────────────────────────────────


def make_bronze_batch(rows: list[tuple]) -> TableFrame:
    """Build a bronze CDC-shaped TableFrame.

    rows is a list of (tenant_id, entity_id, name, op, source_ts) tuples.
    """
    assert rows, "make_bronze_batch needs at least one row"
    n = len(rows)
    return TableFrame.from_dict(
        {
            "tenant_id": [r[0] for r in rows],
            "entity_id": [r[1] for r in rows],
            "name": [r[2] for r in rows],
            M_OP: [r[3] for r in rows],
            M_SOURCE_TS: [r[4] for r in rows],
            M_BRONZE_TS: [BRONZE_TS] * n,
            M_PK: [EXPECTED_PK_COLS] * n,
        }
    ).cast(
        {
            "tenant_id": td_dtypes.String,
            "entity_id": td_dtypes.Int64,
            "name": td_dtypes.String,
            M_OP: td_dtypes.String,
        }
    )


def make_t_sentinel(source_ts: datetime) -> TableFrame:
    """Build a T (erase) sentinel row with null PKs."""
    return TableFrame.from_dict(
        {
            "tenant_id": [None],
            "entity_id": [None],
            "name": [None],
            M_OP: [OP_TOMBSTONE],
            M_SOURCE_TS: [source_ts],
            M_BRONZE_TS: [BRONZE_TS],
            M_PK: [EXPECTED_PK_COLS],
        }
    ).cast(
        {
            "tenant_id": td_dtypes.String,
            "entity_id": td_dtypes.Int64,
            "name": td_dtypes.String,
            M_OP: td_dtypes.String,
        }
    )


def make_scd1_head(rows: list[tuple]) -> TableFrame:
    """Build a prior `_scd1_raw` TableFrame — one row per PK, carrying
    the bronze metadata from when each row was last touched."""
    assert rows, "make_scd1_head needs at least one row"
    n = len(rows)
    return TableFrame.from_dict(
        {
            "tenant_id": [r[0] for r in rows],
            "entity_id": [r[1] for r in rows],
            "name": [r[2] for r in rows],
            M_OP: [OP_INSERT] * n,
            M_SOURCE_TS: [r[3] for r in rows],
            M_BRONZE_TS: [BRONZE_TS] * n,
            M_PK: [EXPECTED_PK_COLS] * n,
        }
    ).cast(
        {
            "tenant_id": td_dtypes.String,
            "entity_id": td_dtypes.Int64,
            "name": td_dtypes.String,
            M_OP: td_dtypes.String,
        }
    )


def user_cols(tf: TableFrame) -> list[str]:
    return [c for c in tf.columns if not c.startswith("$td.")]


def to_rows(tf: TableFrame) -> list[dict]:
    pdf = tf.to_pandas()
    keep = [c for c in pdf.columns if not c.startswith("$td.")]
    return pdf[keep].to_dict(orient="records")


def call(
    bronze: list[TableFrame],
    scd1_head: TableFrame | None,
) -> TableFrame:
    return build_scd1_silver(bronze=bronze, scd1_head=scd1_head)


# ── First-fire tests ─────────────────────────────────────────────────────────


def test_first_fire_single_insert():
    """No prior, single I → row lands in output."""
    bronze = [make_bronze_batch([("t1", 1, "Alice", OP_INSERT, TS_1)])]
    out = call(bronze, scd1_head=None)

    rows = to_rows(out)
    assert len(rows) == 1
    assert rows[0]["name"] == "Alice"


def test_first_fire_empty_scd1_head_treated_like_none():
    bronze = [make_bronze_batch([("t1", 1, "Alice", OP_INSERT, TS_1)])]
    empty_head = make_scd1_head([("dummy", 999, "x", TS_PRIOR)]).filter(
        col("entity_id") < 0
    )
    out = call(bronze, scd1_head=empty_head)

    assert len(to_rows(out)) == 1


# ── Empty-bronze guard ──────────────────────────────────────────────────────


def test_empty_bronze_list_returns_scd1_head_unchanged():
    """`bronze=[]` with a non-empty `scd1_head` → output is scd1_head
    unchanged."""
    prior = make_scd1_head(
        [
            ("t1", 1, "Alice", TS_PRIOR),
            ("t1", 2, "Bob", TS_PRIOR),
        ]
    )
    out = call([], scd1_head=prior)

    rows = to_rows(out)
    assert len(rows) == 2
    assert {r["name"] for r in rows} == {"Alice", "Bob"}


def test_empty_bronze_frames_treated_as_empty():
    """`bronze=[empty_frame]` (non-empty list, every frame empty) is
    equivalent to `bronze=[]` — return scd1_head unchanged."""
    prior = make_scd1_head([("t1", 1, "Alice", TS_PRIOR)])
    empty_bronze = make_bronze_batch(
        [("t1", 99, "ghost", OP_INSERT, TS_1)]
    ).filter(col("entity_id") < 0)
    out = call([empty_bronze], scd1_head=prior)

    rows = to_rows(out)
    assert len(rows) == 1
    assert rows[0]["name"] == "Alice"


def test_empty_bronze_with_no_scd1_head_raises():
    """Both inputs empty → ValueError. No schema to return."""
    with pytest.raises(ValueError, match="bronze input is empty"):
        call([], scd1_head=None)


# ── Intra-batch op collapse ──────────────────────────────────────────────────


def test_intra_batch_i_then_u_keeps_only_last_value():
    """I→U for same PK in one batch → only the U value lands.
    Verifies the `unique(keep="last")` collapse."""
    bronze = [
        make_bronze_batch(
            [
                ("t1", 1, "Alice@v1", OP_INSERT, TS_1),
                ("t1", 1, "Alice@v2", OP_UPDATE, TS_2),
            ]
        )
    ]
    out = call(bronze, scd1_head=None)

    rows = to_rows(out)
    assert len(rows) == 1
    assert rows[0]["name"] == "Alice@v2"


def test_intra_batch_i_then_d_results_in_no_row():
    """I→D in one batch → final state is "deleted" → no row in output."""
    bronze = [
        make_bronze_batch(
            [
                ("t1", 1, "Alice", OP_INSERT, TS_1),
                ("t1", 1, "Alice", OP_DELETE, TS_2),
            ]
        )
    ]
    out = call(bronze, scd1_head=None)

    assert len(to_rows(out)) == 0


def test_intra_batch_i_d_i_collapses_to_inserted():
    """I→D→I → final state is "inserted" (the second I) → row stays."""
    bronze = [
        make_bronze_batch(
            [
                ("t1", 1, "Alice@v1", OP_INSERT, TS_1),
                ("t1", 1, "Alice@v1", OP_DELETE, TS_2),
                ("t1", 1, "Alice@v2", OP_INSERT, TS_3),
            ]
        )
    ]
    out = call(bronze, scd1_head=None)

    rows = to_rows(out)
    assert len(rows) == 1
    assert rows[0]["name"] == "Alice@v2"


# ── Cross-commit (with prior) tests ──────────────────────────────────────────


def test_prior_row_untouched_carries_through():
    """Prior has Alice + Bob; new batch touches only Alice. Bob
    carries through unchanged."""
    prior = make_scd1_head(
        [
            ("t1", 1, "Alice@v0", TS_PRIOR),
            ("t1", 2, "Bob@v0", TS_PRIOR),
        ]
    )
    bronze = [make_bronze_batch([("t1", 1, "Alice@v1", OP_UPDATE, TS_1)])]
    out = call(bronze, scd1_head=prior)

    rows = to_rows(out)
    by_entity = {int(r["entity_id"]): r for r in rows}
    assert by_entity[1]["name"] == "Alice@v1"
    assert by_entity[2]["name"] == "Bob@v0"


def test_prior_row_overwritten_by_u():
    """Prior Alice; new U Alice → prior row replaced by new value."""
    prior = make_scd1_head([("t1", 1, "Alice@v0", TS_PRIOR)])
    bronze = [make_bronze_batch([("t1", 1, "Alice@v1", OP_UPDATE, TS_1)])]
    out = call(bronze, scd1_head=prior)

    rows = to_rows(out)
    assert len(rows) == 1
    assert rows[0]["name"] == "Alice@v1"
    # The new row's $m.source_ts should be TS_1 (from the new bronze),
    # not TS_PRIOR.
    assert rows[0][M_SOURCE_TS].to_pydatetime() == TS_1


def test_prior_row_removed_by_d():
    """Prior Alice; new D Alice → row removed from output."""
    prior = make_scd1_head([("t1", 1, "Alice@v0", TS_PRIOR)])
    bronze = [make_bronze_batch([("t1", 1, "Alice", OP_DELETE, TS_1)])]
    out = call(bronze, scd1_head=prior)

    assert len(to_rows(out)) == 0


def test_composite_pk_anti_join_requires_all_pk_cols_match():
    """An update on (t1, 1) does NOT affect (t1, 2) or (t2, 1)."""
    prior = make_scd1_head(
        [
            ("t1", 1, "row-a", TS_PRIOR),
            ("t1", 2, "row-b", TS_PRIOR),
            ("t2", 1, "row-c", TS_PRIOR),
        ]
    )
    bronze = [make_bronze_batch([("t1", 1, "row-a@updated", OP_UPDATE, TS_1)])]
    out = call(bronze, scd1_head=prior)

    rows = to_rows(out)
    by_key = {(r["tenant_id"], int(r["entity_id"])): r for r in rows}
    assert by_key[("t1", 1)]["name"] == "row-a@updated"
    assert by_key[("t1", 2)]["name"] == "row-b"
    assert by_key[("t2", 1)]["name"] == "row-c"


# ── T-sentinel tests ─────────────────────────────────────────────────────────


def test_t_sentinel_wipes_prior_state():
    """T → prior_scd1_head is discarded; only post-T ops contribute."""
    prior = make_scd1_head(
        [
            ("t1", 1, "Alice@v0", TS_PRIOR),
            ("t1", 2, "Bob@v0", TS_PRIOR),
        ]
    )
    bronze = [
        make_t_sentinel(TS_2),
        make_bronze_batch([("t1", 3, "Carol", OP_INSERT, TS_3)]),
    ]
    out = call(bronze, scd1_head=prior)

    rows = to_rows(out)
    assert len(rows) == 1
    assert rows[0]["name"] == "Carol"


def test_t_sentinel_drops_ops_before_cutoff():
    """Ops at-or-before the T's source_ts are dropped; ops after are
    kept. The pre-T row should NOT land in the output."""
    # Embed both the pre-T row and the T sentinel in a single
    # TableFrame so the diagonal-concat at the helper entry doesn't
    # have to unify tabsdata's $td.src column across separate
    # `from_dict` constructions (which can stack Null vs Binary).
    pre_t_and_t = TableFrame.from_dict(
        {
            "tenant_id": ["t1", None],
            "entity_id": [1, None],
            "name": ["before", None],
            M_OP: [OP_INSERT, OP_TOMBSTONE],
            M_SOURCE_TS: [TS_1, TS_2],
            M_BRONZE_TS: [BRONZE_TS, BRONZE_TS],
            M_PK: [EXPECTED_PK_COLS, EXPECTED_PK_COLS],
        }
    ).cast(
        {
            "tenant_id": td_dtypes.String,
            "entity_id": td_dtypes.Int64,
            "name": td_dtypes.String,
            M_OP: td_dtypes.String,
        }
    )
    post_t = make_bronze_batch([("t1", 2, "after", OP_INSERT, TS_3)])
    out = call([pre_t_and_t, post_t], scd1_head=None)

    rows = to_rows(out)
    assert len(rows) == 1
    assert rows[0]["name"] == "after"


def test_latest_t_takes_precedence():
    """Two T's → the latest T sets the cutoff."""
    bronze = [
        make_t_sentinel(TS_1),
        make_bronze_batch([("t1", 1, "between", OP_INSERT, TS_2)]),
        make_t_sentinel(TS_3),
        make_bronze_batch([("t1", 2, "after-T2", OP_INSERT, TS_4)]),
    ]
    out = call(bronze, scd1_head=None)

    rows = to_rows(out)
    assert len(rows) == 1
    assert rows[0]["name"] == "after-T2"


# ── Bronze-metadata flow-through ────────────────────────────────────────────


def test_bronze_metadata_rides_through():
    """`$m.op` / `$m.bronze_ts` / `$m.pk` / `$m.source_ts` survive on
    output — stage-2 cleansing drops them."""
    bronze = [make_bronze_batch([("t1", 1, "Alice", OP_INSERT, TS_1)])]
    out = call(bronze, scd1_head=None)

    cols = user_cols(out)
    assert M_OP in cols
    assert M_BRONZE_TS in cols
    assert M_PK in cols
    assert M_SOURCE_TS in cols


# ── Multiple commits ────────────────────────────────────────────────────────


def test_multiple_bronze_commits_diagonally_concatenated():
    """`bronze` as a list of multiple TableFrames → all commits folded
    into the latest-per-PK collapse."""
    a = make_bronze_batch([("t1", 1, "Alice@v1", OP_INSERT, TS_1)])
    b = make_bronze_batch([("t1", 2, "Bob", OP_INSERT, TS_2)])
    c = make_bronze_batch([("t1", 1, "Alice@v2", OP_UPDATE, TS_3)])
    out = call([a, b, c], scd1_head=None)

    rows = to_rows(out)
    by_entity = {int(r["entity_id"]): r for r in rows}
    assert by_entity[1]["name"] == "Alice@v2"
    assert by_entity[2]["name"] == "Bob"
