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
from tabsdatak._medallion.silver.scd import build_scd2_silver
from tabsdatak._medallion.constants import (
    MEDALLION_SYSTEM_COLUMN_BRONZE_TS,
    MEDALLION_SYSTEM_COLUMN_OP,
    MEDALLION_SYSTEM_COLUMN_PK,
    MEDALLION_SYSTEM_COLUMN_SOURCE_TS,
    MEDALLION_SYSTEM_COLUMN_VALID_FROM,
    MEDALLION_SYSTEM_COLUMN_VALID_TO,
)

pytestmark = pytest.mark.medallion

# ── Bronze metadata columns ──────────────────────────────────────────────────
M_OP = MEDALLION_SYSTEM_COLUMN_OP
M_SOURCE_TS = MEDALLION_SYSTEM_COLUMN_SOURCE_TS
M_BRONZE_TS = MEDALLION_SYSTEM_COLUMN_BRONZE_TS
M_PK = MEDALLION_SYSTEM_COLUMN_PK

# ── SCD2 row-shape columns ───────────────────────────────────────────────────
VALID_FROM = MEDALLION_SYSTEM_COLUMN_VALID_FROM
VALID_TO = MEDALLION_SYSTEM_COLUMN_VALID_TO

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
    Stamps the rest of the bronze metadata: @td.medallion.bronze_ts and @td.medallion.pk.
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
    """Build a T (erase) sentinel row. PKs are null; the only meaningful
    payload is the op marker and the source_ts."""
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


def make_valid_head(rows: list[tuple]) -> TableFrame:
    """Build a prior `_valid_raw` TableFrame.

    rows is a list of (tenant_id, entity_id, name, source_ts) tuples
    representing currently-active SCD2 rows (valid_to = None).
    """
    assert rows, "make_valid_head needs at least one row"
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
            VALID_FROM: [r[3] for r in rows],
            VALID_TO: [None] * n,
        }
    ).cast(
        {
            "tenant_id": td_dtypes.String,
            "entity_id": td_dtypes.Int64,
            "name": td_dtypes.String,
            M_OP: td_dtypes.String,
            # All-None VALID_TO would land as the Null dtype without this
            # cast, then clash with the helper's Datetime('μs', 'UTC')
            # ``valid_to`` during the bridge concat.
            VALID_TO: td_dtypes.Datetime(time_unit="us", time_zone="UTC"),
        }
    )


def make_expired_head(rows: list[tuple]) -> TableFrame:
    """Build a prior `_expired_raw` TableFrame.

    rows is a list of (tenant_id, entity_id, name, valid_from, valid_to)
    tuples representing already-closed SCD2 versions.
    """
    assert rows, "make_expired_head needs at least one row"
    n = len(rows)
    return TableFrame.from_dict(
        {
            "tenant_id": [r[0] for r in rows],
            "entity_id": [r[1] for r in rows],
            "name": [r[2] for r in rows],
            M_OP: [OP_UPDATE] * n,
            M_SOURCE_TS: [r[3] for r in rows],
            M_BRONZE_TS: [BRONZE_TS] * n,
            M_PK: [EXPECTED_PK_COLS] * n,
            VALID_FROM: [r[3] for r in rows],
            VALID_TO: [r[4] for r in rows],
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
    pdf = tf._to_pandas()
    keep = [c for c in pdf.columns if not c.startswith("$td.")]
    return pdf[keep].to_dict(orient="records")


def call(
    bronze: list[TableFrame],
    valid_head: TableFrame | None,
    expired_head: TableFrame | None = None,
) -> tuple[TableFrame, TableFrame]:
    return build_scd2_silver(
        bronze=bronze, valid_head=valid_head, expired_head=expired_head
    )


# ── First-fire tests ─────────────────────────────────────────────────────────


def test_first_fire_single_insert_lands_in_valid():
    """No prior silver, one I op → new_valid has the row with valid_to
    None; new_expired is empty."""
    bronze = [make_bronze_batch([("t1", 1, "Alice", OP_INSERT, TS_1)])]
    new_valid, new_expired = call(bronze, valid_head=None)

    valid_rows = to_rows(new_valid)
    expired_rows = to_rows(new_expired)
    assert len(valid_rows) == 1
    assert len(expired_rows) == 0
    assert valid_rows[0]["name"] == "Alice"
    assert valid_rows[0][VALID_FROM].to_pydatetime() == TS_1
    assert valid_rows[0][VALID_TO] is None or valid_rows[0][VALID_TO] != valid_rows[0][VALID_TO]
    # The above checks "is None or is NaT" — pandas surfaces null
    # Datetime cells as NaT, which is != itself.


def test_first_fire_empty_valid_head_treated_like_none():
    """An empty valid_head TableFrame behaves the same as None."""
    bronze = [make_bronze_batch([("t1", 1, "Alice", OP_INSERT, TS_1)])]
    empty_head = make_valid_head([("dummy", 999, "x", TS_PRIOR)]).filter(
        col("entity_id") < 0
    )
    new_valid, new_expired = call(bronze, valid_head=empty_head)

    assert len(to_rows(new_valid)) == 1
    assert len(to_rows(new_expired)) == 0


# ── Empty-bronze guard ──────────────────────────────────────────────────────


def test_empty_bronze_returns_both_heads_unchanged():
    """`bronze=[]` with non-empty `valid_head` and `expired_head` →
    both heads pass through unchanged. The helper does no work on an
    empty batch."""
    prior_valid = make_valid_head(
        [
            ("t1", 1, "Alice", TS_PRIOR),
            ("t1", 2, "Bob", TS_PRIOR),
        ]
    )
    prior_expired = make_expired_head(
        [("t1", 3, "Carol@v0", TS_PRIOR, TS_1)]
    )
    new_valid, new_expired = call(
        [], valid_head=prior_valid, expired_head=prior_expired
    )

    valid_rows = to_rows(new_valid)
    assert {r["name"] for r in valid_rows} == {"Alice", "Bob"}

    expired_rows = to_rows(new_expired)
    assert len(expired_rows) == 1
    assert expired_rows[0]["name"] == "Carol@v0"


def test_empty_bronze_with_none_expired_head_returns_none_expired():
    """`bronze=[]` + non-empty `valid_head` + `expired_head=None` →
    valid_head passes through; new_expired is None (the helper returns
    expired_head unchanged, even when it's None)."""
    prior_valid = make_valid_head([("t1", 1, "Alice", TS_PRIOR)])
    new_valid, new_expired = call([], valid_head=prior_valid, expired_head=None)

    assert len(to_rows(new_valid)) == 1
    assert new_expired is None


def test_empty_bronze_frames_treated_as_empty():
    """`bronze=[empty_frame]` (non-empty list but every frame is empty)
    behaves the same as `bronze=[]` — heads pass through unchanged."""
    prior_valid = make_valid_head([("t1", 1, "Alice", TS_PRIOR)])
    prior_expired = make_expired_head(
        [("t1", 2, "Bob@v0", TS_PRIOR, TS_1)]
    )
    empty_bronze = make_bronze_batch(
        [("t1", 99, "ghost", OP_INSERT, TS_1)]
    ).filter(col("entity_id") < 0)
    new_valid, new_expired = call(
        [empty_bronze], valid_head=prior_valid, expired_head=prior_expired
    )

    valid_rows = to_rows(new_valid)
    assert len(valid_rows) == 1
    assert valid_rows[0]["name"] == "Alice"

    expired_rows = to_rows(new_expired)
    assert len(expired_rows) == 1
    assert expired_rows[0]["name"] == "Bob@v0"


def test_empty_bronze_with_no_valid_head_raises():
    """Both bronze and valid_head empty → ValueError. The helper has
    no schema reference to fall back on, regardless of expired_head."""
    with pytest.raises(ValueError, match="bronze input is empty"):
        call([], valid_head=None, expired_head=None)


# ── Intra-batch transitions ──────────────────────────────────────────────────


def test_intra_batch_i_then_u_creates_one_expired_one_valid():
    """I then U for the same PK in one batch:
    - The I row gets closed by the U (intra_batch_expired).
    - The U row becomes the new valid version."""
    bronze = [
        make_bronze_batch(
            [
                ("t1", 1, "Alice@v1", OP_INSERT, TS_1),
                ("t1", 1, "Alice@v2", OP_UPDATE, TS_2),
            ]
        )
    ]
    new_valid, new_expired = call(bronze, valid_head=None)

    valid_rows = to_rows(new_valid)
    expired_rows = to_rows(new_expired)
    assert len(valid_rows) == 1
    assert valid_rows[0]["name"] == "Alice@v2"
    assert valid_rows[0][VALID_FROM].to_pydatetime() == TS_2

    assert len(expired_rows) == 1
    assert expired_rows[0]["name"] == "Alice@v1"
    assert expired_rows[0][VALID_FROM].to_pydatetime() == TS_1
    assert expired_rows[0][VALID_TO].to_pydatetime() == TS_2


def test_intra_batch_i_then_d_creates_one_expired_no_valid():
    """I then D for the same PK in one batch:
    - The I row gets closed by the D (intra_batch_expired with valid_to=D.source_ts).
    - No new valid row (D doesn't create a version)."""
    bronze = [
        make_bronze_batch(
            [
                ("t1", 1, "Alice", OP_INSERT, TS_1),
                ("t1", 1, "Alice", OP_DELETE, TS_2),
            ]
        )
    ]
    new_valid, new_expired = call(bronze, valid_head=None)

    assert len(to_rows(new_valid)) == 0
    expired_rows = to_rows(new_expired)
    assert len(expired_rows) == 1
    assert expired_rows[0][VALID_FROM].to_pydatetime() == TS_1
    assert expired_rows[0][VALID_TO].to_pydatetime() == TS_2


def test_intra_batch_i_u_u_chain_creates_two_expired_one_valid():
    """I → U → U for one PK: first two land in expired, last in valid."""
    bronze = [
        make_bronze_batch(
            [
                ("t1", 1, "v1", OP_INSERT, TS_1),
                ("t1", 1, "v2", OP_UPDATE, TS_2),
                ("t1", 1, "v3", OP_UPDATE, TS_3),
            ]
        )
    ]
    new_valid, new_expired = call(bronze, valid_head=None)

    valid_rows = to_rows(new_valid)
    expired_rows = to_rows(new_expired)
    assert len(valid_rows) == 1
    assert valid_rows[0]["name"] == "v3"
    assert valid_rows[0][VALID_FROM].to_pydatetime() == TS_3

    assert len(expired_rows) == 2
    expired_by_name = {r["name"]: r for r in expired_rows}
    assert expired_by_name["v1"][VALID_TO].to_pydatetime() == TS_2
    assert expired_by_name["v2"][VALID_TO].to_pydatetime() == TS_3


# ── Bridge tests (interaction with prior valid_head) ────────────────────────


def test_bridge_closes_prior_valid_when_u_lands():
    """Prior valid Alice exists; a U for Alice lands → the prior row
    moves to expired with valid_to=U.source_ts, the U row becomes the
    new valid."""
    prior = make_valid_head([("t1", 1, "Alice@v0", TS_PRIOR)])
    bronze = [make_bronze_batch([("t1", 1, "Alice@v1", OP_UPDATE, TS_1)])]
    new_valid, new_expired = call(bronze, valid_head=prior)

    valid_rows = to_rows(new_valid)
    expired_rows = to_rows(new_expired)
    assert len(valid_rows) == 1
    assert valid_rows[0]["name"] == "Alice@v1"
    assert valid_rows[0][VALID_FROM].to_pydatetime() == TS_1

    assert len(expired_rows) == 1
    assert expired_rows[0]["name"] == "Alice@v0"
    assert expired_rows[0][VALID_FROM].to_pydatetime() == TS_PRIOR
    assert expired_rows[0][VALID_TO].to_pydatetime() == TS_1


def test_bridge_closes_prior_valid_when_d_lands():
    """Prior valid Alice; D for Alice → prior moves to expired with
    valid_to=D.source_ts, no new valid row."""
    prior = make_valid_head([("t1", 1, "Alice@v0", TS_PRIOR)])
    bronze = [make_bronze_batch([("t1", 1, "Alice", OP_DELETE, TS_1)])]
    new_valid, new_expired = call(bronze, valid_head=prior)

    assert len(to_rows(new_valid)) == 0
    expired_rows = to_rows(new_expired)
    assert len(expired_rows) == 1
    assert expired_rows[0]["name"] == "Alice@v0"
    assert expired_rows[0][VALID_TO].to_pydatetime() == TS_1


def test_untouched_prior_rows_carry_through_to_valid():
    """Prior valid has Alice + Bob; new batch only touches Alice. Bob
    stays in new_valid (prior_carried)."""
    prior = make_valid_head(
        [
            ("t1", 1, "Alice@v0", TS_PRIOR),
            ("t1", 2, "Bob@v0", TS_PRIOR),
        ]
    )
    bronze = [make_bronze_batch([("t1", 1, "Alice@v1", OP_UPDATE, TS_1)])]
    new_valid, new_expired = call(bronze, valid_head=prior)

    valid_rows = to_rows(new_valid)
    assert len(valid_rows) == 2
    by_entity = {int(r["entity_id"]): r for r in valid_rows}
    # Bob untouched.
    assert by_entity[2]["name"] == "Bob@v0"
    assert by_entity[2][VALID_FROM].to_pydatetime() == TS_PRIOR
    # Alice updated.
    assert by_entity[1]["name"] == "Alice@v1"
    assert by_entity[1][VALID_FROM].to_pydatetime() == TS_1

    expired_rows = to_rows(new_expired)
    assert len(expired_rows) == 1
    assert expired_rows[0]["name"] == "Alice@v0"


def test_bridge_uses_first_op_source_ts_when_multiple_ops_on_same_pk():
    """Prior valid Alice + new batch has U Alice@v1, U Alice@v2.
    The prior row's valid_to = first batch op's source_ts (TS_1),
    NOT the latest."""
    prior = make_valid_head([("t1", 1, "Alice@v0", TS_PRIOR)])
    bronze = [
        make_bronze_batch(
            [
                ("t1", 1, "Alice@v1", OP_UPDATE, TS_1),
                ("t1", 1, "Alice@v2", OP_UPDATE, TS_2),
            ]
        )
    ]
    new_valid, new_expired = call(bronze, valid_head=prior)

    valid_rows = to_rows(new_valid)
    assert len(valid_rows) == 1
    assert valid_rows[0]["name"] == "Alice@v2"

    expired_rows = to_rows(new_expired)
    expired_by_name = {r["name"]: r for r in expired_rows}
    assert len(expired_by_name) == 2
    # Prior row closed at the FIRST batch op's source_ts.
    assert expired_by_name["Alice@v0"][VALID_TO].to_pydatetime() == TS_1
    # Intra-batch: v1 closed by v2 at TS_2.
    assert expired_by_name["Alice@v1"][VALID_TO].to_pydatetime() == TS_2


# ── T-sentinel tests ─────────────────────────────────────────────────────────


def test_t_sentinel_closes_all_prior_valid_into_expired():
    """T sentinel → all prior_valid rows move into expired_raw with
    @td.medallion.valid_to = T.source_ts. Post-T ops re-seed valid_raw from
    scratch. Nothing disappears."""
    prior = make_valid_head(
        [
            ("t1", 1, "Alice@v0", TS_PRIOR),
            ("t1", 2, "Bob@v0", TS_PRIOR),
        ]
    )
    bronze = [make_t_sentinel(TS_2), make_bronze_batch([("t1", 3, "Carol", OP_INSERT, TS_3)])]
    new_valid, new_expired = call(bronze, valid_head=prior)

    # New valid: only Carol (post-T).
    valid_rows = to_rows(new_valid)
    assert len(valid_rows) == 1
    assert valid_rows[0]["name"] == "Carol"
    assert int(valid_rows[0]["entity_id"]) == 3

    # Expired: Alice and Bob, both closed at the T's source_ts.
    expired_rows = to_rows(new_expired)
    assert len(expired_rows) == 2
    by_name = {r["name"]: r for r in expired_rows}
    assert set(by_name) == {"Alice@v0", "Bob@v0"}
    assert by_name["Alice@v0"][VALID_TO].to_pydatetime() == TS_2
    assert by_name["Bob@v0"][VALID_TO].to_pydatetime() == TS_2


def test_t_sentinel_drops_ops_before_cutoff():
    """Ops at-or-before the T's source_ts are dropped. Only ops with
    source_ts > T.source_ts are folded.

    The T sentinel and surrounding ops are inlined into a single bronze
    frame here. With multiple frames, synthetic test data has unmaterialized
    ``$td.src`` system-column values that clash during the diagonal vstack
    at collect time — a tabsdata test-data artifact, not a helper bug.
    Production commits all share a consistent ``$td.src``, so the multi-
    frame path is exercised in ``test_multiple_bronze_commits_diagonally_concatenated``.
    """
    bronze = [
        make_bronze_batch(
            [
                ("t1", 1, "before", OP_INSERT, TS_1),  # before T → dropped
                (None, None, None, OP_TOMBSTONE, TS_2),  # T sentinel
                ("t1", 2, "after", OP_INSERT, TS_3),  # after T → kept
            ]
        )
    ]
    new_valid, _ = call(bronze, valid_head=None)

    valid_rows = to_rows(new_valid)
    assert len(valid_rows) == 1
    assert valid_rows[0]["name"] == "after"
    assert int(valid_rows[0]["entity_id"]) == 2


def test_latest_t_takes_precedence():
    """Two T's in the batch — the LATEST T sets the cutoff."""
    bronze = [
        make_t_sentinel(TS_1),
        make_bronze_batch(
            [
                ("t1", 1, "between", OP_INSERT, TS_2),  # between T1 and T2 — dropped
            ]
        ),
        make_t_sentinel(TS_3),
        make_bronze_batch(
            [
                ("t1", 2, "after-T2", OP_INSERT, TS_4),  # after T2 — kept
            ]
        ),
    ]
    new_valid, _ = call(bronze, valid_head=None)

    valid_rows = to_rows(new_valid)
    assert len(valid_rows) == 1
    assert valid_rows[0]["name"] == "after-T2"


# ── Bronze-metadata flow-through ────────────────────────────────────────────


def test_bronze_metadata_rides_through_to_both_outputs():
    """`@td.medallion.op` / `@td.medallion.bronze_ts` / `@td.medallion.pk` / `@td.medallion.source_ts` survive on
    both new_valid and new_expired — stage-2 cleansing will drop them."""
    bronze = [
        make_bronze_batch(
            [
                ("t1", 1, "v1", OP_INSERT, TS_1),
                ("t1", 1, "v2", OP_UPDATE, TS_2),
            ]
        )
    ]
    new_valid, new_expired = call(bronze, valid_head=None)

    for tf, label in [(new_valid, "new_valid"), (new_expired, "new_expired")]:
        cols = user_cols(tf)
        assert M_OP in cols, f"{label}: `@td.medallion.op` missing — {cols!r}"
        assert M_BRONZE_TS in cols, f"{label}: `@td.medallion.bronze_ts` missing — {cols!r}"
        assert M_PK in cols, f"{label}: `@td.medallion.pk` missing — {cols!r}"
        assert M_SOURCE_TS in cols, f"{label}: `@td.medallion.source_ts` missing — {cols!r}"


def test_valid_from_set_from_source_ts_on_new_valid():
    """`valid_from` on new_valid = the row's own `@td.medallion.source_ts`."""
    bronze = [
        make_bronze_batch(
            [
                ("t1", 1, "Alice", OP_INSERT, TS_1),
                ("t1", 2, "Bob", OP_INSERT, TS_2),
            ]
        )
    ]
    new_valid, _ = call(bronze, valid_head=None)

    rows = to_rows(new_valid)
    by_entity = {int(r["entity_id"]): r for r in rows}
    assert by_entity[1][VALID_FROM].to_pydatetime() == TS_1
    assert by_entity[2][VALID_FROM].to_pydatetime() == TS_2


# ── Independence + composite key ────────────────────────────────────────────


def test_multiple_pks_processed_independently():
    """Ops on different PKs in the same batch don't cross-pollute."""
    bronze = [
        make_bronze_batch(
            [
                ("t1", 1, "Alice@v1", OP_INSERT, TS_1),
                ("t1", 2, "Bob@v1", OP_INSERT, TS_1),
                ("t1", 1, "Alice@v2", OP_UPDATE, TS_2),  # Alice updated
                ("t1", 2, "Bob@v1", OP_DELETE, TS_2),  # Bob deleted
            ]
        )
    ]
    new_valid, new_expired = call(bronze, valid_head=None)

    valid_rows = to_rows(new_valid)
    # Only Alice@v2 stays valid; Bob is deleted.
    assert len(valid_rows) == 1
    assert valid_rows[0]["name"] == "Alice@v2"

    # Two expired: Alice@v1 (closed by U at TS_2) and Bob@v1 (closed
    # by D at TS_2).
    expired_by_name = {r["name"]: r for r in to_rows(new_expired)}
    assert set(expired_by_name) == {"Alice@v1", "Bob@v1"}
    assert expired_by_name["Alice@v1"][VALID_TO].to_pydatetime() == TS_2
    assert expired_by_name["Bob@v1"][VALID_TO].to_pydatetime() == TS_2


def test_multiple_bronze_commits_diagonally_concatenated():
    """`bronze` as a list of multiple TableFrames → all commits folded."""
    a = make_bronze_batch([("t1", 1, "Alice", OP_INSERT, TS_1)])
    b = make_bronze_batch([("t1", 2, "Bob", OP_INSERT, TS_2)])
    c = make_bronze_batch([("t1", 1, "Alice@new", OP_UPDATE, TS_3)])
    new_valid, new_expired = call([a, b, c], valid_head=None)

    valid_rows = to_rows(new_valid)
    valid_by_entity = {int(r["entity_id"]): r for r in valid_rows}
    assert valid_by_entity[1]["name"] == "Alice@new"
    assert valid_by_entity[2]["name"] == "Bob"

    expired_rows = to_rows(new_expired)
    assert len(expired_rows) == 1
    assert expired_rows[0]["name"] == "Alice"
    assert expired_rows[0][VALID_TO].to_pydatetime() == TS_3
