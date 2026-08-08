#
# Copyright 2026 Tabsdata Inc.
#

from datetime import datetime, timezone

import pytest

import tabsdatak.tableframe.datatypes as td_dtypes
from tabsdatak.tableframe import TableFrame
from tabsdatak.tableframe.functions import col, lit
from tabsdatak._medallion.silver.append_only import build_append_only_silver

pytestmark = pytest.mark.medallion

# ── Bronze input metadata columns (build_append_only_cdc_bronze output) ─────
M_OP = "$m.op"
M_SOURCE_TS = "$m.source_ts"
M_BRONZE_TS = "$m.bronze_ts"
M_PK = "$m.pk"
# ── Silver output metadata column ────────────────────────────────────────────
M_SILVER_TS = "$m.silver_ts"

# Composite primary key used in these tests.
PK = [
    {"tenant_id": td_dtypes.String},
    {"entity_id": td_dtypes.Int64},
]
EXPECTED_PK_COLS = ["tenant_id", "entity_id"]

# UTC-aware sample timestamps.
TS_A = datetime(2024, 1, 1, 12, 0, 0, tzinfo=timezone.utc)
TS_B = datetime(2024, 1, 2, 12, 0, 0, tzinfo=timezone.utc)
TS_C = datetime(2024, 1, 3, 12, 0, 0, tzinfo=timezone.utc)
BRONZE_TS = datetime(2024, 6, 1, 0, 0, 0, tzinfo=timezone.utc)


# ── Helpers ──────────────────────────────────────────────────────────────────


def make_bronze_batch(rows: list[tuple]) -> TableFrame:
    """Build a bronze-shaped append-only TableFrame (the kind
    `build_append_only_cdc_bronze` emits).

    rows is a list of (tenant_id, entity_id, payload, source_ts) tuples.
    Stamps all bronze metadata: `$m.op` = "I", `$m.bronze_ts` =
    BRONZE_TS, `$m.pk` = ["tenant_id", "entity_id"].
    """
    assert rows, "make_bronze_batch needs at least one row"
    n = len(rows)
    return TableFrame.from_dict(
        {
            "tenant_id": [r[0] for r in rows],
            "entity_id": [r[1] for r in rows],
            "payload": [r[2] for r in rows],
            M_OP: ["I"] * n,
            M_SOURCE_TS: [r[3] for r in rows],
            M_BRONZE_TS: [BRONZE_TS] * n,
            M_PK: [EXPECTED_PK_COLS] * n,
        }
    ).cast(
        {
            "tenant_id": td_dtypes.String,
            "entity_id": td_dtypes.Int64,
            "payload": td_dtypes.String,
            M_OP: td_dtypes.String,
        }
    )


def make_silver_head(rows: list[tuple]) -> TableFrame:
    """Build a silver-shaped TableFrame — the prior committed silver
    state the helper anti-joins against.

    rows is a list of (tenant_id, entity_id, payload, source_ts)
    tuples. Silver carries `$m.source_ts` but not `$m.op`, `$m.bronze_ts`,
    or `$m.pk` (those are dropped during the bronze → silver step).
    """
    assert rows, "make_silver_head needs at least one row"
    return TableFrame.from_dict(
        {
            "tenant_id": [r[0] for r in rows],
            "entity_id": [r[1] for r in rows],
            "payload": [r[2] for r in rows],
            M_SOURCE_TS: [r[3] for r in rows],
        }
    ).cast(
        {
            "tenant_id": td_dtypes.String,
            "entity_id": td_dtypes.Int64,
            "payload": td_dtypes.String,
        }
    )


def user_cols(tf: TableFrame) -> list[str]:
    """Drop tabsdata system columns ($td.*) so we can assert on user cols."""
    return [c for c in tf.columns if not c.startswith("$td.")]


def to_rows(tf: TableFrame) -> list[dict]:
    """Return rows as list-of-dicts with system columns stripped."""
    pdf = tf._to_pandas()
    keep = [c for c in pdf.columns if not c.startswith("$td.")]
    return pdf[keep].to_dict(orient="records")


def call(
    bronze_new: list[TableFrame],
    silver_head: TableFrame | None,
) -> TableFrame:
    return build_append_only_silver(
        bronze_new=bronze_new, silver_head=silver_head
    )


def _assert_utc_aware(ts, label: str) -> None:
    """Pin the contract that timestamps are UTC-aware. A regression
    to naive timestamps trips ``tzinfo is None``; a non-UTC offset
    trips the offset comparison.
    """
    assert ts is not None, f"{label} missing"
    assert ts.tzinfo is not None, f"{label} should be UTC-aware, got naive: {ts!r}"
    assert ts.utcoffset() == timezone.utc.utcoffset(None), (
        f"{label} should be in UTC; got offset {ts.utcoffset()!r}"
    )


# ── Tests ────────────────────────────────────────────────────────────────────


def test_first_fire_silver_head_none_passes_cleaned_rows_through():
    """No prior silver → emit cleaned bronze rows as-is (still
    intra-batch deduped). The headline case for the first-ever silver
    fire after onboarding."""
    bronze = [
        make_bronze_batch(
            [
                ("t1", 1, "hello", TS_A),
                ("t1", 2, "world", TS_B),
            ]
        )
    ]
    out = call(bronze, silver_head=None)

    rows = to_rows(out)
    assert len(rows) == 2
    keys = {(r["tenant_id"], int(r["entity_id"])) for r in rows}
    assert keys == {("t1", 1), ("t1", 2)}


def test_first_fire_empty_silver_head_treated_like_none():
    """An empty silver_head TableFrame is handled the same as None —
    no anti-join, all bronze rows pass through after intra-batch dedup."""
    bronze = [make_bronze_batch([("t1", 1, "hello", TS_A)])]
    empty_silver = make_silver_head([("dummy", 999, "x", TS_C)]).filter(
        col("entity_id") < 0  # produces an empty TableFrame
    )
    out = call(bronze, silver_head=empty_silver)

    rows = to_rows(out)
    assert len(rows) == 1
    assert rows[0]["tenant_id"] == "t1" and int(rows[0]["entity_id"]) == 1


def test_intra_batch_dedup_within_single_commit():
    """The same PK appearing twice in ONE bronze commit collapses to
    one row in silver."""
    bronze = [
        make_bronze_batch(
            [
                ("t1", 1, "v1", TS_A),
                ("t1", 1, "v1-dup", TS_A),  # same PK as previous row
                ("t1", 2, "v2", TS_B),
            ]
        )
    ]
    out = call(bronze, silver_head=None)

    rows = to_rows(out)
    assert len(rows) == 2
    keys = {(r["tenant_id"], int(r["entity_id"])) for r in rows}
    assert keys == {("t1", 1), ("t1", 2)}


def test_intra_batch_dedup_across_multiple_commits():
    """At-least-once delivery: the same PK can land in two bronze
    commits that get folded into one silver fire. Dedup catches it."""
    a = make_bronze_batch([("t1", 1, "from-a", TS_A)])
    b = make_bronze_batch([("t1", 1, "from-b", TS_A)])  # same PK as a
    out = call([a, b], silver_head=None)

    rows = to_rows(out)
    assert len(rows) == 1
    assert rows[0]["tenant_id"] == "t1" and int(rows[0]["entity_id"]) == 1


def test_cross_commit_dedup_drops_pks_already_in_silver():
    """PKs already written to silver are anti-joined out of the
    incoming batch — the headline cross-commit-dup case."""
    silver = make_silver_head(
        [
            ("t1", 1, "seen-1", TS_A),
            ("t1", 2, "seen-2", TS_B),
        ]
    )
    bronze = [
        make_bronze_batch(
            [
                ("t1", 1, "again-1", TS_A),  # already in silver → drop
                ("t1", 3, "new-3", TS_C),  # new → keep
                ("t1", 2, "again-2", TS_B),  # already in silver → drop
            ]
        )
    ]
    out = call(bronze, silver_head=silver)

    rows = to_rows(out)
    assert len(rows) == 1
    assert rows[0]["tenant_id"] == "t1" and int(rows[0]["entity_id"]) == 3


def test_composite_pk_dedup_requires_all_pk_cols_to_match():
    """The dedup key is the composite PK — matching only ONE column
    of the PK is NOT a dup. A row with (t1, 99) is distinct from any
    silver row with tenant_id=t1 and entity_id≠99."""
    silver = make_silver_head([("t1", 1, "seen", TS_A)])
    bronze = [
        make_bronze_batch(
            [
                ("t1", 99, "new-shares-tenant", TS_B),
                ("t2", 1, "new-shares-entity", TS_B),
            ]
        )
    ]
    out = call(bronze, silver_head=silver)

    rows = to_rows(out)
    assert len(rows) == 2
    keys = {(r["tenant_id"], int(r["entity_id"])) for r in rows}
    assert keys == {("t1", 99), ("t2", 1)}


def test_bronze_metadata_columns_dropped_from_output():
    """`$m.op` and `$m.bronze_ts` are dropped; `$m.source_ts` and
    `$m.pk` are preserved on the output. The helper does NOT stamp
    `$m.silver_ts`."""
    bronze = [make_bronze_batch([("t1", 1, "v", TS_A)])]
    out = call(bronze, silver_head=None)

    cols = user_cols(out)
    assert M_OP not in cols, f"`$m.op` should be dropped; got {cols!r}"
    assert M_BRONZE_TS not in cols, f"`$m.bronze_ts` should be dropped; got {cols!r}"
    assert M_SOURCE_TS in cols, (
        f"`$m.source_ts` should be preserved; got {cols!r}"
    )
    assert M_PK in cols, f"`$m.pk` should be preserved; got {cols!r}"
    assert M_SILVER_TS not in cols, (
        f"`$m.silver_ts` should NOT be added by the helper; got {cols!r}"
    )


def test_source_ts_value_preserved_on_output():
    """`$m.source_ts` flows through with its UTC-aware value intact."""
    bronze = [
        make_bronze_batch(
            [
                ("t1", 1, "a", TS_A),
                ("t1", 2, "b", TS_B),
            ]
        )
    ]
    out = call(bronze, silver_head=None)

    rows = to_rows(out)
    by_entity = {int(r["entity_id"]): r for r in rows}
    assert by_entity[1][M_SOURCE_TS].to_pydatetime() == TS_A
    assert by_entity[2][M_SOURCE_TS].to_pydatetime() == TS_B


def test_pk_column_values_flow_through():
    """PK column values are preserved on output."""
    bronze = [
        make_bronze_batch(
            [
                ("t1", 1, "p1", TS_A),
                ("t2", 7, "p2", TS_B),
            ]
        )
    ]
    out = call(bronze, silver_head=None)

    rows = to_rows(out)
    keys = {(r["tenant_id"], int(r["entity_id"])) for r in rows}
    assert keys == {("t1", 1), ("t2", 7)}


def test_non_pk_value_columns_flow_through():
    """Non-PK value columns (e.g. payload) flow through unchanged."""
    bronze = [make_bronze_batch([("t1", 1, "the-payload", TS_A)])]
    out = call(bronze, silver_head=None)

    cols = user_cols(out)
    assert "payload" in cols
    rows = to_rows(out)
    assert rows[0]["payload"] == "the-payload"


def test_multiple_bronze_commits_diagonally_concatenated():
    """`bronze_new` as a list of multiple TableFrames → diagonal
    concat preserves rows from all batches (modulo dedup)."""
    a = make_bronze_batch([("t1", 1, "a1", TS_A)])
    b = make_bronze_batch(
        [
            ("t1", 2, "b2", TS_B),
            ("t1", 3, "b3", TS_B),
        ]
    )
    c = make_bronze_batch([("t2", 9, "c9", TS_C)])
    out = call([a, b, c], silver_head=None)

    rows = to_rows(out)
    assert len(rows) == 4
    keys = {(r["tenant_id"], int(r["entity_id"])) for r in rows}
    assert keys == {("t1", 1), ("t1", 2), ("t1", 3), ("t2", 9)}


def test_single_pk_dedup_works():
    """A bronze frame whose `$m.pk` stamps a one-element key list
    drives a single-column dedup."""
    bronze = [
        TableFrame.from_dict(
            {
                "entity_id": [1, 2, 1],  # 1 is duplicated
                "payload": ["a", "b", "a-dup"],
                M_OP: ["I", "I", "I"],
                M_SOURCE_TS: [TS_A, TS_B, TS_A],
                M_BRONZE_TS: [BRONZE_TS, BRONZE_TS, BRONZE_TS],
                M_PK: [["entity_id"], ["entity_id"], ["entity_id"]],
            }
        ).cast(
            {
                "entity_id": td_dtypes.Int64,
                "payload": td_dtypes.String,
                M_OP: td_dtypes.String,
            }
        )
    ]
    out = build_append_only_silver(bronze_new=bronze, silver_head=None)

    rows = to_rows(out)
    assert len(rows) == 2
    keys = {int(r["entity_id"]) for r in rows}
    assert keys == {1, 2}


def test_cross_commit_dedup_combined_with_intra_batch_dedup():
    """Two layers of dedup compose: an incoming batch with intra-batch
    dups AND PKs that already exist in silver — output is just the
    new, deduped rows."""
    silver = make_silver_head([("t1", 1, "seen", TS_A)])
    bronze = [
        make_bronze_batch(
            [
                ("t1", 1, "again-1", TS_A),  # in silver → drop
                ("t1", 2, "new-2-v1", TS_B),  # new
                ("t1", 2, "new-2-v2", TS_B),  # dup of previous row
                ("t1", 1, "again-1-dup", TS_A),  # in silver, also intra-batch dup
            ]
        )
    ]
    out = call(bronze, silver_head=silver)

    rows = to_rows(out)
    assert len(rows) == 1
    assert rows[0]["tenant_id"] == "t1" and int(rows[0]["entity_id"]) == 2


def test_silver_head_with_extra_columns_doesnt_confuse_dedup():
    """The helper projects `silver_head` to just the PK cols before
    the anti-join, so extra columns on silver_head (e.g. derived
    columns added by downstream cleansing) don't change the dedup."""
    silver = make_silver_head([("t1", 1, "seen", TS_A)]).with_columns(
        lit("enriched").alias("extra_silver_col")
    )
    bronze = [
        make_bronze_batch(
            [
                ("t1", 1, "again", TS_A),  # in silver → drop
                ("t1", 2, "new", TS_B),
            ]
        )
    ]
    out = call(bronze, silver_head=silver)

    rows = to_rows(out)
    assert len(rows) == 1
    assert int(rows[0]["entity_id"]) == 2
    # The silver_head's extra column should NOT leak into the helper's
    # output — output schema is driven by bronze.
    assert "extra_silver_col" not in user_cols(out)


# ─────────────────────────────────────────────────────────────────────────────
# No bronze data at all → None
# ─────────────────────────────────────────────────────────────────────────────


@pytest.mark.parametrize(
    "bronze_new",
    [
        pytest.param([], id="empty_list"),
        pytest.param(None, id="none"),
        pytest.param([None, None], id="all_none_entries"),
    ],
)
@pytest.mark.parametrize("with_silver", [False, True], ids=["no_silver", "with_silver"])
def test_no_bronze_data_returns_none(bronze_new, with_silver):
    """No bronze data → `None`. When `bronze_new` concatenates to nothing —
    an empty list, `None`, or all-`None` entries — `concat` yields `None`
    and the helper returns `None`. This early return fires BEFORE
    `silver_head` is consulted, so it returns `None` whether or not a prior
    silver state exists (the 'nothing new to append this fire' path)."""
    silver_head = make_silver_head([("t1", 1, "seen", TS_A)]) if with_silver else None
    out = call(bronze_new=bronze_new, silver_head=silver_head)
    assert out is None
