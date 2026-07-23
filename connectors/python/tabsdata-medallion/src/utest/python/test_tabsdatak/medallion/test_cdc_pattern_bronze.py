#
# Copyright 2026 Tabsdata Inc.
#

from datetime import datetime, timezone

import pytest

import tabsdatak.tableframe.datatypes as td_dtypes
from tabsdatak.tableframe import TableFrame
from tabsdatak._medallion.bronze.cdc import (
    OP_DELETE,
    OP_INSERT,
    OP_TOMBSTONE,
    OP_UPDATE,
    build_cdc_bronze,
)

pytestmark = pytest.mark.medallion

# ── Metadata column names ────────────────────────────────────────────────────
# Output metadata columns produced by build_cdc_bronze.
M_OP = "$m.op"
M_SOURCE_TS = "$m.source_ts"
M_BRONZE_TS = "$m.bronze_ts"
M_PK = "$m.pk"

# Input cdc metadata columns (what the upstream CDC trigger emits, before
# build_cdc_bronze renames them into the $m.* namespace).
IN_OP = "@td..cdc.meta.op"
IN_SOURCE_TS = "@td..cdc.meta.ts"

# Composite primary key used in these tests.
PK = [
    {"tenant_id": td_dtypes.String},
    {"entity_id": td_dtypes.Int64},
]
EXPECTED_PK_COLS = ["tenant_id", "entity_id"]


# ── Helpers ──────────────────────────────────────────────────────────────────

# Timestamps well before / after ``datetime.now(tz=timezone.utc)`` so the
# bootstrap filter (stream.$m.source_change_ts > bootstrap_ts) is
# deterministic. UTC-aware on both sides — ``build_cdc_bronze`` now stamps
# all timestamps with ``tzinfo=UTC``, and Polars refuses to compare
# tz-aware against tz-naive datetimes.
TS_PAST = datetime(2000, 1, 1, 12, 0, 0, tzinfo=timezone.utc)
TS_FUTURE = datetime(2099, 1, 1, 12, 0, 0, tzinfo=timezone.utc)


def make_cdc(
    tenants: list[str],
    entities: list[int],
    ops: list[str],
    source_ts: list[datetime],
    extra: dict[str, list] | None = None,
) -> TableFrame:
    """Build a synthetic cdc commit-table.

    Uses the raw upstream column names (`@td..cdc.meta.*`) — the helper renames
    them into the `$m.*` namespace internally.
    """
    data = {
        "tenant_id": tenants,
        "entity_id": entities,
        IN_OP: ops,
        IN_SOURCE_TS: source_ts,
    }
    if extra:
        data.update(extra)
    return TableFrame.from_dict(data)


def make_full(tenants: list[str], entities: list[int]) -> TableFrame:
    """Build a synthetic full-snapshot commit-table matching the source MySQL schema:

        sim_counter BIGINT     PK
        tenant_id   VARCHAR(20) NOT NULL
        entity_id   BIGINT     NOT NULL
        data_ts     DATETIME   NOT NULL
        name        VARCHAR(80)
        status      VARCHAR(20)
        category    VARCHAR(20)
        amount      DECIMAL(12,2)
        quantity    INT
        score       DOUBLE
        is_active   TINYINT(1) NOT NULL

    The helper only reads `tenant_id` and `entity_id` from `full`, but populating
    the full schema keeps the test fixtures realistic.
    """
    n = len(tenants)
    assert len(entities) == n, "tenants and entities must be the same length"
    return TableFrame.from_dict(
        {
            "sim_counter": list(range(1, n + 1)),
            "tenant_id": tenants,
            "entity_id": entities,
            "data_ts": [TS_PAST] * n,
            "name": [f"name-{i}" for i in range(n)],
            "status": ["active"] * n,
            "category": ["cat-A"] * n,
            "amount": [100.50 + i for i in range(n)],
            "quantity": [10 + i for i in range(n)],
            "score": [0.5 + i * 0.1 for i in range(n)],
            "is_active": [1] * n,
        }
    )


def user_cols(tf: TableFrame) -> list[str]:
    """Drop tabsdata system columns ($td.*) so we can assert on user columns."""
    return [c for c in tf.columns if not c.startswith("$td.")]


def to_rows(tf: TableFrame) -> list[dict]:
    """Return rows as list-of-dicts with system columns stripped."""
    pdf = tf.to_pandas()
    keep = [c for c in pdf.columns if not c.startswith("$td.")]
    return pdf[keep].to_dict(orient="records")


def call(full_commits, cdc_commits) -> TableFrame:
    return build_cdc_bronze(full_commits, cdc_commits, pk=PK)


# ── Tests ────────────────────────────────────────────────────────────────────


def test_pure_incremental_returns_stream_with_bronze_ts():
    """Branch: cdc has rows, no full_commits → return stream directly.

    The cdc columns pass through as-is; a $m.bronze_ts column is appended.
    """
    cdc = make_cdc(
        tenants=["t1", "t2"],
        entities=[10, 20],
        ops=[OP_INSERT, OP_UPDATE],
        source_ts=[TS_PAST, TS_FUTURE],
    )
    out = call(full_commits=[], cdc_commits=[cdc])

    cols = user_cols(out)
    assert M_OP in cols
    assert M_SOURCE_TS in cols
    assert M_BRONZE_TS in cols  # added by the helper

    rows = to_rows(out)
    assert len(rows) == 2
    assert {r["tenant_id"] for r in rows} == {"t1", "t2"}
    assert {r[M_OP] for r in rows} == {OP_INSERT, OP_UPDATE}


def test_empty_cdc_no_full_returns_empty_stream():
    """Branch: cdc empty AND no full_commits → empty fallback, empty result."""
    out = call(full_commits=[], cdc_commits=[])
    assert len(to_rows(out)) == 0
    cols = user_cols(out)
    assert M_OP in cols
    assert M_SOURCE_TS in cols
    assert M_BRONZE_TS in cols


def test_bootstrap_no_cdc_emits_erase_and_seed():
    """Branch: full_commits present, no cdc → erase + seed rows only."""
    full = make_full(tenants=["a", "b"], entities=[1, 2])
    out = call(full_commits=[full], cdc_commits=[])

    rows = to_rows(out)
    # One erase sentinel + one seed row per bootstrap entity.
    assert len(rows) == 1 + 2

    ops = [r[M_OP] for r in rows]
    assert ops.count(OP_TOMBSTONE) == 1  # erase
    assert ops.count(OP_INSERT) == 2  # seed

    import pandas as pd

    erase = [r for r in rows if r[M_OP] == OP_TOMBSTONE][0]
    assert pd.isna(erase["tenant_id"])
    assert pd.isna(erase["entity_id"])

    seed_keys = {(r["tenant_id"], int(r["entity_id"])) for r in rows if r[M_OP] == OP_INSERT}
    assert seed_keys == {("a", 1), ("b", 2)}


def test_bootstrap_plus_old_cdc_filters_out_cdc():
    """Branch: cdc source_ts strictly older than bootstrap_ts → all cdc filtered."""
    full = make_full(tenants=["a"], entities=[1])
    cdc = make_cdc(
        tenants=["a"],
        entities=[1],
        ops=[OP_UPDATE],
        source_ts=[TS_PAST],  # before bootstrap_ts (datetime.now)
    )
    out = call(full_commits=[full], cdc_commits=[cdc])

    rows = to_rows(out)
    # Only erase + 1 seed row; the old cdc row is dropped.
    assert len(rows) == 2
    assert sorted(r[M_OP] for r in rows) == sorted([OP_INSERT, OP_TOMBSTONE])


def test_bootstrap_plus_new_cdc_keeps_cdc():
    """Branch: cdc source_ts strictly newer than bootstrap_ts → all cdc kept."""
    full = make_full(tenants=["a"], entities=[1])
    cdc = make_cdc(
        tenants=["a", "a"],
        entities=[1, 2],
        ops=[OP_UPDATE, OP_DELETE],
        source_ts=[TS_FUTURE, TS_FUTURE],
    )
    out = call(full_commits=[full], cdc_commits=[cdc])

    rows = to_rows(out)
    # erase (1) + seed (1) + cdc kept (2) = 4
    assert len(rows) == 4
    ops = sorted(r[M_OP] for r in rows)
    assert ops == sorted([OP_DELETE, OP_INSERT, OP_TOMBSTONE, OP_UPDATE])


def test_bootstrap_plus_mixed_cdc_keeps_only_newer():
    """Branch: cdc has both pre- and post-bootstrap rows → only newer ones survive."""
    full = make_full(tenants=["a"], entities=[1])
    cdc = make_cdc(
        tenants=["t-old", "t-new1", "t-new2"],
        entities=[10, 20, 30],
        ops=[OP_UPDATE, OP_UPDATE, OP_DELETE],
        source_ts=[TS_PAST, TS_FUTURE, TS_FUTURE],
    )
    out = call(full_commits=[full], cdc_commits=[cdc])

    rows = to_rows(out)
    # erase (1) + seed (1) + cdc kept (2) = 4
    assert len(rows) == 4
    tenants_in_cdc_rows = {r["tenant_id"] for r in rows if r[M_OP] in (OP_UPDATE, OP_DELETE)}
    assert tenants_in_cdc_rows == {"t-new1", "t-new2"}


def test_only_last_full_commit_is_used_as_bootstrap():
    """Branch: multiple full_commits → ONLY full_commits[-1] feeds the seed."""
    older_full = make_full(tenants=["older"], entities=[99])
    latest_full = make_full(tenants=["latest1", "latest2"], entities=[1, 2])

    out = call(full_commits=[older_full, latest_full], cdc_commits=[])

    rows = to_rows(out)
    seed_rows = [r for r in rows if r[M_OP] == OP_INSERT]
    seed_keys = {(r["tenant_id"], int(r["entity_id"])) for r in seed_rows}
    assert seed_keys == {("latest1", 1), ("latest2", 2)}
    # The older snapshot's tenant must NOT appear in the seed.
    assert all(r["tenant_id"] != "older" for r in seed_rows)


def test_multiple_cdc_commits_are_concatenated():
    """Branch: cdc_commits has multiple frames → diagonal concat preserves rows from all."""
    cdc_a = make_cdc(["t1"], [1], [OP_INSERT], [TS_FUTURE])
    cdc_b = make_cdc(["t2"], [2], [OP_UPDATE], [TS_FUTURE])
    cdc_c = make_cdc(["t3"], [3], [OP_DELETE], [TS_FUTURE])

    out = call(full_commits=[], cdc_commits=[cdc_a, cdc_b, cdc_c])

    rows = to_rows(out)
    assert len(rows) == 3
    assert {r["tenant_id"] for r in rows} == {"t1", "t2", "t3"}


def test_full_snapshot_columns_flow_through_to_seed():
    """All snapshot columns (data_ts, name, status, amount, ...) appear on seed rows."""
    full = make_full(tenants=["a"], entities=[1])
    out = call(full_commits=[full], cdc_commits=[])

    cols = user_cols(out)
    for expected in [
        "sim_counter",
        "tenant_id",
        "entity_id",
        "data_ts",
        "name",
        "status",
        "category",
        "amount",
        "quantity",
        "score",
        "is_active",
    ]:
        assert expected in cols, f"missing snapshot column on output: {expected}"

    rows = to_rows(out)
    seed_row = [r for r in rows if r[M_OP] == OP_INSERT][0]
    assert seed_row["tenant_id"] == "a"
    assert int(seed_row["entity_id"]) == 1
    assert seed_row["name"] == "name-0"
    assert seed_row["status"] == "active"
    assert seed_row["category"] == "cat-A"
    assert float(seed_row["amount"]) == 100.50
    assert int(seed_row["quantity"]) == 10
    assert float(seed_row["score"]) == 0.5
    assert int(seed_row["is_active"]) == 1

    # And the erase row should have nulls for the snapshot-only columns
    # (diagonal concat fills them).
    import pandas as pd

    erase_row = [r for r in rows if r[M_OP] == OP_TOMBSTONE][0]
    snapshot_only_cols = [
        "sim_counter",
        "data_ts",
        "name",
        "status",
        "category",
        "amount",
        "quantity",
        "score",
        "is_active",
    ]
    for snapshot_only in snapshot_only_cols:
        v = erase_row[snapshot_only]
        assert pd.isna(v), f"erase row should have null {snapshot_only}, got {v!r}"


def test_pk_meta_column_present_on_every_row():
    """`$m.pk` should appear on every row across all branches with the
    composite-key column names from the `pk` arg passed to the helper."""
    full = make_full(tenants=["a"], entities=[1])
    cdc = make_cdc(
        tenants=["t1", "t2"],
        entities=[1, 2],
        ops=[OP_UPDATE, OP_DELETE],
        source_ts=[TS_FUTURE, TS_FUTURE],
    )
    out = call(full_commits=[full], cdc_commits=[cdc])

    cols = user_cols(out)
    assert M_PK in cols

    rows = to_rows(out)
    # erase (1) + seed (1) + cdc kept (2) = 4 — covers every branch.
    assert len(rows) == 4
    for r in rows:
        # pandas surfaces a list-typed cell as a Python list / numpy array; coerce.
        assert (
            list(r[M_PK]) == EXPECTED_PK_COLS
        ), f"row with op={r[M_OP]!r} has pk={r[M_PK]!r}, expected {EXPECTED_PK_COLS!r}"


def test_pk_meta_column_on_empty_fallback():
    """Even on the empty fallback (no cdc, no full), the $m.pk column must be
    declared on the schema so downstream consumers can rely on it."""
    out = call(full_commits=[], cdc_commits=[])
    assert M_PK in user_cols(out)


def test_cdc_data_payload_columns_flow_through():
    """All @td..cdc.data.* (and other) cdc columns flow through to the output."""
    cdc = make_cdc(
        tenants=["t1"],
        entities=[1],
        ops=[OP_UPDATE],
        source_ts=[TS_FUTURE],
        extra={
            "@td..cdc.data.amount": [42.5],
            "@td..cdc.data.note": ["hello"],
        },
    )
    out = call(full_commits=[], cdc_commits=[cdc])

    cols = user_cols(out)
    assert "@td..cdc.data.amount" in cols
    assert "@td..cdc.data.note" in cols

    rows = to_rows(out)
    assert rows[0]["@td..cdc.data.amount"] == 42.5
    assert rows[0]["@td..cdc.data.note"] == "hello"


def test_cdc_data_col_new_prefix_is_stripped():
    """Columns named `@td..cdc.data.col.new.<name>` are renamed to plain `<name>`.

    These are the post-image payload columns the publisher emits for each
    tracked source column; downstream consumers expect them as bare names.
    Other `@td..cdc.data.*` columns (without the `.col.new.` infix) are
    preserved as-is.
    """
    cdc = make_cdc(
        tenants=["t1"],
        entities=[1],
        ops=[OP_UPDATE],
        source_ts=[TS_FUTURE],
        extra={
            "@td..cdc.data.col.new.amount": [42.5],
            "@td..cdc.data.col.new.status": ["active"],
            # A sibling @td..cdc.data.* column without the .col.new. infix —
            # must NOT be renamed (negative control).
            "@td..cdc.data.other": ["preserved"],
        },
    )
    out = call(full_commits=[], cdc_commits=[cdc])

    cols = user_cols(out)
    # Prefixed payload columns are renamed to bare names.
    assert "amount" in cols
    assert "status" in cols
    # And the prefixed forms are gone.
    assert "@td..cdc.data.col.new.amount" not in cols
    assert "@td..cdc.data.col.new.status" not in cols
    # Sibling @td..cdc.data.* column without the prefix is untouched.
    assert "@td..cdc.data.other" in cols

    rows = to_rows(out)
    assert rows[0]["amount"] == 42.5
    assert rows[0]["status"] == "active"
    assert rows[0]["@td..cdc.data.other"] == "preserved"


def test_cdc_data_col_old_prefix_is_dropped():
    """Columns named `@td..cdc.data.col.old.<name>` are pre-image payloads and
    must be dropped — bronze is post-image only.

    Mixed with `.col.new.*` (renamed) and a sibling `@td..cdc.data.*` column
    (preserved) to verify the three behaviors don't interfere.
    """
    cdc = make_cdc(
        tenants=["t1"],
        entities=[1],
        ops=[OP_UPDATE],
        source_ts=[TS_FUTURE],
        extra={
            "@td..cdc.data.col.old.amount": [10.0],
            "@td..cdc.data.col.old.status": ["inactive"],
            "@td..cdc.data.col.new.amount": [42.5],
            "@td..cdc.data.col.new.status": ["active"],
            "@td..cdc.data.other": ["preserved"],
        },
    )
    out = call(full_commits=[], cdc_commits=[cdc])

    cols = user_cols(out)
    # Pre-image columns are gone.
    assert "@td..cdc.data.col.old.amount" not in cols
    assert "@td..cdc.data.col.old.status" not in cols
    # Post-image columns landed as bare names.
    assert "amount" in cols
    assert "status" in cols
    # Sibling @td..cdc.data.* column is untouched.
    assert "@td..cdc.data.other" in cols

    rows = to_rows(out)
    # The retained `amount` / `status` values are the post-image ones.
    assert rows[0]["amount"] == 42.5
    assert rows[0]["status"] == "active"
    assert rows[0]["@td..cdc.data.other"] == "preserved"
