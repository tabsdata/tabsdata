#
# Copyright 2026 Tabsdata Inc.
#

from datetime import datetime, timezone

import pytest

import tabsdatak.tableframe.datatypes as td_dtypes
from tabsdatak.tableframe import TableFrame
from tabsdatak._medallion.silver.scd import (
from tabsdatak._medallion.constants import (
    MEDALLION_SYSTEM_COLUMN_BRONZE_TS,
    MEDALLION_SYSTEM_COLUMN_OP,
    MEDALLION_SYSTEM_COLUMN_PK,
    MEDALLION_SYSTEM_COLUMN_SOURCE_TS,
    MEDALLION_SYSTEM_COLUMN_VALID_FROM,
    MEDALLION_SYSTEM_COLUMN_VALID_TO,
)
    build_scd1_silver_cleanse,
    build_scd2_silver_cleanse,
)

pytestmark = pytest.mark.medallion

# ── Bronze-metadata columns that the cleanse helpers strip ───────────────────
M_OP = MEDALLION_SYSTEM_COLUMN_OP
M_SOURCE_TS = MEDALLION_SYSTEM_COLUMN_SOURCE_TS
M_BRONZE_TS = MEDALLION_SYSTEM_COLUMN_BRONZE_TS
M_PK = MEDALLION_SYSTEM_COLUMN_PK

# SCD2 row-shape columns the helpers preserve.
VALID_FROM = MEDALLION_SYSTEM_COLUMN_VALID_FROM
VALID_TO = MEDALLION_SYSTEM_COLUMN_VALID_TO

PK_COLS = ["tenant_id", "entity_id"]

TS_1 = datetime(2024, 1, 1, 12, 0, 0, tzinfo=timezone.utc)
TS_2 = datetime(2024, 1, 2, 12, 0, 0, tzinfo=timezone.utc)
BRONZE_TS = datetime(2024, 6, 1, 0, 0, 0, tzinfo=timezone.utc)


# ── Helpers ──────────────────────────────────────────────────────────────────


def make_scd2_valid_raw(rows: list[tuple]) -> TableFrame:
    """rows: (tenant_id, entity_id, name, source_ts) tuples."""
    n = len(rows)
    return TableFrame.from_dict(
        {
            "tenant_id": [r[0] for r in rows],
            "entity_id": [r[1] for r in rows],
            "name": [r[2] for r in rows],
            M_OP: ["I"] * n,
            M_SOURCE_TS: [r[3] for r in rows],
            M_BRONZE_TS: [BRONZE_TS] * n,
            M_PK: [PK_COLS] * n,
            VALID_FROM: [r[3] for r in rows],
            VALID_TO: [None] * n,
        }
    ).cast(
        {
            "tenant_id": td_dtypes.String,
            "entity_id": td_dtypes.Int64,
            "name": td_dtypes.String,
            M_OP: td_dtypes.String,
            VALID_TO: td_dtypes.Datetime(time_unit="us", time_zone="UTC"),
        }
    )


def make_scd2_expired_raw(rows: list[tuple]) -> TableFrame:
    """rows: (tenant_id, entity_id, name, valid_from, valid_to) tuples."""
    n = len(rows)
    return TableFrame.from_dict(
        {
            "tenant_id": [r[0] for r in rows],
            "entity_id": [r[1] for r in rows],
            "name": [r[2] for r in rows],
            M_OP: ["U"] * n,
            M_SOURCE_TS: [r[3] for r in rows],
            M_BRONZE_TS: [BRONZE_TS] * n,
            M_PK: [PK_COLS] * n,
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


def make_scd1_raw(rows: list[tuple]) -> TableFrame:
    """rows: (tenant_id, entity_id, name, source_ts) tuples."""
    n = len(rows)
    return TableFrame.from_dict(
        {
            "tenant_id": [r[0] for r in rows],
            "entity_id": [r[1] for r in rows],
            "name": [r[2] for r in rows],
            M_OP: ["I"] * n,
            M_SOURCE_TS: [r[3] for r in rows],
            M_BRONZE_TS: [BRONZE_TS] * n,
            M_PK: [PK_COLS] * n,
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


# ============================================================================
# build_scd2_silver_cleanse
# ============================================================================


def test_scd2_cleanse_drops_bronze_metadata_from_both_outputs():
    """The three bronze-only columns (@td.medallion.op, @td.medallion.bronze_ts, @td.medallion.pk) are
    dropped from BOTH the valid and expired outputs."""
    valid = make_scd2_valid_raw([("t1", 1, "Alice", TS_2)])
    expired = make_scd2_expired_raw([("t1", 1, "Alice@v0", TS_1, TS_2)])

    out_valid, out_expired = build_scd2_silver_cleanse([valid], [expired])

    for tf, label in [(out_valid, "valid"), (out_expired, "expired")]:
        cols = user_cols(tf)
        assert M_OP not in cols, f"{label}: `@td.medallion.op` should be dropped — {cols!r}"
        assert M_BRONZE_TS not in cols, f"{label}: `@td.medallion.bronze_ts` should be dropped — {cols!r}"
        assert M_PK in cols, f"{label}: `@td.medallion.pk` should be preserved — {cols!r}"


def test_scd2_cleanse_preserves_source_ts_and_valid_columns():
    """`@td.medallion.source_ts`, `@td.medallion.valid_from`, `@td.medallion.valid_to` are kept on both
    outputs — they're silver-relevant, not bronze metadata."""
    valid = make_scd2_valid_raw([("t1", 1, "Alice", TS_2)])
    expired = make_scd2_expired_raw([("t1", 1, "Alice@v0", TS_1, TS_2)])

    out_valid, out_expired = build_scd2_silver_cleanse([valid], [expired])

    for tf, label in [(out_valid, "valid"), (out_expired, "expired")]:
        cols = user_cols(tf)
        assert M_SOURCE_TS in cols, f"{label}: `@td.medallion.source_ts` should be preserved"
        assert VALID_FROM in cols, f"{label}: `@td.medallion.valid_from` should be preserved"
        assert VALID_TO in cols, f"{label}: `@td.medallion.valid_to` should be preserved"


def test_scd2_cleanse_preserves_pk_and_value_columns():
    """PK + value columns flow through unchanged."""
    valid = make_scd2_valid_raw([("t1", 1, "Alice", TS_2)])
    expired = make_scd2_expired_raw([("t1", 1, "Alice@v0", TS_1, TS_2)])

    out_valid, out_expired = build_scd2_silver_cleanse([valid], [expired])

    valid_row = to_rows(out_valid)[0]
    assert valid_row["tenant_id"] == "t1"
    assert int(valid_row["entity_id"]) == 1
    assert valid_row["name"] == "Alice"

    expired_row = to_rows(out_expired)[0]
    assert expired_row["tenant_id"] == "t1"
    assert int(expired_row["entity_id"]) == 1
    assert expired_row["name"] == "Alice@v0"


def test_scd2_cleanse_concatenates_multiple_commits_diagonally():
    """Multiple TableFrames per arg get diagonal-concated before the drop."""
    valid_a = make_scd2_valid_raw([("t1", 1, "Alice", TS_2)])
    valid_b = make_scd2_valid_raw([("t1", 2, "Bob", TS_2)])
    expired_a = make_scd2_expired_raw([("t1", 1, "Alice@v0", TS_1, TS_2)])
    expired_b = make_scd2_expired_raw([("t1", 2, "Bob@v0", TS_1, TS_2)])

    out_valid, out_expired = build_scd2_silver_cleanse(
        [valid_a, valid_b], [expired_a, expired_b]
    )

    assert len(to_rows(out_valid)) == 2
    assert len(to_rows(out_expired)) == 2


def test_scd2_cleanse_outputs_are_schema_aligned():
    """Same projection is applied to both valid and expired, so they
    end up with the same user-visible column set (downstream UNION ALL
    is trivial)."""
    valid = make_scd2_valid_raw([("t1", 1, "Alice", TS_2)])
    expired = make_scd2_expired_raw([("t1", 1, "Alice@v0", TS_1, TS_2)])

    out_valid, out_expired = build_scd2_silver_cleanse([valid], [expired])

    assert set(user_cols(out_valid)) == set(user_cols(out_expired))


# ============================================================================
# build_scd1_silver_cleanse
# ============================================================================


def test_scd1_cleanse_drops_bronze_op_and_bronze_ts():
    """`@td.medallion.op` and `@td.medallion.bronze_ts` are dropped; `@td.medallion.pk` is preserved."""
    scd1 = make_scd1_raw([("t1", 1, "Alice", TS_1)])

    out = build_scd1_silver_cleanse([scd1])

    cols = user_cols(out)
    assert M_OP not in cols
    assert M_BRONZE_TS not in cols
    assert M_PK in cols


def test_scd1_cleanse_preserves_source_ts():
    """`@td.medallion.source_ts` is silver-relevant and stays on the output."""
    scd1 = make_scd1_raw([("t1", 1, "Alice", TS_1)])

    out = build_scd1_silver_cleanse([scd1])

    assert M_SOURCE_TS in user_cols(out)


def test_scd1_cleanse_preserves_pk_and_value_columns():
    """PK + value columns flow through unchanged."""
    scd1 = make_scd1_raw(
        [
            ("t1", 1, "Alice", TS_1),
            ("t2", 7, "Bob", TS_2),
        ]
    )

    out = build_scd1_silver_cleanse([scd1])

    rows = to_rows(out)
    assert len(rows) == 2
    by_key = {(r["tenant_id"], int(r["entity_id"])): r for r in rows}
    assert by_key[("t1", 1)]["name"] == "Alice"
    assert by_key[("t2", 7)]["name"] == "Bob"


def test_scd1_cleanse_concatenates_multiple_commits_diagonally():
    """Multiple TableFrames get diagonal-concated before the drop."""
    a = make_scd1_raw([("t1", 1, "Alice", TS_1)])
    b = make_scd1_raw([("t1", 2, "Bob", TS_2)])

    out = build_scd1_silver_cleanse([a, b])

    assert len(to_rows(out)) == 2
