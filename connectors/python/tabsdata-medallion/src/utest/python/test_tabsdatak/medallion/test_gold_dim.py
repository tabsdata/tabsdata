#
# Copyright 2026 Tabsdata Inc.
#

from datetime import datetime, timezone

import pytest

import tabsdatak.tableframe.datatypes as td_dtypes
from tabsdatak.tableframe import TableFrame
from tabsdatak.tableframe.functions import col, lit
from tabsdatak._medallion._gold._dim import (
    _drop_pk,
    build_type1_dim,
    build_type2_dim,
)

pytestmark = pytest.mark.medallion

# ── Gold row-shape columns ───────────────────────────────────────────────────
M_PK = "$m.pk"
M_SOURCE_TS = "$m.source_ts"
VALID_FROM = "$m.valid_from"
VALID_TO = "$m.valid_to"
ENTITY_SK = "$m.entity_sk"
VERSION_SK = "$m.version_sk"

# Composite natural key used in these tests.
PK_COLS = ["tenant_id", "entity_id"]

# UTC-aware sample timestamps.
TS_1 = datetime(2024, 1, 1, 12, 0, 0, tzinfo=timezone.utc)
TS_2 = datetime(2024, 1, 2, 12, 0, 0, tzinfo=timezone.utc)
TS_3 = datetime(2024, 1, 3, 12, 0, 0, tzinfo=timezone.utc)


# ── Fixtures ─────────────────────────────────────────────────────────────────


def make_valid_silver(rows: list[tuple]) -> TableFrame:
    """Build a cleansed SCD2 `_valid_silver` frame (open rows, valid_to=None).

    rows: (tenant_id, entity_id, city, valid_from) tuples.
    """
    n = len(rows)
    return TableFrame.from_dict(
        {
            "tenant_id": [r[0] for r in rows],
            "entity_id": [r[1] for r in rows],
            "city": [r[2] for r in rows],
            M_SOURCE_TS: [r[3] for r in rows],
            M_PK: [PK_COLS] * n,
            VALID_FROM: [r[3] for r in rows],
            VALID_TO: [None] * n,
        }
    ).cast(
        {
            "tenant_id": td_dtypes.String,
            "entity_id": td_dtypes.Int64,
            "city": td_dtypes.String,
            # All-None valid_to lands as Null dtype without the cast, then
            # clashes with the Datetime valid_to coming from expired rows.
            VALID_TO: td_dtypes.Datetime(time_unit="us", time_zone="UTC"),
        }
    )


def make_expired_silver(rows: list[tuple]) -> TableFrame:
    """Build a cleansed SCD2 `_expired_silver` frame (closed rows).

    rows: (tenant_id, entity_id, city, valid_from, valid_to) tuples.
    """
    n = len(rows)
    return TableFrame.from_dict(
        {
            "tenant_id": [r[0] for r in rows],
            "entity_id": [r[1] for r in rows],
            "city": [r[2] for r in rows],
            M_SOURCE_TS: [r[4] for r in rows],
            M_PK: [PK_COLS] * n,
            VALID_FROM: [r[3] for r in rows],
            VALID_TO: [r[4] for r in rows],
        }
    ).cast(
        {
            "tenant_id": td_dtypes.String,
            "entity_id": td_dtypes.Int64,
            "city": td_dtypes.String,
        }
    )


def make_current_silver(rows: list[tuple]) -> TableFrame:
    """Build a cleansed SCD1 `_current_silver` frame (reference data — no
    valid_from / valid_to effective dating).

    rows: (country_code, name) tuples. Single-column natural key.
    """
    n = len(rows)
    return TableFrame.from_dict(
        {
            "country_code": [r[0] for r in rows],
            "name": [r[1] for r in rows],
            M_SOURCE_TS: [TS_1] * n,
            M_PK: [["country_code"]] * n,
        }
    ).cast({"country_code": td_dtypes.String, "name": td_dtypes.String})


def make_history(rows: list[tuple]) -> TableFrame:
    """Build a prior `dim_<entity>_history` frame (gold Type 2 output shape:
    no $m.pk; carries $m.version_sk + $m.valid_from / $m.valid_to).

    rows: (tenant_id, entity_id, city, valid_from, valid_to, version_sk).
    """
    n = len(rows)
    return TableFrame.from_dict(
        {
            "tenant_id": [r[0] for r in rows],
            "entity_id": [r[1] for r in rows],
            "city": [r[2] for r in rows],
            M_SOURCE_TS: [r[3] for r in rows],
            VALID_FROM: [r[3] for r in rows],
            VALID_TO: [r[4] for r in rows],
            VERSION_SK: [r[5] for r in rows],
        }
    ).cast(
        {
            "tenant_id": td_dtypes.String,
            "entity_id": td_dtypes.Int64,
            "city": td_dtypes.String,
            VALID_TO: td_dtypes.Datetime(time_unit="us", time_zone="UTC"),
            VERSION_SK: td_dtypes.UInt64,
        }
    )


def empty_like(tf: TableFrame) -> TableFrame:
    """A zero-row frame with the same schema (filter on an always-false lit)."""
    return tf.filter(lit(False))


def user_cols(tf: TableFrame) -> list[str]:
    return [c for c in tf.columns if not c.startswith("$td.")]


def to_rows(tf: TableFrame) -> list[dict]:
    pdf = tf.to_pandas()
    keep = [c for c in pdf.columns if not c.startswith("$td.")]
    return pdf[keep].to_dict(orient="records")


# ── _drop_pk ─────────────────────────────────────────────────────────────────


def test_drop_pk_present():
    """`$m.pk` is dropped when present."""
    tf = make_valid_silver([("t1", 1, "NYC", TS_1)])
    out = _drop_pk(tf)
    assert M_PK not in user_cols(out)
    # Other columns survive.
    assert "tenant_id" in user_cols(out)
    assert M_SOURCE_TS in user_cols(out)


def test_drop_pk_absent_is_noop():
    """When `$m.pk` is absent, the frame is returned unchanged (guard branch)."""
    tf = make_valid_silver([("t1", 1, "NYC", TS_1)]).drop([M_PK])
    out = _drop_pk(tf)
    assert sorted(user_cols(out)) == sorted(user_cols(tf))
    assert to_rows(out) == to_rows(tf)


# ── build_type1_dim ──────────────────────────────────────────────────────────


def test_type1_adds_entity_sk_and_drops_pk():
    valid = make_valid_silver([("t1", 1, "NYC", TS_1), ("t1", 2, "LA", TS_1)])
    out = build_type1_dim(valid)

    cols = user_cols(out)
    assert ENTITY_SK in cols
    assert M_PK not in cols
    # Natural key + value + retained metadata survive.
    for c in ("tenant_id", "entity_id", "city", M_SOURCE_TS, VALID_FROM, VALID_TO):
        assert c in cols
    # One row per input row.
    assert len(to_rows(out)) == 2


def test_type1_none_silver_head_returns_none():
    """First fire — the upstream silver table has never committed, so
    `silver_head` is None → return None."""
    assert build_type1_dim(None) is None


def test_type1_entity_sk_deterministic_for_same_key():
    valid_a = make_valid_silver([("t1", 1, "NYC", TS_1)])
    valid_b = make_valid_silver([("t1", 1, "BOSTON", TS_3)])  # same key, diff value
    sk_a = to_rows(build_type1_dim(valid_a))[0][ENTITY_SK]
    sk_b = to_rows(build_type1_dim(valid_b))[0][ENTITY_SK]
    # entity_sk is a hash of the natural key only — value/time changes don't
    # move it.
    assert sk_a == sk_b


def test_type1_entity_sk_distinct_for_different_keys():
    valid = make_valid_silver([("t1", 1, "NYC", TS_1), ("t1", 2, "LA", TS_1)])
    rows = to_rows(build_type1_dim(valid))
    sks = {r[ENTITY_SK] for r in rows}
    assert len(sks) == 2  # two distinct natural keys → two distinct sks


def test_type1_reference_data_shape():
    """Reference data (SCD1 `_current_silver`) has no valid_from/valid_to —
    build_type1_dim still works (only touches $m.pk + adds entity_sk)."""
    current = make_current_silver([("US", "United States"), ("CA", "Canada")])
    out = build_type1_dim(current)

    cols = user_cols(out)
    assert ENTITY_SK in cols
    assert M_PK not in cols
    assert VALID_FROM not in cols  # no SCD2 dating on reference data
    assert len(to_rows(out)) == 2


# ── build_type2_dim ──────────────────────────────────────────────────────────


def test_type2_first_fire_no_history():
    """history=None, no expired → output is the current-valid rows shaped into
    history (version_sk added, $m.pk dropped, valid_to stays null)."""
    valid = make_valid_silver([("t1", 1, "NYC", TS_1), ("t1", 2, "LA", TS_1)])
    out = build_type2_dim(history_head=None, expired_new=[], valid_head=valid)

    cols = user_cols(out)
    assert VERSION_SK in cols
    assert M_PK not in cols
    assert VALID_FROM in cols and VALID_TO in cols
    rows = to_rows(out)
    assert len(rows) == 2
    # Open versions: valid_to is null.
    assert all(r[VALID_TO] is None or str(r[VALID_TO]) == "NaT" for r in rows)


def test_type2_version_transition_closes_and_opens():
    """c1 moves NYC→SF: history holds the closed NYC version + the open SF
    version + the untouched c2 version."""
    valid_v1 = make_valid_silver([("t1", 1, "NYC", TS_1), ("t1", 2, "LA", TS_1)])
    history_v1 = build_type2_dim(None, [], valid_v1)

    expired = make_expired_silver([("t1", 1, "NYC", TS_1, TS_2)])  # NYC closed
    valid_v2 = make_valid_silver([("t1", 1, "SF", TS_2), ("t1", 2, "LA", TS_1)])
    out = build_type2_dim(history_v1, [expired], valid_v2)

    rows = to_rows(out)
    assert len(rows) == 3
    cities_to = {(r["city"], r[VALID_TO] is None or str(r[VALID_TO]) == "NaT")
                 for r in rows}
    # NYC closed (valid_to set → False), SF open, LA open.
    assert ("NYC", False) in cities_to
    assert ("SF", True) in cities_to
    assert ("LA", True) in cities_to


def test_type2_version_sk_stable_across_close():
    """A version's version_sk = hash(nk, valid_from) is identical whether the
    version is open (in valid) or closed (in expired) — valid_from is what's
    hashed, and it doesn't change when the version closes."""
    valid_v1 = make_valid_silver([("t1", 1, "NYC", TS_1)])
    history_v1 = build_type2_dim(None, [], valid_v1)
    nyc_open_sk = to_rows(history_v1)[0][VERSION_SK]

    expired = make_expired_silver([("t1", 1, "NYC", TS_1, TS_2)])
    valid_v2 = make_valid_silver([("t1", 1, "SF", TS_2)])
    out = build_type2_dim(history_v1, [expired], valid_v2)

    nyc_closed = [r for r in to_rows(out) if r["city"] == "NYC"]
    assert len(nyc_closed) == 1
    assert nyc_closed[0][VERSION_SK] == nyc_open_sk


def test_type2_prior_expired_passthrough_unchanged():
    """Already-closed rows in history@HEAD pass through verbatim — their
    version_sk is NOT recomputed."""
    sentinel_sk = 424242
    history = make_history([("t1", 1, "OLD", TS_1, TS_2, sentinel_sk)])
    valid = make_valid_silver([("t1", 1, "NEW", TS_2)])
    out = build_type2_dim(history, [], valid)

    sks = {r[VERSION_SK] for r in to_rows(out)}
    assert sentinel_sk in sks  # passthrough preserved its surrogate key
    assert len(to_rows(out)) == 2  # closed passthrough + new open


def test_type2_prior_open_rows_are_dropped():
    """Open rows (valid_to null) in history@HEAD are NOT carried through — the
    current-valid branch replaces them."""
    sentinel_sk = 999999
    history = make_history([("t1", 1, "OPEN_OLD", TS_1, None, sentinel_sk)])
    valid = make_valid_silver([("t1", 1, "OPEN_NEW", TS_2)])
    out = build_type2_dim(history, [], valid)

    rows = to_rows(out)
    assert len(rows) == 1  # only the new open row
    assert rows[0]["city"] == "OPEN_NEW"
    assert sentinel_sk not in {r[VERSION_SK] for r in rows}


def test_type2_empty_history_frame_is_skipped():
    """A zero-row (but typed) history frame skips branch 1, same as None."""
    valid = make_valid_silver([("t1", 1, "NYC", TS_1)])
    empty_hist = empty_like(build_type2_dim(None, [], valid))
    out = build_type2_dim(empty_hist, [], valid)
    assert len(to_rows(out)) == 1


def test_type2_empty_expired_list_is_skipped():
    """expired_new=[] → concat returns None → branch 2 skipped."""
    valid = make_valid_silver([("t1", 1, "NYC", TS_1)])
    out = build_type2_dim(None, [], valid)
    assert len(to_rows(out)) == 1
    assert VERSION_SK in user_cols(out)


def test_type2_empty_expired_frame_is_skipped():
    """expired_new=[empty frame] → concat returns an empty frame → branch 2
    skipped via the is_empty() guard."""
    valid = make_valid_silver([("t1", 1, "NYC", TS_1)])
    empty_expired = empty_like(make_expired_silver([("t1", 9, "X", TS_1, TS_2)]))
    out = build_type2_dim(None, [empty_expired], valid)
    assert len(to_rows(out)) == 1


def test_type2_multiple_expired_commits_concatenated():
    """Several `_expired_silver@NEW` commits are all folded into history."""
    valid = make_valid_silver([("t1", 1, "SF", TS_3)])
    expired_a = make_expired_silver([("t1", 1, "NYC", TS_1, TS_2)])
    expired_b = make_expired_silver([("t1", 1, "BOSTON", TS_2, TS_3)])
    out = build_type2_dim(None, [expired_a, expired_b], valid)

    rows = to_rows(out)
    # two closed versions + one open version.
    assert len(rows) == 3
    assert {r["city"] for r in rows} == {"NYC", "BOSTON", "SF"}
