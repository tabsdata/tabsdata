#
# Copyright 2026 Tabsdata Inc.
#

from datetime import datetime, timezone

import pytest

import tabsdatak.tableframe.datatypes as td_dtypes
from tabsdatak.tableframe import TableFrame
from tabsdatak.tableframe.functions import col, lit
from tabsdatak._medallion.gold.dim import (
    _drop_pk,
    build_type1_dim,
    build_type2_dim,
)
from tabsdatak._medallion.gold.fact import build_fact
from tabsdatak._medallion.constants import (
    MEDALLION_SYSTEM_COLUMN_DATE_SK,
    MEDALLION_SYSTEM_COLUMN_ENTITY_SK,
    MEDALLION_SYSTEM_COLUMN_PK,
    MEDALLION_SYSTEM_COLUMN_SOURCE_TS,
    MEDALLION_SYSTEM_COLUMN_VALID_FROM,
    MEDALLION_SYSTEM_COLUMN_VALID_TO,
    MEDALLION_SYSTEM_COLUMN_VERSION_SK,
)

pytestmark = pytest.mark.medallion

# ── Gold row-shape columns ───────────────────────────────────────────────────
M_PK = MEDALLION_SYSTEM_COLUMN_PK
M_SOURCE_TS = MEDALLION_SYSTEM_COLUMN_SOURCE_TS
VALID_FROM = MEDALLION_SYSTEM_COLUMN_VALID_FROM
VALID_TO = MEDALLION_SYSTEM_COLUMN_VALID_TO
ENTITY_SK = MEDALLION_SYSTEM_COLUMN_ENTITY_SK
VERSION_SK = MEDALLION_SYSTEM_COLUMN_VERSION_SK

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
    no @td.medallion.pk; carries @td.medallion.version_sk + @td.medallion.valid_from / @td.medallion.valid_to).

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
    pdf = tf._to_pandas()
    keep = [c for c in pdf.columns if not c.startswith("$td.")]
    return pdf[keep].to_dict(orient="records")


# ── _drop_pk ─────────────────────────────────────────────────────────────────


def test_drop_pk_present():
    """`@td.medallion.pk` is dropped when present."""
    tf = make_valid_silver([("t1", 1, "NYC", TS_1)])
    out = _drop_pk(tf)
    assert M_PK not in user_cols(out)
    # Other columns survive.
    assert "tenant_id" in user_cols(out)
    assert M_SOURCE_TS in user_cols(out)


def test_drop_pk_absent_is_noop():
    """When `@td.medallion.pk` is absent, the frame is returned unchanged (guard branch)."""
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
    build_type1_dim still works (only touches @td.medallion.pk + adds entity_sk)."""
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
    history (version_sk added, @td.medallion.pk dropped, valid_to stays null)."""
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


# ── build_fact ───────────────────────────────────────────────────────────────

DATE_SK = MEDALLION_SYSTEM_COLUMN_DATE_SK

# Timestamps spread across the year so version intervals are unambiguous.
JAN = datetime(2024, 1, 1, tzinfo=timezone.utc)
JUN = datetime(2024, 6, 1, tzinfo=timezone.utc)
FEB_15 = datetime(2024, 2, 15, tzinfo=timezone.utc)
AUG_20 = datetime(2024, 8, 20, tzinfo=timezone.utc)


def make_event_silver(rows: list[tuple]) -> TableFrame:
    """Append-only event silver. rows: (order_id, customer_id, amount, source_ts)."""
    n = len(rows)
    return TableFrame.from_dict(
        {
            "order_id": [r[0] for r in rows],
            "customer_id": [r[1] for r in rows],
            "amount": [r[2] for r in rows],
            M_SOURCE_TS: [r[3] for r in rows],
            M_PK: [["order_id"]] * n,
        }
    ).cast(
        {
            "order_id": td_dtypes.Int64,
            "customer_id": td_dtypes.String,
            "amount": td_dtypes.Float64,
        }
    )


def make_dim_version_history(rows: list[tuple], key: str = "customer_id") -> TableFrame:
    """SCD2 history dim for fact pinning.
    rows: (key_value, valid_from, valid_to, version_sk)."""
    n = len(rows)
    return TableFrame.from_dict(
        {
            key: [r[0] for r in rows],
            VALID_FROM: [r[1] for r in rows],
            VALID_TO: [r[2] for r in rows],
            VERSION_SK: [r[3] for r in rows],
        }
    ).cast(
        {
            key: td_dtypes.String,
            VALID_TO: td_dtypes.Datetime(time_unit="us", time_zone="UTC"),
            VERSION_SK: td_dtypes.UInt64,
        }
    )


def test_fact_pins_version_by_event_ts():
    """Each event links to the dim version active at its @td.medallion.source_ts. Exercises
    both range-filter branches: the closed version (valid_to set) and the open
    version (valid_to null)."""
    events = make_event_silver([
        (1, "c1", 10.0, FEB_15),   # Feb → version active Jan–Jun
        (2, "c1", 20.0, AUG_20),   # Aug → version active Jun–open
    ])
    hist = make_dim_version_history([
        ("c1", JAN, JUN, 111),     # closed version
        ("c1", JUN, None, 222),    # open version
    ])
    out = build_fact([events], [(hist, ["customer_id"], "customer_version_sk")])

    cols = user_cols(out)
    assert "customer_version_sk" in cols
    assert DATE_SK in cols
    assert M_PK not in cols              # @td.medallion.pk dropped
    assert "amount" in cols             # measure rides through
    assert M_SOURCE_TS in cols          # event time kept

    by_order = {r["order_id"]: r for r in to_rows(out)}
    assert by_order[1]["customer_version_sk"] == 111
    assert by_order[2]["customer_version_sk"] == 222
    # date_sk = YYYYMMDD from @td.medallion.source_ts.
    assert by_order[1][DATE_SK] == 20240215
    assert by_order[2][DATE_SK] == 20240820


def test_fact_multiple_dims_pinned():
    """Two referenced dims each contribute their own version_sk column."""
    events = make_event_silver([(1, "c1", 10.0, FEB_15)])
    # reuse the customer history; add a product history keyed on product_id.
    events = events.with_columns(col("customer_id").alias("product_id"))
    cust = make_dim_version_history([("c1", JAN, None, 111)])
    prod = make_dim_version_history([("c1", JAN, None, 999)], key="product_id")
    out = build_fact(
        [events],
        [
            (cust, ["customer_id"], "customer_version_sk"),
            (prod, ["product_id"], "product_version_sk"),
        ],
    )
    row = to_rows(out)[0]
    assert row["customer_version_sk"] == 111
    assert row["product_version_sk"] == 999


def test_fact_orphan_fk_is_dropped():
    """An event whose FK has no matching dim version is dropped (inner join)."""
    events = make_event_silver([
        (1, "c1", 10.0, FEB_15),   # matches
        (2, "ZZ", 99.0, FEB_15),   # unknown customer → dropped
    ])
    hist = make_dim_version_history([("c1", JAN, None, 111)])
    out = build_fact([events], [(hist, ["customer_id"], "customer_version_sk")])
    rows = to_rows(out)
    assert len(rows) == 1
    assert rows[0]["order_id"] == 1


def test_fact_event_before_first_version_is_dropped():
    """An event predating the entity's first version is dropped by the range
    filter (no covering interval)."""
    events = make_event_silver([(1, "c1", 10.0, JAN)])
    # version only starts in June — the January event has no covering version.
    hist = make_dim_version_history([("c1", JUN, None, 222)])
    out = build_fact([events], [(hist, ["customer_id"], "customer_version_sk")])
    assert to_rows(out) == []


def test_fact_empty_events_returns_none():
    """No new events → concat returns None → build_fact returns None."""
    hist = make_dim_version_history([("c1", JAN, None, 111)])
    assert build_fact([], [(hist, ["customer_id"], "customer_version_sk")]) is None
