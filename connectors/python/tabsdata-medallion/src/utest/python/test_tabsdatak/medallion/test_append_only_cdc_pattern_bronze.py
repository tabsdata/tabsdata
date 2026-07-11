#
# Copyright 2026 Tabsdata Inc.
#

from datetime import datetime, timezone

import polars as pl
import pytest

import tabsdatak.tableframe.datatypes as td_dtypes
from tabsdatak.tableframe import TableFrame
from tabsdatak._medallion._bronze._append_only_cdc import (
    OP_INSERT,
    build_append_only_cdc_bronze,
)

pytestmark = pytest.mark.medallion

# ── Metadata column names ────────────────────────────────────────────────────
# Output metadata columns produced by build_append_only_cdc_bronze.
M_OP = "$m.op"
M_SOURCE_TS = "$m.source_ts"
M_BRONZE_TS = "$m.bronze_ts"
M_PK = "$m.pk"

# Composite primary key used in these tests.
PK = [
    {"tenant_id": td_dtypes.String},
    {"entity_id": td_dtypes.Int64},
]
EXPECTED_PK_COLS = ["tenant_id", "entity_id"]

# The upstream column carrying the event time; build_append_only_cdc_bronze
# renames this column to "$m.source_ts" on its way through.
EVENT_TS_COL = "event_ts"

# UTC-aware sample timestamps — input event_ts column is expected to be
# tz-aware, and the helper preserves that through the rename.
TS_A = datetime(2024, 1, 1, 12, 0, 0, tzinfo=timezone.utc)
TS_B = datetime(2024, 1, 2, 12, 0, 0, tzinfo=timezone.utc)

# Naive (no tz) counterparts — exercise the ``dt.replace_time_zone("UTC")``
# branch where the helper attaches UTC to a naive upstream column.
TS_A_NAIVE = datetime(2024, 1, 1, 12, 0, 0)
TS_B_NAIVE = datetime(2024, 1, 2, 12, 0, 0)


# ── Helpers ──────────────────────────────────────────────────────────────────


def make_event_batch(rows: list[tuple]) -> TableFrame:
    """Build an event-batch TableFrame.

    rows is a list of (tenant_id, entity_id, event_ts, payload) tuples.
    Schema:
      tenant_id  VARCHAR    PK part 1
      entity_id  BIGINT     PK part 2
      event_ts   TIMESTAMP  source event time (renamed to $m.source_ts by the helper)
      payload    VARCHAR    arbitrary payload column — flows through unchanged
    """
    assert rows, "make_event_batch needs at least one row"
    # Explicit casts on the non-datetime columns pin dtypes so all-None
    # payload columns don't degrade to ``Null`` (which would break the
    # diagonal concat of multi-batch tests). The datetime column is left
    # to Polars's tz-aware inference from the tz-aware Python datetimes.
    return TableFrame.from_dict(
        {
            "tenant_id": [r[0] for r in rows],
            "entity_id": [r[1] for r in rows],
            EVENT_TS_COL: [r[2] for r in rows],
            "payload": [r[3] for r in rows],
        }
    ).cast(
        {
            "tenant_id": td_dtypes.String,
            "entity_id": td_dtypes.Int64,
            "payload": td_dtypes.String,
        }
    )


def make_event_batch_with_tz(rows: list[tuple], tz_name: str) -> TableFrame:
    """Build a TableFrame whose event_ts column is interpreted in ``tz_name``.

    Treats the naive datetime in each tuple as a wall-clock value in
    ``tz_name``. So ``(21:00, "Asia/Tokyo")`` means 21:00 JST — the same
    instant as 12:00 UTC.

    Implementation note: ``TableFrame.from_dict`` auto-normalizes any
    tz-aware Python datetime to UTC at construction, and constructing a
    pl.DataFrame with ``schema=pl.Datetime("us", tz_name)`` stores naive
    inputs as that wall-clock-in-UTC (NOT as that wall-clock-in-tz_name).
    The only reliable way to attach a non-UTC tz interpretation is to
    construct the column naive, then call ``dt.replace_time_zone(tz_name)``
    to declare "this wall-clock is in tz_name" — that's what this helper
    does. The internal storage shifts to the correct UTC instant; the
    dtype's time_zone becomes ``tz_name`` so the function under test
    routes through ``dt.convert_time_zone("UTC")``.
    """
    pdf = pl.DataFrame(
        {
            "tenant_id": [r[0] for r in rows],
            "entity_id": [r[1] for r in rows],
            EVENT_TS_COL: [r[2] for r in rows],
            "payload": [r[3] for r in rows],
        },
        schema={
            "tenant_id": pl.String,
            "entity_id": pl.Int64,
            EVENT_TS_COL: pl.Datetime("us"),  # naive — declared below
            "payload": pl.String,
        },
    ).with_columns(pl.col(EVENT_TS_COL).dt.replace_time_zone(tz_name))
    return TableFrame.from_polars(pdf)


def user_cols(tf: TableFrame) -> list[str]:
    """Drop tabsdata system columns ($td.*) so we can assert on user columns."""
    return [c for c in tf.columns if not c.startswith("$td.")]


def to_rows(tf: TableFrame) -> list[dict]:
    """Return rows as list-of-dicts with system columns stripped."""
    pdf = tf.to_pandas()
    keep = [c for c in pdf.columns if not c.startswith("$td.")]
    return pdf[keep].to_dict(orient="records")


def call(new: list[TableFrame]) -> TableFrame:
    return build_append_only_cdc_bronze(new=new, pk=PK, event_ts_col=EVENT_TS_COL)


def _assert_utc_aware(ts, label: str) -> None:
    """Pin the contract that timestamps are UTC-aware. A regression to
    naive timestamps trips ``tzinfo is None``; a non-UTC offset trips
    the offset comparison.
    """
    assert ts is not None, f"{label} missing"
    assert ts.tzinfo is not None, f"{label} should be UTC-aware, got naive: {ts!r}"
    assert ts.utcoffset() == timezone.utc.utcoffset(
        None
    ), f"{label} should be in UTC; got offset {ts.utcoffset()!r}"


# ── Tests ────────────────────────────────────────────────────────────────────


def test_single_batch_every_row_becomes_insert():
    """Every row in a single new-batch becomes an I event (append-only semantics)."""
    new = [
        make_event_batch(
            [
                ("t1", 1, TS_A, "hello"),
                ("t1", 2, TS_B, "world"),
            ]
        )
    ]
    out = call(new)

    rows = to_rows(out)
    assert len(rows) == 2
    assert all(r[M_OP] == OP_INSERT for r in rows)


def test_event_ts_column_renamed_to_source_ts():
    """The upstream event_ts column is renamed to `$m.source_ts`; the
    original column name is gone from the output."""
    new = [make_event_batch([("t1", 1, TS_A, "hello")])]
    out = call(new)

    cols = user_cols(out)
    assert M_SOURCE_TS in cols
    assert (
        EVENT_TS_COL not in cols
    ), f"original event_ts column should be renamed, but still present in {cols!r}"


def test_source_ts_value_preserved_through_rename():
    """The rename preserves the original event_ts values byte-identically."""
    new = [
        make_event_batch(
            [
                ("t1", 1, TS_A, "a"),
                ("t1", 2, TS_B, "b"),
            ]
        )
    ]
    out = call(new)

    rows = to_rows(out)
    by_entity = {int(r["entity_id"]): r for r in rows}
    # pd.Timestamp.to_pydatetime() returns a tz-aware datetime; compares
    # equal to a tz-aware Python datetime at the same instant.
    assert by_entity[1][M_SOURCE_TS].to_pydatetime() == TS_A
    assert by_entity[2][M_SOURCE_TS].to_pydatetime() == TS_B


def test_pk_column_values_flow_through():
    """The PK columns from the input are preserved on the output rows."""
    new = [
        make_event_batch(
            [
                ("t1", 1, TS_A, "p1"),
                ("t2", 7, TS_B, "p2"),
            ]
        )
    ]
    out = call(new)

    rows = to_rows(out)
    keys = {(r["tenant_id"], int(r["entity_id"])) for r in rows}
    assert keys == {("t1", 1), ("t2", 7)}


def test_non_pk_value_columns_flow_through():
    """Non-PK, non-event_ts columns flow through unchanged."""
    new = [make_event_batch([("t1", 1, TS_A, "data-1")])]
    out = call(new)

    cols = user_cols(out)
    assert (
        "payload" in cols
    ), f"non-PK column 'payload' should pass through, got {cols!r}"
    rows = to_rows(out)
    assert rows[0]["payload"] == "data-1"


def test_multiple_batches_diagonally_concatenated():
    """Multiple TableFrames in `new` → diagonal concat preserves rows from all."""
    a = make_event_batch([("t1", 1, TS_A, "a1")])
    b = make_event_batch(
        [
            ("t1", 2, TS_B, "b2"),
            ("t1", 3, TS_B, "b3"),
        ]
    )
    c = make_event_batch([("t2", 9, TS_A, "c9")])
    out = call([a, b, c])

    rows = to_rows(out)
    assert len(rows) == 4
    assert all(r[M_OP] == OP_INSERT for r in rows)
    keys = {(r["tenant_id"], int(r["entity_id"])) for r in rows}
    assert keys == {("t1", 1), ("t1", 2), ("t1", 3), ("t2", 9)}


def test_pk_meta_column_lists_composite_keys():
    """`$m.pk` is populated on every row with the composite-key column names
    from the `pk` arg."""
    new = [
        make_event_batch(
            [
                ("t1", 1, TS_A, "p"),
                ("t1", 2, TS_B, "q"),
            ]
        )
    ]
    out = call(new)

    assert M_PK in user_cols(out)
    rows = to_rows(out)
    for r in rows:
        assert (
            list(r[M_PK]) == EXPECTED_PK_COLS
        ), f"row with op={r[M_OP]!r} has pk={r[M_PK]!r}, expected {EXPECTED_PK_COLS!r}"


def test_bronze_ts_present_and_utc_aware():
    """`$m.bronze_ts` is stamped on every row, UTC-aware (from
    `datetime.now(tz=timezone.utc)` inside the helper)."""
    new = [
        make_event_batch(
            [
                ("t1", 1, TS_A, "p"),
                ("t1", 2, TS_B, "q"),
            ]
        )
    ]
    out = call(new)

    assert M_BRONZE_TS in user_cols(out)
    rows = to_rows(out)
    for r in rows:
        _assert_utc_aware(r[M_BRONZE_TS], f"{M_BRONZE_TS} (op={r[M_OP]!r})")


def test_source_ts_preserves_utc_awareness_through_rename():
    """The renamed `$m.source_ts` preserves UTC awareness from the upstream
    event_ts column — the rename is a metadata-only op."""
    new = [make_event_batch([("t1", 1, TS_A, "p")])]
    out = call(new)

    rows = to_rows(out)
    for r in rows:
        _assert_utc_aware(r[M_SOURCE_TS], f"{M_SOURCE_TS} (op={r[M_OP]!r})")


def test_naive_event_ts_tagged_as_utc_preserving_wall_clock():
    """``dt.replace_time_zone("UTC")`` attaches UTC to a naive input, preserving
    the wall-clock value. Covers the common case where upstream sources
    (CSVs, simple CDC) emit naive timestamps that are meant to be UTC.
    """
    new = [
        make_event_batch(
            [
                ("t1", 1, TS_A_NAIVE, "p"),
                ("t1", 2, TS_B_NAIVE, "q"),
            ]
        )
    ]
    out = call(new)

    rows = to_rows(out)
    by_entity = {int(r["entity_id"]): r for r in rows}

    for ent_id, expected_naive in [(1, TS_A_NAIVE), (2, TS_B_NAIVE)]:
        src_ts = by_entity[ent_id][M_SOURCE_TS]
        _assert_utc_aware(src_ts, f"{M_SOURCE_TS} (entity_id={ent_id})")
        # Wall-clock preserved — naive 12:00 is interpreted as 12:00 UTC,
        # NOT converted from local time.
        expected_utc = expected_naive.replace(tzinfo=timezone.utc)
        assert (
            src_ts.to_pydatetime() == expected_utc
        ), f"entity_id={ent_id}: got {src_ts!r}, expected {expected_utc!r}"


def test_utc_aware_event_ts_unchanged_by_replace_time_zone():
    """For an already-UTC input, ``dt.replace_time_zone("UTC")`` is a no-op —
    the instant is preserved byte-identically through the rename + tag.
    """
    new = [make_event_batch([("t1", 1, TS_A, "p")])]
    out = call(new)

    src_ts = to_rows(out)[0][M_SOURCE_TS]
    _assert_utc_aware(src_ts, M_SOURCE_TS)
    assert src_ts.to_pydatetime() == TS_A


def test_non_utc_tz_aware_event_ts_converted_to_utc():
    """Non-UTC tz-aware input → ``dt.convert_time_zone("UTC")``.

    The absolute instant is preserved; the wall-clock changes to its UTC
    equivalent. 21:00 in Asia/Tokyo (UTC+9) is the same instant as 12:00
    UTC, so the output's wall-clock must read 12:00.
    """
    tokyo_local = datetime(2024, 1, 1, 21, 0, 0)  # wall-clock 21:00 in JST
    new = [make_event_batch_with_tz([("t1", 1, tokyo_local, "p")], "Asia/Tokyo")]
    out = call(new)

    src_ts = to_rows(out)[0][M_SOURCE_TS]
    _assert_utc_aware(src_ts, M_SOURCE_TS)
    # Compare absolute instant: pd.Timestamp.to_pydatetime() returns a
    # tz-aware datetime; equality on tz-aware datetimes is instant-based.
    # 21:00 JST == 12:00 UTC.
    expected_utc = datetime(2024, 1, 1, 12, 0, 0, tzinfo=timezone.utc)
    assert (
        src_ts.to_pydatetime() == expected_utc
    ), f"got {src_ts!r}, expected {expected_utc!r}"


def test_non_datetime_event_ts_raises_typeerror():
    """If event_ts_col points to a non-Datetime column, raise TypeError."""
    bad = TableFrame.from_dict(
        {
            "tenant_id": ["t1"],
            "entity_id": [1],
            EVENT_TS_COL: ["not-a-datetime"],  # String, not Datetime
            "payload": ["p"],
        }
    ).cast(
        {
            "tenant_id": td_dtypes.String,
            "entity_id": td_dtypes.Int64,
            EVENT_TS_COL: td_dtypes.String,
            "payload": td_dtypes.String,
        }
    )

    with pytest.raises(TypeError, match=EVENT_TS_COL):
        call([bad])


def test_no_update_or_delete_ops():
    """Append-only: only I events. No U or D, regardless of input content."""
    new = [
        make_event_batch(
            [
                ("t1", 1, TS_A, "a"),
                ("t1", 1, TS_B, "b"),  # same PK as previous row — still I, no de-dup
                ("t1", 2, TS_A, "c"),
            ]
        )
    ]
    out = call(new)

    rows = to_rows(out)
    assert len(rows) == 3  # NO de-duplication on PK collisions either.
    assert {r[M_OP] for r in rows} == {OP_INSERT}
