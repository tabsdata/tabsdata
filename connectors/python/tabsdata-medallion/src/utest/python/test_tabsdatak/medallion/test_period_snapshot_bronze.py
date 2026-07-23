#
# Copyright 2026 Tabsdata Inc.
#

from datetime import date, datetime, timezone

import polars as pl
import pytest

import tabsdatak.tableframe.datatypes as td_dtypes
from tabsdatak.tableframe import TableFrame
from tabsdatak._medallion.bronze.period_snapshot import build_period_snapshot_bronze

pytestmark = pytest.mark.medallion

# ── Metadata column names ────────────────────────────────────────────────────
# Output metadata columns produced by build_period_snapshot_bronze.
M_PERIOD = "$m.period"
M_SOURCE_TS = "$m.source_ts"
M_BRONZE_TS = "$m.bronze_ts"
M_PK = "$m.pk"
M_OP = "$m.op"

# Composite primary key used in these tests.
PK = [
    {"tenant_id": td_dtypes.String},
    {"entity_id": td_dtypes.Int64},
]
EXPECTED_PK_COLS = ["tenant_id", "entity_id"]

# UTC-aware period stamps — what the periodic publisher emits when it
# stamps `$m.period` with a tz-aware datetime (Pattern 5).
PERIOD_A = datetime(2024, 1, 1, 0, 0, 0, tzinfo=timezone.utc)
PERIOD_B = datetime(2024, 1, 2, 0, 0, 0, tzinfo=timezone.utc)

# Date counterparts — covers the test_trf.py-style publisher that stamps
# `datetime.now(tz=timezone.utc).date()` (a Date, not a Datetime). The
# helper mirrors whatever dtype the publisher chose.
PERIOD_A_DATE = date(2024, 1, 1)
PERIOD_B_DATE = date(2024, 1, 2)


# ── Helpers ──────────────────────────────────────────────────────────────────


def make_period_batch(rows: list[tuple], period) -> TableFrame:
    """Build a period-snapshot batch with `$m.period` stamped on every row.

    rows is a list of (tenant_id, entity_id, payload) tuples. The same
    period stamp is applied to every row in the batch — mirroring the
    periodic publisher's "one period per fire" contract.
    """
    assert rows, "make_period_batch needs at least one row"
    return TableFrame.from_dict(
        {
            "tenant_id": [r[0] for r in rows],
            "entity_id": [r[1] for r in rows],
            "payload": [r[2] for r in rows],
            M_PERIOD: [period] * len(rows),
        }
    ).cast(
        {
            "tenant_id": td_dtypes.String,
            "entity_id": td_dtypes.Int64,
            "payload": td_dtypes.String,
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


def call(new: list[TableFrame]) -> TableFrame:
    return build_period_snapshot_bronze(new=new, pk=PK)


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


def test_single_batch_passes_through():
    """Every row in a single new-batch flows through to the output —
    period-snapshot semantics: no diff, no synthesized ops, just the
    rows tagged with bronze metadata."""
    new = [
        make_period_batch(
            [
                ("t1", 1, "hello"),
                ("t1", 2, "world"),
            ],
            period=PERIOD_A,
        )
    ]
    out = call(new)

    rows = to_rows(out)
    assert len(rows) == 2


def test_source_ts_mirrors_period():
    """`$m.source_ts` is populated from the input's `$m.period` column,
    byte-for-byte per row."""
    new = [
        make_period_batch(
            [
                ("t1", 1, "a"),
                ("t1", 2, "b"),
            ],
            period=PERIOD_A,
        )
    ]
    out = call(new)

    rows = to_rows(out)
    for r in rows:
        # pd.Timestamp.to_pydatetime() returns a tz-aware datetime;
        # compares equal to a tz-aware Python datetime at the same instant.
        assert r[M_SOURCE_TS].to_pydatetime() == PERIOD_A


def test_period_column_preserved_on_output():
    """The original `$m.period` column stays on every output row — the
    mirror is a copy, not a rename. Downstream consumers can still read
    the original period stamp."""
    new = [make_period_batch([("t1", 1, "p")], period=PERIOD_A)]
    out = call(new)

    cols = user_cols(out)
    assert (
        M_PERIOD in cols
    ), f"`$m.period` should be preserved on output, got cols {cols!r}"

    rows = to_rows(out)
    assert rows[0][M_PERIOD].to_pydatetime() == PERIOD_A


def test_pk_column_values_flow_through():
    """The PK columns from the input are preserved on the output rows."""
    new = [
        make_period_batch(
            [
                ("t1", 1, "p1"),
                ("t2", 7, "p2"),
            ],
            period=PERIOD_A,
        )
    ]
    out = call(new)

    rows = to_rows(out)
    keys = {(r["tenant_id"], int(r["entity_id"])) for r in rows}
    assert keys == {("t1", 1), ("t2", 7)}


def test_non_pk_value_columns_flow_through():
    """Non-PK, non-metadata columns flow through unchanged."""
    new = [make_period_batch([("t1", 1, "data-1")], period=PERIOD_A)]
    out = call(new)

    cols = user_cols(out)
    assert (
        "payload" in cols
    ), f"non-PK column 'payload' should pass through, got {cols!r}"
    rows = to_rows(out)
    assert rows[0]["payload"] == "data-1"


def test_multiple_batches_diagonally_concatenated():
    """Multiple TableFrames in `new` → diagonal concat preserves rows
    from all batches."""
    a = make_period_batch([("t1", 1, "a1")], period=PERIOD_A)
    b = make_period_batch(
        [
            ("t1", 2, "b2"),
            ("t1", 3, "b3"),
        ],
        period=PERIOD_A,
    )
    c = make_period_batch([("t2", 9, "c9")], period=PERIOD_B)
    out = call([a, b, c])

    rows = to_rows(out)
    assert len(rows) == 4
    keys = {(r["tenant_id"], int(r["entity_id"])) for r in rows}
    assert keys == {("t1", 1), ("t1", 2), ("t1", 3), ("t2", 9)}


def test_different_periods_across_batches_carry_through():
    """When `new` contains batches stamped with DIFFERENT periods, each
    row's `$m.source_ts` matches its own batch's period — the helper
    mirrors per-row, not per-batch."""
    a = make_period_batch([("t1", 1, "from-a")], period=PERIOD_A)
    b = make_period_batch([("t1", 2, "from-b")], period=PERIOD_B)
    out = call([a, b])

    rows = to_rows(out)
    by_entity = {int(r["entity_id"]): r for r in rows}
    assert by_entity[1][M_SOURCE_TS].to_pydatetime() == PERIOD_A
    assert by_entity[2][M_SOURCE_TS].to_pydatetime() == PERIOD_B


def test_pk_meta_column_lists_composite_keys():
    """`$m.pk` is populated on every row with the composite-key column
    names from the `pk` arg."""
    new = [
        make_period_batch(
            [
                ("t1", 1, "p"),
                ("t1", 2, "q"),
            ],
            period=PERIOD_A,
        )
    ]
    out = call(new)

    assert M_PK in user_cols(out)
    rows = to_rows(out)
    for r in rows:
        assert (
            list(r[M_PK]) == EXPECTED_PK_COLS
        ), f"row has pk={r[M_PK]!r}, expected {EXPECTED_PK_COLS!r}"


def test_single_pk_in_pk_meta_column():
    """`$m.pk` for a single-key PK is a one-element list (not a scalar)."""
    single_pk = [{"entity_id": td_dtypes.Int64}]
    batch = TableFrame.from_dict(
        {
            "entity_id": [1, 2],
            "payload": ["a", "b"],
            M_PERIOD: [PERIOD_A, PERIOD_A],
        }
    ).cast({"entity_id": td_dtypes.Int64, "payload": td_dtypes.String})

    out = build_period_snapshot_bronze(new=[batch], pk=single_pk)

    rows = to_rows(out)
    for r in rows:
        assert list(r[M_PK]) == ["entity_id"]


def test_bronze_ts_present_and_utc_aware():
    """`$m.bronze_ts` is stamped on every row, UTC-aware (from
    `datetime.now(tz=timezone.utc)` inside the helper)."""
    new = [
        make_period_batch(
            [
                ("t1", 1, "p"),
                ("t1", 2, "q"),
            ],
            period=PERIOD_A,
        )
    ]
    out = call(new)

    assert M_BRONZE_TS in user_cols(out)
    rows = to_rows(out)
    for r in rows:
        _assert_utc_aware(r[M_BRONZE_TS], M_BRONZE_TS)


def test_no_op_column_added():
    """Period-snapshot bronze does NOT stamp `$m.op` — every row is a
    snapshot, not an I/U/D event. This is the distinguishing feature
    vs. the CDC / snapshot-diff / append-only bronze flavors."""
    new = [make_period_batch([("t1", 1, "p")], period=PERIOD_A)]
    out = call(new)

    assert M_OP not in user_cols(out), (
        f"`$m.op` should not be present in period-snapshot bronze; "
        f"got cols {user_cols(out)!r}"
    )


def test_datetime_period_dtype_preserved_in_source_ts():
    """A Datetime `$m.period` (tz-aware) propagates the same dtype into
    `$m.source_ts`. The helper mirrors the column — no cast."""
    new = [make_period_batch([("t1", 1, "p")], period=PERIOD_A)]
    out = call(new)

    period_dtype = out.schema[M_PERIOD]
    source_ts_dtype = out.schema[M_SOURCE_TS]
    assert period_dtype == source_ts_dtype, (
        f"`$m.source_ts` dtype {source_ts_dtype!r} should mirror "
        f"`$m.period` dtype {period_dtype!r}"
    )
    assert isinstance(
        source_ts_dtype, pl.Datetime
    ), f"`$m.source_ts` should be Datetime, got {source_ts_dtype!r}"


def test_date_period_dtype_preserved_in_source_ts():
    """A Date `$m.period` (as the test_trf.py publisher uses) propagates
    the Date dtype into `$m.source_ts`. Documents that the helper does
    NOT coerce a Date to a Datetime — the publisher's choice rides
    through."""
    batch = TableFrame.from_dict(
        {
            "tenant_id": ["t1"],
            "entity_id": [1],
            "payload": ["p"],
            M_PERIOD: [PERIOD_A_DATE],
        }
    ).cast(
        {
            "tenant_id": td_dtypes.String,
            "entity_id": td_dtypes.Int64,
            "payload": td_dtypes.String,
        }
    )

    out = call([batch])

    period_dtype = out.schema[M_PERIOD]
    source_ts_dtype = out.schema[M_SOURCE_TS]
    assert period_dtype == source_ts_dtype
    assert source_ts_dtype == pl.Date, (
        f"`$m.source_ts` should be Date when `$m.period` is Date, "
        f"got {source_ts_dtype!r}"
    )


def test_missing_period_column_raises():
    """If the input doesn't carry `$m.period`, the helper fails. The
    column is a hard precondition — the periodic publisher is expected
    to stamp it on every row."""
    bad = TableFrame.from_dict(
        {
            "tenant_id": ["t1"],
            "entity_id": [1],
            "payload": ["p"],
            # NB: no $m.period column.
        }
    ).cast(
        {
            "tenant_id": td_dtypes.String,
            "entity_id": td_dtypes.Int64,
            "payload": td_dtypes.String,
        }
    )

    with pytest.raises(Exception, match=r"\$m\.period|period"):
        # The error may surface lazily — touch the output to force
        # collection.
        out = build_period_snapshot_bronze(new=[bad], pk=PK)
        out.to_pandas()


@pytest.mark.parametrize(
    "new",
    [
        pytest.param([], id="empty_list"),
        pytest.param(None, id="none"),
        pytest.param([None, None], id="all_none_entries"),
    ],
)
def test_no_new_data_returns_none(new):
    """No new data → `None`. When `new` is an empty list, `None`, or a
    list whose entries are all `None`, `concat` yields `None`, and the
    helper short-circuits to `None` instead of building an output frame
    (the periodic transformer's "nothing fired this period" path)."""
    assert build_period_snapshot_bronze(new=new, pk=PK) is None
