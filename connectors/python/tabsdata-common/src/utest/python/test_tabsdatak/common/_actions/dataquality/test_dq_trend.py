#
# Copyright 2026 Tabsdata Inc.
#

"""Unit tests for ``dq_trend_tfr``, the entry point the generated trend module
calls. Each test drives it the way the runner does: a summary table version in,
the previous trend table in, the next trend table out.

The window is anchored on the newest summary timestamp, not on wall clock, so
every case here is deterministic.
"""

import gc
import importlib.util
import inspect
import itertools
import sys
import tempfile
from datetime import datetime, timedelta, timezone
from pathlib import Path

import polars as pl
import pytest

from tabsdatak._api import callable_to_str
from tabsdatak.common._actions.dataquality.engine import ClassifierFamily, DQColumns
from tabsdatak.common._actions.dataquality.trend import (
    OUTPUT,
    Grain,
    TrendColumns,
    build_trend_tfr_mod,
    cleanup_trend_tfr_mod,
    dq_trend_tfr,
)
from tabsdatak.tableframe._core.hooks import tableframe_of
from tabsdatak.tableframe._engine.constant.column.names import (
    TD_TIMESTAMP_SYSTEM_COLUMN_NAME,
    TD_TRANSACTION_SYSTEM_COLUMN_NAME,
)

pytestmark = pytest.mark.dq

DAYS, WEEKS, MONTHS = 7, 12, 24

UTC = timezone.utc

# Where `stored()` puts the trend between executions; the OS reclaims it.
_STORAGE = tempfile.TemporaryDirectory(prefix="td_trend_tests_")
_VERSION = itertools.count()

# A Monday, so week buckets truncate to this instant's own date.
MONDAY = datetime(2026, 1, 5, 12, tzinfo=UTC)

SUMMARY_DTYPES = {
    DQColumns.DQ_TRUE.value: pl.UInt64,
    DQColumns.DQ_FALSE.value: pl.UInt64,
    DQColumns.DQ_NONE.value: pl.UInt64,
    DQColumns.DQ_NAN.value: pl.UInt64,
    TD_TIMESTAMP_SYSTEM_COLUMN_NAME: pl.Datetime("us", UTC),
}


def classifier(
    name: str,
    *,
    passed: int = 3,
    failed: int = 1,
    nulls: int = 0,
    nans: int = 0,
    family: str = ClassifierFamily.BOOLEAN.value,
) -> dict:
    """One summary row, as the ``Summary`` operator would have written it."""
    return {
        DQColumns.DQ_TYPE.value: family,
        DQColumns.DQ_CLASSIFIER.value: name,
        DQColumns.DQ_TRUE.value: passed,
        DQColumns.DQ_FALSE.value: failed,
        DQColumns.DQ_NONE.value: nulls,
        DQColumns.DQ_NAN.value: nans,
    }


def summary(at: datetime, txn: str, rows: list[dict] | None = None):
    """A summary table version: its rows plus the system columns the server
    stamps on, which is where the trend's bucket and txn come from."""
    rows = rows if rows is not None else [classifier("@td.dq.a_ok")]
    columns = {key: [row[key] for row in rows] for key in rows[0]}
    columns[TD_TIMESTAMP_SYSTEM_COLUMN_NAME] = [at] * len(rows)
    columns[TD_TRANSACTION_SYSTEM_COLUMN_NAME] = [txn] * len(rows)
    return tableframe_of(pl.LazyFrame(columns, schema_overrides=SUMMARY_DTYPES))


def roll(summary_tf, prev=None, days=DAYS, weeks=WEEKS, months=MONTHS):
    return dq_trend_tfr(summary_tf, prev, days, weeks, months)


def rows_of(trend) -> pl.DataFrame:
    """The trend's own columns, dropping the system columns TableFrame appends."""
    return trend._lf.collect().select(OUTPUT)


def tiers(trend) -> pl.DataFrame:
    """The trend proper, without the folded-transaction ledger the roll carries
    alongside it for bookkeeping."""
    return rows_of(trend).filter(pl.col(TrendColumns.GRAIN.value) != Grain.FOLDED.value)


def ledger_txns(trend) -> list[str]:
    return sorted(grain(trend, Grain.FOLDED)[TrendColumns.TXN.value].to_list())


def grain(trend, which: Grain) -> pl.DataFrame:
    return rows_of(trend).filter(pl.col(TrendColumns.GRAIN.value) == which.value)


def counts(trend, which: Grain, column: TrendColumns = TrendColumns.RUNS) -> list[int]:
    return grain(trend, which)[column.value].to_list()


def marks(trend) -> list[datetime]:
    """The high-water mark, which every row of a version carries alike."""
    return rows_of(trend)[TrendColumns.FOLDED_THROUGH.value].unique().to_list()


def stored(trend) -> object:
    """`prev` as the runner supplies it: the trend read back out of storage.

    Threading the frame straight back in instead would nest one lazy plan
    inside the next, which costs more at every step and is not the shape
    production ever sees -- each execution there scans the stored table.
    """
    path = Path(_STORAGE.name) / f"trend-{next(_VERSION)}.parquet"
    rows_of(trend).write_parquet(path)
    return tableframe_of(pl.scan_parquet(path))


def replay(executions: list[tuple[datetime, str]], **retention):
    """Run one execution per (timestamp, txn), threading the output forward."""
    trend = None
    for at, txn in executions:
        trend = stored(roll(summary(at, txn), trend, **retention))
    return trend


def test_no_summary_yields_no_trend():
    assert roll(None) is None


def test_summary_without_boolean_classifiers_yields_no_trend():
    # Categorizers carry no pass/fail, so there is nothing to trend and the
    # transformer must not write an empty table over the previous one.
    only_categorizers = summary(
        MONDAY,
        "txn-1",
        [classifier("@td.dq.a_bins", family=ClassifierFamily.CATEGORICAL.value)],
    )
    assert roll(only_categorizers) is None


def test_non_boolean_rows_are_excluded_from_a_mixed_summary():
    mixed = summary(
        MONDAY,
        "txn-1",
        [
            classifier("@td.dq.a_ok"),
            classifier("@td.dq.a_bins", family=ClassifierFamily.CATEGORICAL.value),
        ],
    )
    classifiers = set(tiers(roll(mixed))[TrendColumns.CLASSIFIER.value].to_list())
    assert classifiers == {"@td.dq.a_ok"}


def test_first_execution_emits_every_grain():
    trend = roll(summary(MONDAY, "txn-1"))
    assert tiers(trend)[TrendColumns.GRAIN.value].to_list() == [
        Grain.MONTH.value,
        Grain.RUN.value,
        Grain.WEEK.value,
    ]


def test_output_columns_are_exactly_the_declared_set():
    trend = roll(summary(MONDAY, "txn-1"))
    assert trend._lf.collect_schema().names()[: len(OUTPUT)] == OUTPUT


def test_the_runners_dtypes_survive_into_the_trend():
    schema = rows_of(roll(summary(MONDAY, "txn-1"))).schema
    assert schema[TrendColumns.BUCKET.value] == pl.Datetime("us", "UTC")
    for column in (
        TrendColumns.RUNS,
        TrendColumns.PASSED,
        TrendColumns.FAILED,
        TrendColumns.NULLS,
        TrendColumns.NANS,
    ):
        assert schema[column.value] == pl.Int64


def test_rows_are_sorted_by_grain_bucket_classifier():
    trend = roll(
        summary(
            MONDAY,
            "txn-1",
            [classifier("@td.dq.b_ok"), classifier("@td.dq.a_ok")],
        )
    )
    frame = rows_of(trend)
    assert frame.equals(
        frame.sort(*[
            c.value
            for c in (TrendColumns.GRAIN, TrendColumns.BUCKET, TrendColumns.CLASSIFIER)
        ])
    )


def test_counts_come_from_the_summary_verdict_columns():
    trend = roll(
        summary(
            MONDAY,
            "txn-1",
            [classifier("@td.dq.a_ok", passed=7, failed=2, nulls=1, nans=3)],
        )
    )
    run = grain(trend, Grain.RUN).to_dicts()[0]
    assert run[TrendColumns.RUNS.value] == 1
    assert run[TrendColumns.PASSED.value] == 7
    assert run[TrendColumns.FAILED.value] == 2
    assert run[TrendColumns.NULLS.value] == 1
    assert run[TrendColumns.NANS.value] == 3


def test_pass_pct_is_passed_over_passed_plus_failed():
    trend = roll(
        summary(MONDAY, "txn-1", [classifier("@td.dq.a_ok", passed=3, failed=1)])
    )
    pct = tiers(trend)[TrendColumns.PASS_PCT.value].to_list()
    assert pct == [75.0, 75.0, 75.0]


def test_pass_pct_is_null_when_a_classifier_has_no_verdicts():
    # Every value was null or NaN, so passed + failed is 0 and the ratio is
    # 0/0. Null reads as "no measurement"; a NaN would reach the published
    # column, where it is not null and does not equal itself.
    trend = roll(
        summary(
            MONDAY,
            "txn-1",
            [classifier("@td.dq.a_ok", passed=0, failed=0, nulls=5, nans=2)],
        )
    )
    pct = tiers(trend)[TrendColumns.PASS_PCT.value]
    assert pct.to_list() == [None, None, None]
    assert not pct.is_nan().fill_null(False).any()


def test_txn_is_kept_on_run_rows_and_dropped_on_coarse_rows():
    # The txn is what makes a replay a no-op, and it only means anything for a
    # row that still maps one-to-one onto a summary version.
    trend = roll(summary(MONDAY, "txn-1"))
    assert grain(trend, Grain.RUN)[TrendColumns.TXN.value].to_list() == ["txn-1"]
    for coarse in (Grain.WEEK, Grain.MONTH):
        assert grain(trend, coarse)[TrendColumns.TXN.value].to_list() == [None]


def test_each_classifier_is_trended_separately():
    trend = roll(
        summary(
            MONDAY,
            "txn-1",
            [
                classifier("@td.dq.a_ok", passed=1, failed=0),
                classifier("@td.dq.b_ok", passed=0, failed=1),
            ],
        )
    )
    run = grain(trend, Grain.RUN).sort(TrendColumns.CLASSIFIER.value)
    assert run[TrendColumns.CLASSIFIER.value].to_list() == [
        "@td.dq.a_ok",
        "@td.dq.b_ok",
    ]
    assert run[TrendColumns.PASSED.value].to_list() == [1, 0]
    assert run[TrendColumns.PASS_PCT.value].to_list() == [100.0, 0.0]


def test_week_bucket_truncates_to_the_monday():
    thursday = MONDAY + timedelta(days=3)
    trend = roll(summary(thursday, "txn-1"))
    assert grain(trend, Grain.WEEK)[TrendColumns.BUCKET.value].to_list() == [
        datetime(2026, 1, 5, tzinfo=UTC)
    ]


def test_month_bucket_truncates_to_the_first():
    trend = roll(summary(datetime(2026, 1, 22, 9, 30, tzinfo=UTC), "txn-1"))
    assert grain(trend, Grain.MONTH)[TrendColumns.BUCKET.value].to_list() == [
        datetime(2026, 1, 1, tzinfo=UTC)
    ]


def test_run_bucket_is_the_summary_timestamp():
    at = datetime(2026, 1, 7, 4, 15, tzinfo=UTC)
    trend = roll(summary(at, "txn-1"))
    assert grain(trend, Grain.RUN)[TrendColumns.BUCKET.value].to_list() == [at]


def test_buckets_cross_the_december_to_january_boundary():
    # Weeks do not restart at the year end -- the week of Monday 29 December
    # 2025 runs on into January -- while the month buckets do split on the 1st.
    trend = replay([
        (datetime(2025, 12, 29, 12, tzinfo=UTC), "txn-dec"),
        (datetime(2026, 1, 1, 12, tzinfo=UTC), "txn-jan-1"),
        (MONDAY, "txn-jan-5"),
    ])

    weeks = grain(trend, Grain.WEEK)
    assert weeks[TrendColumns.BUCKET.value].to_list() == [
        datetime(2025, 12, 29, tzinfo=UTC),
        datetime(2026, 1, 5, tzinfo=UTC),
    ]
    assert weeks[TrendColumns.RUNS.value].to_list() == [2, 1]

    months = grain(trend, Grain.MONTH)
    assert months[TrendColumns.BUCKET.value].to_list() == [
        datetime(2025, 12, 1, tzinfo=UTC),
        datetime(2026, 1, 1, tzinfo=UTC),
    ]
    assert months[TrendColumns.RUNS.value].to_list() == [1, 2]


def test_runs_in_different_weeks_get_separate_week_buckets():
    trend = replay([(MONDAY, "txn-1"), (MONDAY + timedelta(days=7), "txn-2")])
    assert grain(trend, Grain.WEEK)[TrendColumns.BUCKET.value].to_list() == [
        datetime(2026, 1, 5, tzinfo=UTC),
        datetime(2026, 1, 12, tzinfo=UTC),
    ]
    assert counts(trend, Grain.WEEK) == [1, 1]


def test_coarse_tiers_count_a_run_from_its_first_execution():
    # Not only once it ages out of the run tier: after one execution the week
    # and month already carry it.
    trend = roll(summary(MONDAY, "txn-1"))
    assert counts(trend, Grain.RUN) == [1]
    assert counts(trend, Grain.WEEK) == [1]
    assert counts(trend, Grain.MONTH) == [1]


def test_coarse_counts_are_the_sum_of_their_runs():
    trend = replay([(MONDAY + timedelta(hours=i), f"txn-{i}") for i in range(4)])
    for column in (
        TrendColumns.PASSED,
        TrendColumns.FAILED,
        TrendColumns.NULLS,
        TrendColumns.NANS,
    ):
        run_total = sum(counts(trend, Grain.RUN, column))
        assert counts(trend, Grain.WEEK, column) == [run_total]
        assert counts(trend, Grain.MONTH, column) == [run_total]


def test_replaying_the_same_summary_version_changes_nothing():
    # Run rows are deduplicated on (classifier, txn), so a re-executed
    # transaction neither duplicates a run row nor inflates the coarse tiers.
    once = roll(summary(MONDAY, "txn-1"))
    twice = roll(summary(MONDAY, "txn-1"), once)
    assert rows_of(twice).equals(rows_of(once))


def test_a_prev_frame_read_back_from_parquet_rolls_the_same():
    first = roll(summary(MONDAY, "txn-1"))
    second = summary(MONDAY + timedelta(days=1), "txn-2")

    in_memory = roll(second, first)
    from_disk = roll(second, stored(first))

    assert rows_of(from_disk).equals(rows_of(in_memory))
    assert rows_of(from_disk).schema == rows_of(in_memory).schema


def test_a_re_attempt_under_a_new_timestamp_changes_nothing():
    # A recover mints a new physical run, so the summary it rewrites carries a
    # fresh `$td.ver.timestamp` while keeping its transaction. Nothing about
    # the bucket marks it as already counted -- only the transaction does.
    trend = replay([(MONDAY, "txn-1"), (MONDAY + timedelta(days=1), "txn-2")], days=1)
    assert counts(trend, Grain.WEEK) == [2]
    assert grain(trend, Grain.RUN)[TrendColumns.TXN.value].to_list() == ["txn-2"]

    re_attempt = roll(summary(MONDAY + timedelta(days=3), "txn-1"), trend, days=1)
    assert counts(re_attempt, Grain.WEEK) == [2]
    assert sum(counts(re_attempt, Grain.MONTH)) == 2


def test_a_transaction_arriving_out_of_order_is_still_counted():
    # Two summaries produced concurrently can land in either order, so nothing
    # may treat "older than what we have folded" as "already folded".
    trend = replay([
        (MONDAY + timedelta(days=3), "txn-late"),
        (MONDAY, "txn-early"),
    ])
    assert sum(counts(trend, Grain.WEEK)) == 2
    assert ledger_txns(trend) == ["txn-early", "txn-late"]


def test_re_attempting_a_transaction_that_arrived_out_of_order():
    trend = replay(
        [(MONDAY + timedelta(days=3), "txn-late"), (MONDAY, "txn-early")], days=1
    )
    re_attempt = roll(summary(MONDAY + timedelta(days=5), "txn-early"), trend, days=1)
    assert sum(counts(re_attempt, Grain.WEEK)) == 2
    assert sum(counts(re_attempt, Grain.MONTH)) == 2


def test_a_re_attempt_changes_nothing_when_no_run_rows_are_kept():
    trend = replay([(MONDAY, "txn-1")], days=0)
    assert grain(trend, Grain.RUN).is_empty()

    re_attempt = roll(summary(MONDAY + timedelta(days=2), "txn-1"), trend, days=0)
    assert counts(re_attempt, Grain.WEEK) == [1]
    assert counts(re_attempt, Grain.MONTH) == [1]


def test_the_ledger_holds_one_row_per_transaction():
    # Not one per classifier: the ledger records which runs were folded, and
    # its size is what bounds the cost of remembering them.
    trend = roll(
        summary(
            MONDAY,
            "txn-1",
            [classifier("@td.dq.a_ok"), classifier("@td.dq.b_ok")],
        )
    )
    assert ledger_txns(trend) == ["txn-1"]


def test_the_ledger_is_forgotten_once_every_coarse_tier_has_expired():
    # Past that the run's own contribution is gone, so folding it again counts
    # it once rather than twice -- and the ledger must not grow forever.
    trend = replay(
        [(MONDAY, "txn-old"), (MONDAY + timedelta(days=200), "txn-new")],
        weeks=1,
        months=1,
    )
    assert ledger_txns(trend) == ["txn-new"]


def test_replaying_a_version_that_left_the_run_tier_changes_nothing():
    # The (classifier, txn) dedup can only match run rows, so once one ages out
    # there is nothing left to recognise the replay by. The mark is what does.
    trend = replay([(MONDAY, "txn-1"), (MONDAY + timedelta(days=2), "txn-2")], days=1)
    assert counts(trend, Grain.WEEK) == [2]

    replayed = roll(summary(MONDAY, "txn-1"), trend, days=1)
    assert counts(replayed, Grain.WEEK) == [2]
    assert sum(counts(replayed, Grain.MONTH)) == 2


def test_replaying_into_a_trend_that_keeps_no_run_rows_changes_nothing():
    # With no day retention the run tier never holds anything, so every replay
    # is the aged-out case and the mark is the only defence.
    trend = replay([(MONDAY, "txn-1")], days=0)
    assert grain(trend, Grain.RUN).is_empty()

    replayed = roll(summary(MONDAY, "txn-1"), trend, days=0)
    assert counts(replayed, Grain.WEEK) == [1]
    assert counts(replayed, Grain.MONTH) == [1]


def test_an_idle_parent_re_feeding_its_last_summary_changes_nothing():
    # Nothing new ran for months; the parent re-emits the summary it already
    # published. Its run row is long gone from every tier.
    trend = roll(summary(MONDAY, "txn-1"))
    idle = roll(summary(MONDAY + timedelta(days=120), "txn-2"), trend)
    assert sum(counts(idle, Grain.MONTH)) == 2

    replayed = roll(summary(MONDAY, "txn-1"), idle)
    assert sum(counts(replayed, Grain.MONTH)) == 2


def test_the_mark_is_the_newest_instant_folded_so_far():
    trend = roll(summary(MONDAY, "txn-1"))
    assert marks(trend) == [MONDAY]

    later = MONDAY + timedelta(days=1)
    assert marks(roll(summary(later, "txn-2"), trend)) == [later]


def test_re_feeding_an_old_summary_does_not_rewind_the_mark():
    # The mark floors `now`, so a replay cannot rewind the retention clock and
    # resurrect rows that have already aged out of a tier.
    newest = MONDAY + timedelta(days=2)
    trend = replay([(MONDAY, "txn-1"), (newest, "txn-2")], days=1)

    replayed = roll(summary(MONDAY, "txn-1"), trend, days=1)
    assert marks(replayed) == [newest]
    assert grain(replayed, Grain.RUN)[TrendColumns.BUCKET.value].to_list() == [newest]


def test_the_mark_survives_a_parquet_round_trip():
    # In production `prev` is always the stored table, so a mark that did not
    # survive parquet would leave the aged-out replay with no defence at all.
    trend = replay([(MONDAY, "txn-1"), (MONDAY + timedelta(days=2), "txn-2")], days=1)
    replayed = roll(summary(MONDAY, "txn-1"), stored(trend), days=1)
    assert counts(replayed, Grain.WEEK) == [2]
    assert sum(counts(replayed, Grain.MONTH)) == 2


def test_a_run_older_than_the_day_window_leaves_the_run_tier():
    trend = replay(
        [(MONDAY, "txn-old"), (MONDAY + timedelta(days=10), "txn-new")], days=7
    )
    assert grain(trend, Grain.RUN)[TrendColumns.BUCKET.value].to_list() == [
        MONDAY + timedelta(days=10)
    ]


def test_a_run_that_left_the_run_tier_is_still_counted_by_the_coarse_tiers():
    # This is what makes the retraction safe: an aged-out row is no longer
    # emitted as a run, so nothing retracts it again and its contribution stays.
    trend = replay(
        [(MONDAY, "txn-old"), (MONDAY + timedelta(days=10), "txn-new")], days=7
    )
    assert counts(trend, Grain.RUN) == [1]
    assert counts(trend, Grain.MONTH) == [2]
    assert sum(counts(trend, Grain.WEEK)) == 2


def test_the_day_window_is_exclusive_at_its_edge():
    # `bucket + days > now`, so a run exactly `days` old has already left.
    edge = replay(
        [(MONDAY, "txn-old"), (MONDAY + timedelta(days=7), "txn-new")], days=7
    )
    assert len(grain(edge, Grain.RUN)) == 1

    inside = replay(
        [(MONDAY, "txn-old"), (MONDAY + timedelta(days=6), "txn-new")], days=7
    )
    assert len(grain(inside, Grain.RUN)) == 2


def test_zero_day_retention_keeps_no_run_rows():
    trend = replay([(MONDAY, "txn-1")], days=0)
    assert grain(trend, Grain.RUN).is_empty()
    assert counts(trend, Grain.WEEK) == [1]
    assert counts(trend, Grain.MONTH) == [1]


def test_a_week_outside_the_week_window_is_dropped():
    trend = replay(
        [(MONDAY, "txn-old"), (MONDAY + timedelta(days=21), "txn-new")],
        days=1,
        weeks=2,
    )
    assert grain(trend, Grain.WEEK)[TrendColumns.BUCKET.value].to_list() == [
        datetime(2026, 1, 26, tzinfo=UTC)
    ]


def test_a_week_that_left_the_week_window_survives_in_its_month():
    # Tiers expire independently: losing the week must not lose the history.
    trend = replay(
        [(MONDAY, "txn-old"), (MONDAY + timedelta(days=21), "txn-new")],
        days=1,
        weeks=2,
    )
    assert sum(counts(trend, Grain.MONTH)) == 2


def test_a_month_outside_the_month_window_is_dropped():
    trend = replay(
        [(MONDAY, "txn-old"), (datetime(2026, 6, 1, 12, tzinfo=UTC), "txn-new")],
        days=1,
        weeks=1,
        months=2,
    )
    assert grain(trend, Grain.MONTH)[TrendColumns.BUCKET.value].to_list() == [
        datetime(2026, 6, 1, tzinfo=UTC)
    ]


def test_zero_month_retention_keeps_no_month_rows():
    trend = replay([(MONDAY, "txn-1")], months=0)
    assert grain(trend, Grain.MONTH).is_empty()
    assert counts(trend, Grain.RUN) == [1]
    assert counts(trend, Grain.WEEK) == [1]


@pytest.fixture
def build():
    """Build generated modules, tearing every one of them down afterwards so a
    failing test cannot leak a module into the rest of the session."""
    roots = []

    def builder(days=DAYS, weeks=WEEKS, months=MONTHS):
        func, root = build_trend_tfr_mod(days, weeks, months)
        roots.append(root)
        return func, root

    yield builder

    for root in roots:
        cleanup_trend_tfr_mod(root)


def module_name(func) -> str:
    """The generated module a built function came from. Unpredictable by
    design: the name carries a unique token so two operators sharing a
    retention cannot collide on it."""
    return func.__module__


def retention_prefix(days=DAYS, weeks=WEEKS, months=MONTHS) -> str:
    return f"dq_trend_{days}d_{weeks}w_{months}m_"


def path_of(root) -> Path:
    """The directory a build handle owns. Registration is handed this, while
    the handle itself stays with the caller."""
    return Path(root.name)


def test_build_writes_the_module_under_the_returned_root(build):
    func, root = build()
    assert (path_of(root) / f"{module_name(func)}.py").is_file()


def test_build_leaves_sys_path_alone(build):
    # The module is loaded by location, so the root never has to be prepended
    # to the import path and never has to be unwound from it.
    before = list(sys.path)
    _, root = build()
    assert sys.path == before
    cleanup_trend_tfr_mod(root)
    assert sys.path == before


def _boom(module) -> None:
    raise RuntimeError("module body failed")


def test_a_module_that_fails_to_load_leaves_nothing_behind(monkeypatch, tmp_path):
    # Half a build is worse than none: a registered-but-unexecuted module would
    # shadow every later build of the same name, and the directory would linger.
    real_spec_from = importlib.util.spec_from_file_location

    def exploding(name, location):
        spec = real_spec_from(name, location)
        spec.loader.exec_module = _boom
        return spec

    monkeypatch.setattr(importlib.util, "spec_from_file_location", exploding)
    monkeypatch.setattr(tempfile, "tempdir", str(tmp_path))

    before_modules = {n for n in sys.modules if n.startswith("dq_trend_")}

    with pytest.raises(RuntimeError):
        build_trend_tfr_mod(DAYS, WEEKS, MONTHS)

    assert {n for n in sys.modules if n.startswith("dq_trend_")} == before_modules
    assert list(tmp_path.glob("td_dq_trend_*")) == []


def test_the_built_function_is_serializable_as_module_and_qualname(build):
    # The whole reason for generating a module: `callable_to_str` refuses a
    # closure, and a closure is what a factory would otherwise return.
    func, _ = build()
    assert callable_to_str(func) == f"{module_name(func)}:dq_trend_tfr"


def test_the_built_function_takes_exactly_the_two_transformer_inputs(build):
    # The signature check allows two input slots; the retention values are
    # literals precisely so they do not appear here as extra parameters.
    func, _ = build()
    assert list(inspect.signature(func).parameters) == ["summary", "prev"]


def test_the_built_function_matches_a_direct_call_with_the_same_retention(build):
    func, _ = build(days=5, weeks=3, months=2)
    summary_tf = summary(MONDAY, "txn-1")
    assert rows_of(func(summary_tf, None)).equals(
        rows_of(dq_trend_tfr(summary_tf, None, days=5, weeks=3, months=2))
    )


def test_retention_is_baked_into_the_module(build):
    # Not defaulted: a build with no day retention keeps no run rows, and one
    # with day retention does, from the same summary.
    summary_tf = summary(MONDAY, "txn-1")

    keeps_nothing, _ = build(days=0, weeks=WEEKS, months=MONTHS)
    assert grain(keeps_nothing(summary_tf, None), Grain.RUN).is_empty()

    keeps_runs, _ = build(days=7, weeks=WEEKS, months=MONTHS)
    assert not grain(keeps_runs(summary_tf, None), Grain.RUN).is_empty()


def test_each_retention_configuration_gets_its_own_module(build):
    first, first_root = build(days=1, weeks=1, months=1)
    second, second_root = build(days=2, weeks=2, months=2)
    assert first_root.name != second_root.name
    assert module_name(first).startswith(retention_prefix(1, 1, 1))
    assert module_name(second).startswith(retention_prefix(2, 2, 2))


def test_the_same_configuration_twice_gets_its_own_module(build):
    # Two Summary operators commonly share a retention. Naming the module after
    # the retention alone made the second load return the first one from the
    # module cache, leaving it bound to the other operator's directory.
    first, first_root = build()
    second, second_root = build()

    assert first_root.name != second_root.name
    assert first is not second
    assert module_name(first) != module_name(second)
    for func, root in ((first, first_root), (second, second_root)):
        assert Path(sys.modules[module_name(func)].__file__).parent == path_of(root)


def test_cleanup_removes_the_generated_module_from_sys_modules(build):
    func, root = build()
    assert module_name(func) in sys.modules
    cleanup_trend_tfr_mod(root)
    assert module_name(func) not in sys.modules


def test_cleanup_deletes_the_directory(build):
    _, root = build()
    directory = path_of(root)
    cleanup_trend_tfr_mod(root)
    assert not directory.exists()


def test_cleanup_leaves_other_generated_modules_alone(build):
    kept, kept_root = build(days=1, weeks=1, months=1)
    _, dropped_root = build(days=2, weeks=2, months=2)

    cleanup_trend_tfr_mod(dropped_root)

    assert module_name(kept) in sys.modules
    assert path_of(kept_root).is_dir()


def test_cleanup_is_safe_to_repeat(build):
    _, root = build()
    cleanup_trend_tfr_mod(root)
    cleanup_trend_tfr_mod(root)


def test_the_same_retention_can_be_rebuilt_after_cleanup(build):
    _, root = build()
    cleanup_trend_tfr_mod(root)

    rebuilt, rebuilt_root = build()
    assert rebuilt_root.name != root.name
    assert grain(rebuilt(summary(MONDAY, "txn-1"), None), Grain.RUN).height == 1


def test_dropping_the_handle_deletes_the_directory():
    # This is the contract the return type exists to make explicit: the handle
    # owns the directory, so whatever holds the built function has to hold the
    # handle too. `TrendTransformer` is what does that, from the moment the
    # module is generated until registration has bundled it.
    func, root = build_trend_tfr_mod(DAYS, WEEKS, MONTHS)
    directory = path_of(root)
    assert directory.is_dir()

    name = module_name(func)
    del root, func
    gc.collect()

    assert not directory.exists()
    sys.modules.pop(name, None)
