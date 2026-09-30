#
# Copyright 2026 Tabsdata Inc.
#

"""Rolling DQ Summary trends at three overlapping grains: exact runs for D days, weeks
for W weeks, months for M months.
"""

from __future__ import annotations

import importlib.util
import os
import sys
import tempfile
from datetime import datetime
from enum import Enum
from pathlib import Path
from typing import Callable
from uuid import uuid4

from tabsdatak.api import TableFrameSpec
from tabsdatak.common._actions.dataquality.engine import ClassifierFamily, DQColumns
from tabsdatak.tableframe import TableFrame
from tabsdatak.tableframe._engine.constant.column.names import (
    TD_TIMESTAMP_SYSTEM_COLUMN_NAME,
    TD_TRANSACTION_SYSTEM_COLUMN_NAME,
)
from tabsdatak.tableframe.datatypes import Float64, Int64, String
from tabsdatak.tableframe.functions import col, concat, lit


class TrendColumns(Enum):
    GRAIN = "grain"
    BUCKET = "bucket"
    CLASSIFIER = "classifier"
    RUNS = "runs"
    PASSED = "passed"
    FAILED = "failed"
    NULLS = "nulls"
    NANS = "nans"
    PASS_PCT = "pass_pct"
    TXN = "txn"
    FOLDED_THROUGH = "folded_through"


_NOW = "now"


class Grain(Enum):
    RUN = "run"
    WEEK = "week"
    MONTH = "month"
    FOLDED = "folded"


KEYS = [
    TrendColumns.GRAIN.value,
    TrendColumns.BUCKET.value,
    TrendColumns.CLASSIFIER.value,
]
COUNTS = [
    TrendColumns.RUNS.value,
    TrendColumns.PASSED.value,
    TrendColumns.FAILED.value,
    TrendColumns.NULLS.value,
    TrendColumns.NANS.value,
]
OUTPUT = [
    *KEYS,
    *COUNTS,
    TrendColumns.PASS_PCT.value,
    TrendColumns.TXN.value,
    TrendColumns.FOLDED_THROUGH.value,
]


def points_from_summary(summary: TableFrame) -> TableFrame:
    """One `run` row per boolean classifier of a summary version."""
    return (
        summary
        .filter(col(DQColumns.DQ_TYPE.value) == ClassifierFamily.BOOLEAN.value)
        .with_columns(
            lit(Grain.RUN.value).alias(TrendColumns.GRAIN.value),
            col(TD_TIMESTAMP_SYSTEM_COLUMN_NAME).alias(TrendColumns.BUCKET.value),
            col(DQColumns.DQ_CLASSIFIER.value).alias(TrendColumns.CLASSIFIER.value),
            lit(1).cast(Int64).alias(TrendColumns.RUNS.value),
            col(DQColumns.DQ_TRUE.value).cast(Int64).alias(TrendColumns.PASSED.value),
            col(DQColumns.DQ_FALSE.value).cast(Int64).alias(TrendColumns.FAILED.value),
            col(DQColumns.DQ_NONE.value).cast(Int64).alias(TrendColumns.NULLS.value),
            col(DQColumns.DQ_NAN.value).cast(Int64).alias(TrendColumns.NANS.value),
            col(TD_TRANSACTION_SYSTEM_COLUMN_NAME).alias(TrendColumns.TXN.value),
        )
        .select(*KEYS, *COUNTS, TrendColumns.TXN.value)
    )


def _tier(prev: TableFrameSpec, grain: str) -> TableFrameSpec:
    if prev is None:
        return None
    return prev.filter(col(TrendColumns.GRAIN.value) == grain).select(
        *KEYS, *COUNTS, TrendColumns.TXN.value
    )


def _fold(
    rows: TableFrameSpec, grain: str, every: str, sign: int = 1
) -> TableFrameSpec:
    """Sum run rows into `grain` buckets of width `every`.

    `sign=-1` negates the counts, so the fold retracts rather than adds. That
    is how a previous execution's contribution is backed out before the same
    rows are folded again (see `roll_trend`).
    """
    if rows is None:
        return None
    return (
        rows
        .with_columns(
            lit(grain).alias(TrendColumns.GRAIN.value),
            col(TrendColumns.BUCKET.value)
            .dt.truncate(every)
            .alias(TrendColumns.BUCKET.value),
        )
        .group_by(*KEYS)
        .agg(*((col(c).sum() * sign).alias(c) for c in COUNTS))
        .with_columns(lit(None, dtype=String).alias(TrendColumns.TXN.value))
    )


def _within(every: str) -> object:
    """Rows whose bucket, pushed forward by the tier's retention, is still ahead of
    now."""
    return col(TrendColumns.BUCKET.value).dt.offset_by(every) > col(_NOW)


def _folded_through(prev: TableFrameSpec) -> datetime | None:
    """The newest instant folded so far, which floors `now`."""
    if prev is None:
        return None
    mark = prev.max_for(TrendColumns.FOLDED_THROUGH.value)
    return mark and mark[TrendColumns.FOLDED_THROUGH.value]


def _ledger(prev: TableFrameSpec) -> TableFrameSpec:
    """The transactions whose contribution the coarse tiers already carry."""
    if prev is None:
        return None
    return prev.filter(col(TrendColumns.GRAIN.value) == Grain.FOLDED.value).select(
        *KEYS, *COUNTS, TrendColumns.TXN.value
    )


def _as_ledger(points: TableFrameSpec) -> TableFrameSpec:
    """One ledger row per transaction carried by `points`."""
    return points.with_columns(
        lit(Grain.FOLDED.value).alias(TrendColumns.GRAIN.value),
        lit(None, dtype=String).alias(TrendColumns.CLASSIFIER.value),
        *(lit(None, dtype=Int64).alias(c) for c in COUNTS),
    ).unique(subset=[TrendColumns.TXN.value])


def roll_trend(
    points: TableFrame,
    prev: TableFrameSpec,
    now: datetime,
    d_days: int,
    w_weeks: int,
    m_months: int,
) -> TableFrame:
    """Merge new run rows into the previous trend table and re-tier it.

    The tiers overlap: every run row contributes to its week and month bucket
    from its first execution, not only once it ages out of the run tier. So a
    week bucket covering the last `d_days` already counts those days, and a
    month bucket already counts the weeks inside it.
    """

    mark = _folded_through(prev)
    if mark is not None:
        now = max(now, mark)

    ledger = _ledger(prev)
    if ledger is not None:
        points = points.join(
            ledger.select(TrendColumns.TXN.value),
            on=TrendColumns.TXN.value,
            how="anti",
        )
    folded = concat([ledger, _as_ledger(points)])

    prev_runs = _tier(prev, Grain.RUN.value)
    runs = (
        concat([points, prev_runs])
        .unique(subset=[TrendColumns.CLASSIFIER.value, TrendColumns.TXN.value])
        .with_columns(lit(now).alias(_NOW))
    )
    fresh = runs.filter(_within(f"{d_days}d"))
    all_runs = runs.drop(_NOW)

    coarse = concat([
        _tier(prev, Grain.WEEK.value),
        _tier(prev, Grain.MONTH.value),
        _fold(prev_runs, Grain.WEEK.value, "1w", sign=-1),
        _fold(prev_runs, Grain.MONTH.value, "1mo", sign=-1),
        _fold(all_runs, Grain.WEEK.value, "1w"),
        _fold(all_runs, Grain.MONTH.value, "1mo"),
    ])
    if coarse is not None:
        coarse = (
            coarse
            .group_by(*KEYS)
            .agg(*(col(c).sum().alias(c) for c in COUNTS))
            .with_columns(
                lit(None, dtype=String).alias(TrendColumns.TXN.value),
                lit(now).alias(_NOW),
            )
            .filter(
                (
                    (col(TrendColumns.GRAIN.value) == Grain.WEEK.value)
                    & _within(f"{w_weeks}w")
                )
                | (
                    (col(TrendColumns.GRAIN.value) == Grain.MONTH.value)
                    & _within(f"{m_months}mo")
                )
            )
        )

    folded = folded.with_columns(lit(now).alias(_NOW)).filter(
        _within(f"{w_weeks}w") | _within(f"{m_months}mo")
    )

    return (
        concat([fresh, coarse, folded])
        .drop(_NOW)
        .with_columns(
            (
                col(TrendColumns.PASSED.value).cast(Float64)
                * 100.0
                / (col(TrendColumns.PASSED.value) + col(TrendColumns.FAILED.value))
            )
            .fill_nan(None)
            .alias(TrendColumns.PASS_PCT.value),
            lit(now).alias(TrendColumns.FOLDED_THROUGH.value),
        )
        .select(*OUTPUT)
        .sort(*KEYS)
    )


def dq_trend_tfr(
    summary: TableFrameSpec,
    prev: TableFrameSpec,
    days: int,
    weeks: int,
    months: int,
) -> TableFrameSpec:
    if summary is None:
        return None
    points = points_from_summary(summary)
    now = points.max_for(TrendColumns.BUCKET.value)
    if now is None:
        return None
    return roll_trend(
        points,
        prev,
        now[TrendColumns.BUCKET.value],
        d_days=days,
        w_weeks=weeks,
        m_months=months,
    )


def build_trend_tfr_mod(
    days: int,
    weeks: int,
    months: int,
) -> tuple[Callable, tempfile.TemporaryDirectory]:
    """Generate a registerable entry point for one retention configuration.

    ``TransformerInfo.func`` is serialized as ``"module:qualname"`` and nothing
    else: a closure's captured values would never reach the runner, and extra
    parameters are refused by the transformer signature check. So the retention
    values are written into a module as literals, and that module is bundled as
    the function's user code.

    Returns:
        The generated ``dq_trend_tfr`` and the handle owning the directory it
        lives in. The handle deletes that directory as soon as it is released,
        and registration reads it later, when it builds the bundle -- so the
        caller has to hold the handle until then, and pass ``root.name`` as
        the registration's ``project_root`` so the module lands in the
        bundle's ``user_code/``.
    """
    root = tempfile.TemporaryDirectory(prefix="td_dq_trend_")
    mod = f"dq_trend_{days}d_{weeks}w_{months}m_{uuid4().hex[:8]}"
    path = Path(root.name) / f"{mod}.py"
    path.write_text(f"""
from tabsdatak.api import TableFrameSpec
from tabsdatak.common._actions.dataquality.trend import (
    dq_trend_tfr as _dq_trend_tfr,
)


def dq_trend_tfr(summary: TableFrameSpec, prev: TableFrameSpec) -> TableFrameSpec:
    return _dq_trend_tfr(summary, prev, days={days}, weeks={weeks}, months={months})
""")

    spec = importlib.util.spec_from_file_location(mod, path)
    module = importlib.util.module_from_spec(spec)
    sys.modules[mod] = module
    try:
        spec.loader.exec_module(module)
    except BaseException:
        del sys.modules[mod]
        root.cleanup()
        raise
    return module.dq_trend_tfr, root


def cleanup_trend_tfr_mod(root: tempfile.TemporaryDirectory) -> None:
    """Discard a directory produced by :func:`build_trend_tfr_mod`.

    Releasing the handle is what deletes the directory; the rest undoes what
    loading the module did to the interpreter.
    """
    prefix = os.path.abspath(root.name) + os.sep
    for name, module in list(sys.modules.items()):
        file = getattr(module, "__file__", None)
        if file and os.path.abspath(file).startswith(prefix):
            del sys.modules[name]

    root.cleanup()
