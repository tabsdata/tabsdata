#
# Copyright 2026 Tabsdata Inc.
#

from __future__ import annotations

import dataclasses
import json

import pytest

import tabsdatak.dataquality as dq
from tabsdatak.dataquality._error import (
    DataQualityErrorCode,
    ParameterValidationError,
)

pytestmark = pytest.mark.dq


def test_bool_criteria_defaults_and_keyword_only_tags():
    ok = dq.AllOk()
    assert ok.none_is_ok is False
    assert ok.nan_is_ok is False
    assert ok.tags is None
    ok = dq.AnyFailed(True, True, tags=["t"])
    assert ok.none_is_ok is True
    assert ok.tags == ["t"]
    with pytest.raises(ParameterValidationError):
        # noinspection PyArgumentList
        dq.AllOk(False, False, ["t"])


def test_category_criteria_bins_is_frozenset_no_scalar_coercion():
    crit = dq.InBins("col", bins=[0, 1, 1, "nan"])
    assert crit.column_name == "col"
    assert crit.bins == frozenset({0, 1, "nan"})
    with pytest.raises(ParameterValidationError):
        # noinspection PyTypeChecker
        dq.InBins("col", bins=0)


def test_operators_are_frozen():
    op = dq.Summary()
    with pytest.raises(dataclasses.FrozenInstanceError):
        op.table = "t"


def test_select_requires_criteria_and_to_table():
    with pytest.raises(ParameterValidationError) as exc:
        # noinspection PyArgumentList
        dq.Select()
    assert exc.value.api_error is DataQualityErrorCode.DQC_28


def test_thresholds():
    assert dq.RowCountThreshold(5).row_count == 5
    assert dq.PercentThreshold(10.5).percent == 10.5
    with pytest.raises(ParameterValidationError):
        dq.RowCountThreshold(-1)
    with pytest.raises(ParameterValidationError):
        dq.PercentThreshold(200)


def test_fail_round_trips_polymorphic_criteria_and_threshold():
    op = dq.Fail(dq.AnyFailed(tags=["x"]), dq.PercentThreshold(10))
    back = dq.Fail.from_json(op.to_json())
    assert isinstance(back.criteria, dq.AnyFailed)
    assert back.criteria.tags == ["x"]
    assert isinstance(back.threshold, dq.PercentThreshold)
    assert back.threshold.percent == 10


@pytest.mark.parametrize(
    "op",
    [
        dq.Enrich("out", tags=["t"]),
        dq.Summary(),
        dq.Filter(dq.AllOk(), to_table="out", include_quality_columns="all"),
        dq.Select(dq.AllOk(), "out"),
        dq.Fail(dq.AnyFailed(), dq.RowCountThreshold(3)),
    ],
)
def test_operator_round_trip(op):
    back = type(op).from_json(op.to_json())
    assert back == op


@pytest.mark.parametrize("policy", ["criteria", "all"])
def test_filter_quality_columns_require_a_redirect_table(policy):
    with pytest.raises(ParameterValidationError) as exc:
        dq.Filter(dq.AnyFailed(), include_quality_columns=policy)
    assert exc.value.api_error is DataQualityErrorCode.DQC_26
    assert policy in str(exc.value)


def test_filter_quality_columns_are_allowed_with_a_redirect_table():
    operator = dq.Filter(
        dq.AnyFailed(), to_table="rejected", include_quality_columns="all"
    )
    assert operator.include_quality_columns == "all"
    assert dq.Filter(dq.AnyFailed()).include_quality_columns == "none"

def test_trends_defaults():
    trend = dq.Trends()
    assert trend.table is None
    assert (trend.days, trend.weeks, trend.months) == (7, 12, 24)


def test_trends_is_frozen():
    trend = dq.Trends()
    with pytest.raises(dataclasses.FrozenInstanceError):
        trend.days = 1


def test_trends_takes_its_fields_in_order():
    trend = dq.Trends("trend_table", 1, 2, 3)
    assert trend.table == "trend_table"
    assert (trend.days, trend.weeks, trend.months) == (1, 2, 3)


def test_trends_table_must_be_a_valid_table_name():
    assert dq.Trends(table="my_trend").table == "my_trend"
    for invalid in ("bad name!", ""):
        with pytest.raises(ParameterValidationError) as exc:
            dq.Trends(table=invalid)
        assert exc.value.api_error is DataQualityErrorCode.DQC_32


@pytest.mark.parametrize("tier", ["days", "weeks", "months"])
def test_trends_retention_may_be_zero(tier):
    # Zero is opting a tier out, not an invalid retention: `months=0` keeps no
    # month rows at all.
    assert getattr(dq.Trends(**{tier: 0}), tier) == 0


def test_trends_cannot_disable_every_tier():
    # Opting one tier out is a configuration; opting all three out would build
    # a table that keeps nothing, which is what leaving `trends` unset is for.
    with pytest.raises(ParameterValidationError) as exc:
        dq.Trends(days=0, weeks=0, months=0)
    assert exc.value.api_error is DataQualityErrorCode.DQC_33


@pytest.mark.parametrize("tier", ["days", "weeks", "months"])
def test_trends_retention_cannot_be_negative(tier):
    with pytest.raises(ParameterValidationError) as exc:
        dq.Trends(**{tier: -1})
    assert exc.value.api_error is DataQualityErrorCode.DQC_32


@pytest.mark.parametrize("tier", ["days", "weeks", "months"])
@pytest.mark.parametrize("value", [7.0, "7", True])
def test_trends_retention_is_a_strict_int(tier, value):
    # Strict: a float, a numeric string, or a bool would each otherwise reach
    # the generated module as a literal and produce a nonsense window.
    with pytest.raises(ParameterValidationError) as exc:
        dq.Trends(**{tier: value})
    assert exc.value.api_error is DataQualityErrorCode.DQC_32


def test_trends_round_trip():
    trend = dq.Trends(table="t", days=1, weeks=2, months=3)
    assert dq.Trends.from_json(trend.to_json()) == trend


def test_trends_serializes_under_its_own_kind():
    assert dq.Trends().__kind__ == "dqTrends"
    assert '"kind":"dqTrends"' in dq.Trends().to_json()


def test_summary_has_no_trend_by_default():
    # The trend table is opt-in: a bare `Summary()` declares none, so no system
    # transformer appears for it.
    assert dq.Summary().trends is None


def test_summary_can_opt_into_the_default_trend():
    assert dq.Summary(trends=dq.Trends()).trends == dq.Trends()


def test_summary_round_trips_with_its_trend():
    summary = dq.Summary(table="s", trends=dq.Trends(days=1, weeks=2, months=3))
    back = dq.Summary.from_json(summary.to_json())
    assert back == summary
    assert isinstance(back.trends, dq.Trends)
    assert (back.trends.days, back.trends.weeks, back.trends.months) == (1, 2, 3)


def test_summary_round_trips_without_a_trend():
    summary = dq.Summary()
    assert '"trends":null' in summary.to_json()
    assert dq.Summary.from_json(summary.to_json()).trends is None


def test_summary_written_before_trends_existed_has_no_trend():
    # A `Summary` stored by a release that had no `trends` field at all. The key
    # is absent, so it deserializes with no trend.
    old = json.dumps({
        "kind": "dqSummary",
        "apiVersion": "1.0",
        "type": "tabsdatak.dataquality._operator:Summary",
        "spec": {"tags": None, "table": "my_summary"},
    })

    back = dq.Summary.from_json(old)

    assert back.table == "my_summary"
    assert back.trends is None
