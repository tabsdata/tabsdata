#
# Copyright 2026 Tabsdata Inc.
#

from __future__ import annotations

import dataclasses

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
