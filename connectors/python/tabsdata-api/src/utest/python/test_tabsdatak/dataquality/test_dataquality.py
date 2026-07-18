#
# Copyright 2026 Tabsdata Inc.
#

from __future__ import annotations

import pytest

import tabsdatak.api as m
import tabsdatak.dataquality as dq
from tabsdatak.dataquality._error import (
    DataQualityErrorCode,
    ParameterValidationError,
)

pytestmark = pytest.mark.dq


def _action() -> dq.DataQuality:
    return dq.DataQuality(
        "my_table",
        classifiers=[dq.IsPositive(["age"]), dq.IsIn(["g"], values={"M", "F"})],
        operators=[dq.Summary(), dq.Fail(dq.AnyFailed(), dq.PercentThreshold(10))],
    )


def test_data_quality_holds_lists_without_coercion():
    action = _action()
    assert action.table == "my_table"
    assert [type(c).__name__ for c in action.classifiers] == ["IsPositive", "IsIn"]
    assert [type(o).__name__ for o in action.operators] == ["Summary", "Fail"]
    # A single classifier/operator is not coerced into a list.
    with pytest.raises(ParameterValidationError):
        # noinspection PyTypeChecker
        dq.DataQuality("t", classifiers=dq.IsPositive(["a"]), operators=[dq.Summary()])


def test_classifiers_and_operators_must_be_non_empty():
    with pytest.raises(ParameterValidationError) as exc:
        dq.DataQuality("t", classifiers=[], operators=[dq.Summary()])
    assert exc.value.api_error is DataQualityErrorCode.DQC_201
    with pytest.raises(ParameterValidationError):
        dq.DataQuality("t", classifiers=[dq.IsNull(["a"])], operators=[])


def test_data_quality_round_trip():
    action = _action()
    back = dq.DataQuality.from_json(action.to_json())
    assert back == action


def test_data_quality_loads_polymorphically_via_on_tables_action():
    action = _action()
    back = m.OnTablesAction.from_json(action.to_json())
    assert isinstance(back, dq.DataQuality)
    assert back == action
