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


def _between(columns, max_val: float) -> dq.IsBetween:
    return dq.IsBetween(columns, min_val=0.0, max_val=max_val)


def _with_classifiers(*classifiers: dq.Classifier) -> dq.DataQuality:
    return dq.DataQuality("t", classifiers=list(classifiers), operators=[dq.Summary()])


def test_two_classifiers_inferring_one_verdict_column_are_rejected():
    with pytest.raises(ParameterValidationError) as exc:
        _with_classifiers(_between(["d"], 15.0), _between(["d"], 40.0))
    assert exc.value.api_error is DataQualityErrorCode.DQC_202
    assert "source column 'd'" in str(exc.value)
    assert "(column, verdict_column)" in str(exc.value)


def test_two_classifiers_sharing_an_explicit_verdict_column_are_rejected():
    with pytest.raises(ParameterValidationError) as exc:
        _with_classifiers(
            _between([("d", "verdict")], 15.0), _between([("d", "verdict")], 40.0)
        )
    assert exc.value.api_error is DataQualityErrorCode.DQC_202
    assert "'verdict'" in str(exc.value)


def test_naming_the_verdict_columns_resolves_the_collision():
    action = _with_classifiers(
        _between([("d", "d_within_15")], 15.0), _between([("d", "d_within_40")], 40.0)
    )
    assert len(action.classifiers) == 2


def test_classifiers_of_different_types_on_one_column_are_allowed():
    action = _with_classifiers(_between(["d"], 15.0), dq.IsPositive(["d"]))
    assert len(action.classifiers) == 2


def test_verdict_column_equal_to_its_source_column_is_rejected():
    with pytest.raises(ParameterValidationError) as exc:
        _with_classifiers(_between([("d", "d")], 15.0))
    assert exc.value.api_error is DataQualityErrorCode.DQC_204


@pytest.mark.parametrize("name", ["$td.x", "@td.x", "@td.dq.d_is_between"])
def test_verdict_column_with_a_reserved_prefix_is_rejected(name: str):
    with pytest.raises(ParameterValidationError) as exc:
        _with_classifiers(_between([("d", name)], 15.0))
    assert exc.value.api_error is DataQualityErrorCode.DQC_203


def test_a_single_wildcard_classifier_is_allowed():
    action = _with_classifiers(_between(None, 15.0))
    assert action.classifiers[0].column_names is None
