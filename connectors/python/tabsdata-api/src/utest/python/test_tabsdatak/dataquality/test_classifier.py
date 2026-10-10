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


def test_classifier_positional_and_keyword_only_signature():
    clsf = dq.IsBetween(["a"], 1, 10, "lower", tags=["t"])
    assert clsf.column_names == ["a"]
    assert clsf.min_val == 1
    assert clsf.max_val == 10
    assert clsf.closed_on == "lower"
    assert clsf.tags == ["t"]
    # tags is keyword-only: an extra positional is rejected.
    with pytest.raises(ParameterValidationError):
        # noinspection PyArgumentList
        dq.IsBetween(["a"], 1, 10, "lower", ["t"])


def test_column_names_accepts_str_and_tuple_elements_no_coercion():
    assert dq.IsNull(["a", ("b", "c")]).column_names == ["a", ("b", "c")]
    # A bare (non-list) column spec is rejected: no scalar->list coercion.
    with pytest.raises(ParameterValidationError):
        # noinspection PyTypeChecker
        dq.IsNull("a")


def test_tags_must_be_a_list_no_coercion():
    with pytest.raises(ParameterValidationError):
        # noinspection PyTypeChecker
        dq.IsNull(["a"], tags="t")


def test_hardcoded_knobs_are_class_constants():
    clsf = dq.IsNull(["a"])
    # IsNull hardcodes on_wrong_type / on_wrong_value: they are class constants,
    # always "ignore", and are not serialized fields.
    assert clsf.on_wrong_type == "ignore"
    assert clsf.on_wrong_value == "ignore"
    spec = clsf.to_dict()["spec"]
    assert "on_wrong_type" not in spec
    assert "on_wrong_value" not in spec


def test_classifiers_are_frozen():
    clsf = dq.IsPositive(["a"])
    with pytest.raises(dataclasses.FrozenInstanceError):
        clsf.on_missing_column = "fail"


def test_is_in_values_is_a_deduplicated_frozenset():
    clsf = dq.IsIn(["a"], values=[1, 2, 2, 3])
    assert clsf.values == frozenset({1, 2, 3})
    # None is a legal member (tests null membership).
    assert dq.IsIn(["a"], values={None, 0}).values == frozenset({None, 0})
    # values is required and must be non-empty.
    with pytest.raises(ParameterValidationError):
        dq.IsIn(["a"], values=[])
    # values must be a collection (no scalar coercion).
    with pytest.raises(ParameterValidationError):
        # noinspection PyTypeChecker
        dq.IsIn(["a"], values=5)


def test_supported_dtypes_are_class_scoped():
    from tabsdatak.tableframe._engine.gtypes import (
        _BOOLEAN_DTYPES,
        _SIGNED_NUMERIC_DTYPES,
        _STRING_DTYPES,
    )

    assert dq.IsTrue.supported_dtypes() == _BOOLEAN_DTYPES
    assert dq.IsPositive.supported_dtypes() == _SIGNED_NUMERIC_DTYPES
    assert dq.Matches.supported_dtypes() == _STRING_DTYPES


@pytest.mark.parametrize(
    "factory, code",
    [
        (lambda: dq.IsBetween(["a"], 10, 5), DataQualityErrorCode.DQC_136),
        (lambda: dq.IsBetween(["a"], 1, "x"), DataQualityErrorCode.DQC_136),
        (lambda: dq.HasLength(["a"], 5, 1), DataQualityErrorCode.DQC_147),
        (lambda: dq.Matches(["a"], pattern="[unclosed"), DataQualityErrorCode.DQC_145),
        (lambda: dq.LinearScale(10, 5), DataQualityErrorCode.DQC_107),
        (lambda: dq.IdentityScale(0, 200), DataQualityErrorCode.DQC_107),
        (
            lambda: dq.LogarithmicScale(1.0, 1000.0, base=10.0),
            DataQualityErrorCode.DQC_107,
        ),
        (
            lambda: dq.LogarithmicScale(-2000.0, 0.0, base=0.5),
            DataQualityErrorCode.DQC_107,
        ),
    ],
)
def test_value_invariants_raise_specific_codes(factory, code):
    with pytest.raises(ParameterValidationError) as exc:
        factory()
    assert exc.value.api_error is code


def test_scale_categorizer_round_trips_polymorphic_scale():
    clsf = dq.ScaleCategorizer(["a"], scale=dq.LinearScale(0, 10, bins=5), tags=["t"])
    back = dq.ScaleCategorizer.from_json(clsf.to_json())
    assert isinstance(back.scale, dq.LinearScale)
    assert back.scale.bins == 5
    assert back.tags == ["t"]


@pytest.mark.parametrize(
    "clsf",
    [
        dq.IsTrue(["a"]),
        dq.IsNull(["a", ("b", "c")], tags=["x"]),
        dq.IsBetween(["a"], 1, 10, "upper"),
        dq.IsIn(["a"], values={"M", "F"}),
        dq.Matches(["a"], pattern=r"\d+"),
        dq.HasLength(["a"], 1, 5),
        dq.ScaleCategorizer(["a"], scale=dq.MonomialScale(0, 10, 2.0)),
    ],
)
def test_classifier_round_trip(clsf):
    back = type(clsf).from_json(clsf.to_json())
    assert back == clsf
