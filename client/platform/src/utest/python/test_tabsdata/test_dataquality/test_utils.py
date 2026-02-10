#
#  Copyright 2025. Tabs Data Inc.
#

from typing import Annotated, List, Literal, Optional, TypeAlias, Union

import pytest
from pydantic import Field, Strict, StrictInt

from tabsdata._utils.validation import (
    _get_parameter_type_hints,
    _normalize_to_list,
    _normalize_to_set,
    _td_validate_call,
)
from tabsdata.exceptions import ErrorCode, ParameterValidationError


# Tests for _normalize_to_list
def test_normalize_to_list_with_none():
    assert _normalize_to_list(None) == []


def test_normalize_to_list_with_none_and_keep_none():
    assert _normalize_to_list(None, keep_none=True) is None


def test_normalize_to_list_with_single_value():
    assert _normalize_to_list("a") == ["a"]
    assert _normalize_to_list(1) == [1]


def test_normalize_to_list_with_list():
    assert _normalize_to_list(["a", "b"]) == ["a", "b"]


# Tests for _normalize_to_set
def test_normalize_to_set_with_none():
    assert _normalize_to_set(None) == set()


def test_normalize_to_set_with_none_and_keep_none():
    assert _normalize_to_set(None, keep_none=True) is None


def test_normalize_to_set_with_single_value():
    assert _normalize_to_set("a") == {"a"}
    assert _normalize_to_set(1) == {1}


def test_normalize_to_set_with_list():
    assert _normalize_to_set(["a", "b"]) == {"a", "b"}


def test_normalize_to_set_with_set():
    assert _normalize_to_set({"a", "b"}) == {"a", "b"}


def test_normalize_to_set_with_tuple():
    assert _normalize_to_set(("a", "b")) == {"a", "b"}


def test_normalize_to_set_with_duplicates():
    assert _normalize_to_set(["a", "b", "a"]) == {"a", "b"}


def test_normalize_to_set_with_nan():
    # Note: float('nan') is not equal to float('nan'), so we need to handle this in the test
    result = _normalize_to_set([1, float("nan"), 2, float("nan")])
    assert len(result) == 3
    assert 1 in result
    assert 2 in result
    assert any(isinstance(x, float) and str(x) == "nan" for x in result)


# Tests for _get_parameter_type_hints
def sample_function(a: int, b: str, c: Optional[List[str]], d) -> None:
    pass


def test_get_parameter_type_hints_with_simple_types():
    assert _get_parameter_type_hints(sample_function, "a") == "int"
    assert _get_parameter_type_hints(sample_function, "b") == "str"


def test_get_parameter_type_hints_with_complex_types():
    assert _get_parameter_type_hints(sample_function, "c") == "Optional[List[str]]"


def test_get_parameter_type_hints_with_no_type_hint():
    assert _get_parameter_type_hints(sample_function, "d") == "Any"


def test_get_parameter_type_hints_with_nonexistent_param():
    assert _get_parameter_type_hints(sample_function, "x") == "Any"


# Tests for _td_validate_call

P0Spec: TypeAlias = Literal["a", "b"]
P1Spec: TypeAlias = Union[int, str]
P2Spec: TypeAlias = Annotated[StrictInt, Field(ge=0)]
P3Spec: TypeAlias = Annotated[list[str], Strict]
P4Spec: TypeAlias = list[dict[int, str]]


@_td_validate_call(ErrorCode.DQC0)
def decorated_function(
    p0: P0Spec = None,
    p1: P1Spec = None,
    p2: P2Spec = None,
    p3: P3Spec = None,
    *,
    p4: P4Spec = None,
    p00: P0Spec = None,
    **kwargs
):
    pass


def test_decorated_function_good_values():
    # Test with valid values
    decorated_function(p0="a", p1=1, p2=0, p3=["a", "b"], p4=[{1: "a"}])
    decorated_function(p0="b", p1="test", p2=100, p3=[], p4=[])
    decorated_function()  # All have default None


def test_decorated_function_wrong_p0():
    with pytest.raises(ParameterValidationError) as excinfo:
        decorated_function(p0="c")
    errors = excinfo.value.errors
    assert len(errors) == 1
    assert errors[0].name == "p0"
    assert errors[0].type == "Literal['a', 'b']"


def test_decorated_function_wrong_p1():
    with pytest.raises(ParameterValidationError) as excinfo:
        decorated_function(p1=1.5)
    errors = excinfo.value.errors
    assert len(errors) == 1
    assert errors[0].name == "p1"
    assert errors[0].type == "Union[int, str]"


def test_decorated_function_wrong_p2():
    with pytest.raises(ParameterValidationError) as excinfo:
        decorated_function(p1=1.5, p2=-1)
    errors = excinfo.value.errors
    assert len(errors) == 2
    assert errors[0].name == "p1"
    assert errors[0].type == "Union[int, str]"
    assert errors[1].name == "p2"
    assert (
        errors[1].type
        == "Annotated[int, Strict(strict=True), FieldInfo(annotation=NoneType,"
        " required=True, metadata=[Ge(ge=0)])]"
    )
