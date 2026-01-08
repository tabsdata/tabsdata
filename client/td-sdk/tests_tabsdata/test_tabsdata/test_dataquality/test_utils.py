#
#  Copyright 2025. Tabs Data Inc.
#

from typing import List, Optional

import pytest

from tabsdata.dataquality._utils import (
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
@_td_validate_call(ErrorCode.PVTEST0)
def decorated_function(a: int, b: str):
    return a, b


def test_td_validate_call_with_valid_args():
    assert decorated_function(1, "test") == (1, "test")


def test_td_validate_call_with_invalid_arg_type():
    with pytest.raises(ParameterValidationError) as exc_info:
        decorated_function("not-an-int", "test")

    assert exc_info.value.error_code == ErrorCode.PVTEST0
    assert "Parameter validation error" in str(exc_info.value)
    assert "function: 'decorated_function'" in str(exc_info.value)
    assert "parameter(s): 'a' : 'int' = 'not-an-int'" in str(exc_info.value)
    assert "error: " in str(exc_info.value)


def test_td_validate_call_with_invalid_kwarg_type():
    with pytest.raises(ParameterValidationError) as exc_info:
        decorated_function(a=1, b=123)

    assert exc_info.value.error_code == ErrorCode.PVTEST0
    assert "Parameter validation error" in str(exc_info.value)
    assert "function: 'decorated_function'" in str(exc_info.value)
    assert "'a' : 'int' = '1'" not in str(exc_info.value)
    assert "'b' : 'str' = '123'" in str(exc_info.value)
    assert "error: " in str(exc_info.value)


class SampleClass:
    @_td_validate_call(ErrorCode.PVTEST0)
    def __init__(self, a: int):
        self.a = a

    @_td_validate_call(ErrorCode.PVTEST0)
    def method(self, b: str):
        return self.a, b


def test_td_validate_call_on_class_method():
    instance = SampleClass(1)
    assert instance.method("test") == (1, "test")

    with pytest.raises(ParameterValidationError) as exc_info:
        instance.method(b=123)

    assert exc_info.value.error_code == ErrorCode.PVTEST0
    assert "Parameter validation error" in str(exc_info.value)
    assert "function: 'SampleClass.method'" in str(exc_info.value)
    assert "'b' : 'str' = '123'" in str(exc_info.value)
    assert "error: " in str(exc_info.value)


def test_td_validate_call_on_class_init():
    with pytest.raises(ParameterValidationError) as exc_info:
        SampleClass("not-an-int")

    assert exc_info.value.error_code == ErrorCode.PVTEST0
    assert "Parameter validation error" in str(exc_info.value)
    assert "function: 'SampleClass.__init__'" in str(exc_info.value)
    assert "'a' : 'int' = 'not-an-int'" in str(exc_info.value)
    assert "error: " in str(exc_info.value)


def test_td_validate_call_with_multiple_invalid_args():
    with pytest.raises(ParameterValidationError) as exc_info:
        decorated_function("not-an-int", 123)

    assert exc_info.value.error_code == ErrorCode.PVTEST0
    assert "Parameter validation error" in str(exc_info.value)
    assert "function: 'decorated_function'" in str(exc_info.value)
    assert "'a' : 'int' = 'not-an-int'" in str(exc_info.value)
    assert "'b' : 'str' = '123'" in str(exc_info.value)
    assert "error: " in str(exc_info.value)
