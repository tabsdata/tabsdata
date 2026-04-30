#
# Copyright 2026 Tabs Data Inc.
#

import math

import pytest

from tabsdata._utils.validation import (
    _get_parameter_type_hints,
    _normalize_to_list,
    _normalize_to_set,
    _td_validate_call,
)
from tabsdata.exceptions import ErrorCode, ParameterValidationError

# noinspection PyUnresolvedReferences
from . import pytestmark  # noqa: F401


# ---------------------------------------------------------------------------
# _normalize_to_set
# ---------------------------------------------------------------------------


class TestNormalizeToSet:
    def test_none_returns_empty_set(self):
        assert _normalize_to_set(None) == set()

    def test_none_with_keep_none(self):
        assert _normalize_to_set(None, keep_none=True) is None

    def test_single_value(self):
        result = _normalize_to_set(42)
        assert result == {42}

    def test_list_deduplicates(self):
        result = _normalize_to_set([1, 2, 2, 3])
        assert result == {1, 2, 3}

    def test_string_becomes_set(self):
        result = _normalize_to_set("abc")
        assert result == {"a", "b", "c"}

    def test_set_passthrough(self):
        result = _normalize_to_set({1, 2, 3})
        assert result == {1, 2, 3}

    def test_nan_handling(self):
        result = _normalize_to_set([1.0, float("nan"), float("nan"), 2.0])
        nan_count = sum(1 for v in result if isinstance(v, float) and math.isnan(v))
        assert nan_count == 1
        assert 1.0 in result
        assert 2.0 in result


# ---------------------------------------------------------------------------
# _normalize_to_list
# ---------------------------------------------------------------------------


class TestNormalizeToList:
    def test_none_returns_empty_list(self):
        assert _normalize_to_list(None) == []

    def test_none_with_keep_none(self):
        assert _normalize_to_list(None, keep_none=True) is None

    def test_single_value_wrapped(self):
        assert _normalize_to_list(42) == [42]

    def test_list_passthrough(self):
        assert _normalize_to_list([1, 2, 3]) == [1, 2, 3]

    def test_string_wrapped(self):
        assert _normalize_to_list("hello") == ["hello"]

    def test_tuple_wrapped(self):
        assert _normalize_to_list((1, 2)) == [(1, 2)]


# ---------------------------------------------------------------------------
# _get_parameter_type_hints
# ---------------------------------------------------------------------------


class TestGetParameterTypeHints:
    def test_basic_type_hint(self):
        def fn(x: int, y: str): pass
        assert "int" in _get_parameter_type_hints(fn, "x")
        assert "str" in _get_parameter_type_hints(fn, "y")

    def test_missing_param_returns_any(self):
        def fn(x: int): pass
        assert _get_parameter_type_hints(fn, "nonexistent") == "Any"

    def test_no_hints_returns_any(self):
        def fn(x): pass
        assert _get_parameter_type_hints(fn, "x") == "Any"


# ---------------------------------------------------------------------------
# _td_validate_call
# ---------------------------------------------------------------------------


class TestTdValidateCall:
    def test_valid_call_passes(self):
        @_td_validate_call(ErrorCode.DCE1)
        def add(a: int, b: int) -> int:
            return a + b

        assert add(1, 2) == 3

    def test_invalid_type_raises_parameter_validation_error(self):
        @_td_validate_call(ErrorCode.DCE1)
        def add(a: int, b: int) -> int:
            return a + b

        with pytest.raises(ParameterValidationError):
            add("not_an_int", "also_not")

    def test_preserves_function_name(self):
        @_td_validate_call(ErrorCode.DCE1)
        def my_function(x: int) -> int:
            return x

        assert my_function.__name__ == "my_function"

    def test_original_exception_reraise(self):
        @_td_validate_call(ErrorCode.DCE1)
        def fn(x: int):
            if x < 0:
                raise ValueError("negative!")
            return x

        with pytest.raises(ValueError, match="negative"):
            fn(-1)
