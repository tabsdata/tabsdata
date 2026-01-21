#
# Copyright 2025 Tabs Data Inc.
#
"""
This module provides utility functions for the data quality feature. These functions
are used for internal purposes such as normalizing data structures, validating
parameters, and handling exceptions.
"""

import math
import re
import typing
from functools import wraps
from typing import Any

from pydantic import ConfigDict, ValidationError, validate_call

from tabsdata.exceptions import ErrorCode, ParameterValidationError


def _normalize_to_set(
    val: Any | None, keep_none: bool = False
) -> None | typing.AbstractSet:
    """
    Normalizes a value into a set, ensuring deduplication if the value is a collection.

    - If `val` is `None`, returns an empty set unless `keep_none` is `True`.
    - If `val` is not a collection, it returns a set containing only that value.
    - If `val` is a collection, it returns a set with the elements of the collection.
    - Handles `NaN` values correctly by treating them as a single unique value.

    Args:
        val: The value to normalize.
        keep_none: If `True`, returns `None` when the input is `None`.

    Returns:
        The value normalized as a set, or `None` if `keep_none` is `True` and
        `val` is `None`.
    """
    if val is None:
        if keep_none:
            return None
        else:
            return set()
    if not isinstance(val, typing.Collection):
        val = [val]
    values = val
    has_nan = any(isinstance(value, float) and math.isnan(value) for value in values)
    values = {
        value
        for value in values
        if not (isinstance(value, float) and math.isnan(value))
    }
    if has_nan:
        values.add(float("nan"))
    return values


def _normalize_to_list(val: Any | None, keep_none: bool = False) -> list[Any] | None:
    """
    Normalizes a value into a list.

    - If `val` is `None`, returns an empty list unless `keep_none` is `True`.
    - If `val` is not a list, it returns a list containing that value.
    - If `val` is already a list, it is returned as is.

    Args:
        val: The value to normalize.
        keep_none: If `True`, returns `None` when the input is `None`.

    Returns:
        The value normalized as a list, or `None` if `keep_none` is `True` and
        `val` is `None`.
    """
    if val is None:
        if keep_none:
            return None
        else:
            return []
    return val if isinstance(val, list) else [val]


def _get_parameter_type_hints(fn: typing.Callable, param: str) -> str:
    """
    Returns a string representation of the type hint for a specific parameter.

    This function inspects a callable's signature and retrieves the type hint
    for the given parameter name. It simplifies complex type hints for better
    readability.

    Args:
        fn: The callable to inspect.
        param: The name of the parameter to get the type hint for.

    Returns:
        A string representing the parameter's type hint, or "Any" if not
        available.
    """
    params_hints = typing.get_type_hints(fn, include_extras=True)
    param_hints = params_hints.get(param)
    if param_hints is None:
        return "Any"
    param_hints = str(param_hints)
    param_hints = re.sub(r"[a-zA-Z0-9_]+\.", "", param_hints)
    param_hints = param_hints.replace("<class '", "").replace("'>", "")
    return param_hints


def _validation_param_error_message(
    name: str, type_hint: str, value: Any, error: str
) -> str:
    """
    Formats a detailed error message for a parameter validation failure.

    Args:
        name: The name of the parameter.
        type_hint: The expected type hint of the parameter.
        value: The actual value that caused the validation error.
        error: The error message from the validation library.

    Returns:
        A formatted string detailing the validation error.
    """
    return f"'{name}' : '{type_hint}' = '{value}' error: '{error}'"


def _validation_error(
    error_code: ErrorCode, f_name: str, f_params_with_error: str
) -> ParameterValidationError:
    """
    Creates a `ParameterValidationError` with a standardized message format.

    Args:
        error_code: The error code associated with this validation failure.
        f_name: The name of the function where the validation failed.
        f_params_with_error: A string containing details of the failing
            parameters.

    Returns:
        An instance of `ParameterValidationError`.
    """
    code = error_code.code()
    msg = (
        f"{code} - "
        "Parameter validation error, "
        f"function: '{f_name}', "
        f"parameter(s): {f_params_with_error}."
    )
    return ParameterValidationError(_validation_error, error_code, msg)


def _td_validate_call(error_code: ErrorCode, f=None):
    """
    A decorator that validates function arguments using Pydantic and raises a
    `ParameterValidationError` with a specific `ErrorCode` on failure.

    This decorator simplifies error handling by wrapping Pydantic's validation
    and converting its `ValidationError` into a custom exception.

    Usage:
        @_td_validate_call(ErrorCode.WCE5)
        def my_function(param1: int, param2: str):
            ...

    Args:
        error_code: The `ErrorCode` to use when raising a
            `ParameterValidationError`.
        f: The function to be decorated. This is typically not passed directly.

    Returns:
        A decorated function with argument validation.
    """
    if f is None:
        return lambda fn: _td_validate_call(error_code, fn)

    # Apply standard Pydantic validation
    validated_func = validate_call(f, config=ConfigDict(arbitrary_types_allowed=True))

    @wraps(f)
    def wrapper(*args, **kwargs):
        try:
            return validated_func(*args, **kwargs)
        except ValidationError as pyd_err:
            for err in pyd_err.errors():
                # 'ctx' (context) often contains the original exception object
                original_err = err.get("ctx", {}).get("error")
                if original_err:
                    raise original_err from None
            raise ParameterValidationError(f, error_code, pyd_err) from pyd_err

    return wrapper
