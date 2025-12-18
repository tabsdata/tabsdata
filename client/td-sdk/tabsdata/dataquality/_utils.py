#
# Copyright 2025 Tabs Data Inc.
#
import inspect
import re
import typing
from functools import wraps
from typing import Any, Optional

from pydantic import ConfigDict, ValidationError, validate_call

from tabsdata.exceptions import ErrorCode, ParameterValidationError


def _normalize_to_list(val: Optional[Any], keep_none: bool = False) -> None | list[Any]:
    """
    Normalizes a value into a list.

    * If the value is `None`, returns an empty list.
    * If the value is not a list, it returns a list with the value.
    * If the value is a list, it returns the value.

    :return: the value normalized as a list.
    """
    if val is None:
        if keep_none:
            return None
        else:
            return []
    return val if isinstance(val, list) else [val]


def _get_parameter_type_hints(fn: typing.Callable, param: str) -> str:
    """
    Returns the string representation of the type hint for a specific
    parameter.
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
    return f"'{name}' : '{type_hint}' = '{value}' error: '{error}"


def _validation_error(
    error_code: ErrorCode, f_name: str, f_params_with_error: str
) -> ParameterValidationError:
    code = error_code.code()
    msg = (
        f"{code} - "
        "Parameter validation error, "
        f"function: '{f_name}', "
        f"parameter(s): {f_params_with_error}."
    )
    return ParameterValidationError(error_code, msg)


def _td_validate_call(error_code: ErrorCode, f=None):
    """
    A decorator that validates arguments and uses the given `ErrorCode` to
    create and raise a `ValidationTabsdataException`.

    Usage:
        @td_validate_call(ErrorCode.WCE5)
        def my_fn(...)
    """

    if f is None:
        return lambda fn: _td_validate_call(error_code, fn)

    # Apply standard Pydantic validation
    validated_func = validate_call(f, config=ConfigDict(arbitrary_types_allowed=True))

    @wraps(f)
    def wrapper(*args, **kwargs):
        try:
            return validated_func(*args, **kwargs)
        except ValidationError as e:

            f_name = f.__name__ if isinstance(f, type) else f.__qualname__

            validation_params = []
            for error_details in e.errors():
                param = error_details["loc"][0]
                if isinstance(param, int):
                    signature = inspect.signature(f)
                    parameters = list(signature.parameters.keys())
                    param = parameters[param]
                param_value = error_details["input"]
                pydantic_msg = error_details["msg"]
                param_hint = _get_parameter_type_hints(f, param)
                validation_param = _validation_param_error_message(
                    param, param_hint, param_value, pydantic_msg
                )
                validation_params.append(validation_param)

            validation_error = _validation_error(
                error_code,
                f_name,
                ", ".join(validation_params),
            )
            raise validation_error from e

    return wrapper
