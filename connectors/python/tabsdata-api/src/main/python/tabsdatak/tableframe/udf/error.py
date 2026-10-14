#
# Copyright 2026 Tabsdata Inc.
#

from __future__ import annotations

from tabsdatak.error import (
    ErrorCode,
    ErrorDef,
    TabsdataException,
)


class UDFException(TabsdataException):
    """Base class for user defined function errors.

    :categories: error
    """

    def __init__(self, api_error: ErrorCode | str | None = None, *args, **kwargs):
        # Polars re-raises an error from inside a UDF as type(error)(text), the
        # text being the original message plus the failing expression; the error
        # code is recovered from the "UDF_N: " prefix of that text.
        if isinstance(api_error, str):
            text = api_error
            code_name, _, message = text.partition(": ")
            api_error = UDFErrorCode.__members__.get(code_name)
            if api_error is None:
                api_error, message = UDFErrorCode.UDF_0, text
            super().__init__(api_error, message)
            return
        super().__init__(api_error, *args, **kwargs)


class UDFDefinitionException(UDFException):
    """The user defined function is not defined correctly.

    :categories: error
    """


class UDFOutputException(UDFException):
    """The user defined function returned output that does not match what it
    declared.

    :categories: error
    """


class UDFErrorCode(ErrorCode):
    """Error codes for user defined functions.

    :categories: error

    Raise via `UDFErrorCode.UDF_N.exception`.
    """

    UDF_0 = ErrorDef(
        UDFException,
        "{udf} raised {error}",
    )
    UDF_1 = ErrorDef(
        UDFDefinitionException,
        "{udf} cannot be instantiated directly; create a subclass instead.",
    )
    UDF_2 = ErrorDef(
        UDFDefinitionException,
        "{udf} must implement exactly one of 'on_batch' or 'on_element'.",
    )
    UDF_3 = ErrorDef(
        UDFDefinitionException,
        "{udf} must not override '{member}'.",
    )
    UDF_4 = ErrorDef(
        UDFDefinitionException,
        "{udf}.__init__() did not call super().__init__(output_columns).",
    )
    UDF_5 = ErrorDef(
        UDFDefinitionException,
        "Output columns must be a (name, data type) tuple or a non-empty list of "
        "them; got {got}.",
    )
    UDF_6 = ErrorDef(
        UDFDefinitionException,
        "Output column at index {index} is invalid: {reason}.",
    )
    UDF_7 = ErrorDef(
        UDFDefinitionException,
        "Output column names must be unique; {names} appear more than once.",
    )
    UDF_8 = ErrorDef(
        UDFDefinitionException,
        "{udf} has signature {signature!r}; it must be one of {signatures}.",
    )
    UDF_9 = ErrorDef(
        UDFDefinitionException,
        "The user defined function needs at least one input column.",
    )
    UDF_10 = ErrorDef(
        UDFOutputException,
        "{udf}.{method} must return a list of {width} {kind}; got {got}.",
    )
    UDF_11 = ErrorDef(
        UDFOutputException,
        "{udf}.on_batch returned {length} values for output column {column!r}; "
        "it must return exactly one per input row ({rows}).",
    )
    UDF_12 = ErrorDef(
        UDFOutputException,
        "{udf} returned values for output column {column!r} that do not fit its "
        "data type {dtype}.",
    )
    UDF_13 = ErrorDef(
        UDFDefinitionException,
        "The user defined function input columns must be distinct; {names} "
        "appear more than once.",
    )
    UDF_14 = ErrorDef(
        UDFDefinitionException,
        "A user defined function must be callable; got {got}.",
    )
    UDF_15 = ErrorDef(
        UDFDefinitionException,
        "TableFrame.udf takes a UDF; got {got}. Turn a plain function into one "
        "with the on_batch or on_element decorators.",
    )
    UDF_16 = ErrorDef(
        UDFDefinitionException,
        "The user defined function input {input} reads a column the frame does "
        "not have.",
    )
