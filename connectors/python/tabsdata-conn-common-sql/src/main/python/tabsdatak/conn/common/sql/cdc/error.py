#
# Copyright 2026 Tabs Data Inc.
#

"""Error machinery for the shared CDC connector code.

Mirrors the per-connector ``error.py`` convention (see
:mod:`tabsdatak.conn.common.sql.error`): a single
:class:`~tabsdatak.error.ErrorCode` enum whose variants each carry an
:class:`~tabsdatak.error.ErrorDef` (exception class + message
template). Raise via :meth:`ErrorCode.exception`::

    raise SqlCdcErrorCode.CDC_2.exception(schema=schema, table=table)

:func:`cdc_validate_call` is the CDC-local replacement for the old
``tabsdata._utils.validation._td_validate_call``: it wraps a callable
with pydantic argument validation and converts a ``ValidationError``
into the bound :class:`SqlCdcException`.
"""

from __future__ import annotations

from functools import wraps
from typing import Any, Callable

from pydantic import (
    ConfigDict,
    ValidationError,
    validate_call as _pydantic_validate_call,
)

from tabsdatak.conn.common.error import ConnException
from tabsdatak.error import ErrorCode, ErrorDef


class SqlCdcException(ConnException):
    """Failure raised by the shared CDC machinery (table resolution,
    schema reflection, buffering / offset handling, spec validation).
    Connector-specific CDC code can translate this into its own
    per-connector codes when relevant."""


class SqlCdcErrorCode(ErrorCode):
    CDC_1 = ErrorDef(
        SqlCdcException,
        "Invalid table specification {spec!r}",
    )
    CDC_2 = ErrorDef(
        SqlCdcException,
        "Table {schema}.{table} not found in database",
    )
    CDC_3 = ErrorDef(
        SqlCdcException,
        "Table {schema}.{table} has no columns",
    )
    CDC_4 = ErrorDef(
        SqlCdcException,
        "CD table {schema}.{table} not found in ASN registration table "
        "{registry}.ibmsnap_register. Refer to the documentation for "
        "instructions on configuring Db2 table replication using ASN, or "
        "contact your system administrator to configure it.",
    )
    CDC_5 = ErrorDef(
        SqlCdcException,
        "Unknown CDC operation type {operation!r} from {source} in table {table}",
    )
    CDC_6 = ErrorDef(
        SqlCdcException,
        "Unsupported start_from value {value!r}",
    )
    CDC_7 = ErrorDef(
        SqlCdcException,
        "Column count mismatch for table {schema}.{table}: catalog returned "
        "{catalog_count} columns but inspector returned {inspector_count}",
    )
    CDC_8 = ErrorDef(
        SqlCdcException,
        "Column name mismatch for table {schema}.{table}: catalog column "
        "{catalog_column!r} does not match inspector column {inspector_column!r}",
    )
    CDC_9 = ErrorDef(
        SqlCdcException,
        "Staged file not found for {file!r} in table {table!r}",
    )
    CDC_10 = ErrorDef(
        SqlCdcException,
        "Buffer sentinel has no base offset; ingester must provide a "
        "base offset at initialization",
    )
    CDC_11 = ErrorDef(
        SqlCdcException,
        "Received change with no offset for table {table!r}",
    )
    CDC_12 = ErrorDef(
        SqlCdcException,
        "Expected offset type {expected!r} but received {actual!r}",
    )
    CDC_13 = ErrorDef(
        SqlCdcException,
        "Invalid CDC timestamp position specification",
    )
    CDC_14 = ErrorDef(
        SqlCdcException,
        "Invalid CDC format specification",
    )


def cdc_validate_call(
    error_code: SqlCdcErrorCode,
    f: Callable | None = None,
) -> Callable:
    """Decorator that validates a callable's arguments with pydantic and
    raises ``error_code``'s bound :class:`SqlCdcException` on failure.

    CDC-local replacement for the old
    ``tabsdata._utils.validation._td_validate_call``; the pydantic
    ``ValidationError`` is chained as the cause (the framework renders
    it onto the message)::

        @cdc_validate_call(SqlCdcErrorCode.CDC_14)
        def __init__(self, ...): ...
    """
    if f is None:
        return lambda fn: cdc_validate_call(error_code, fn)

    validated = _pydantic_validate_call(
        f, config=ConfigDict(arbitrary_types_allowed=True)
    )

    @wraps(f)
    def wrapper(*args: Any, **kwargs: Any) -> Any:
        try:
            return validated(*args, **kwargs)
        except ValidationError as pyd_err:
            raise error_code.exception(cause=pyd_err)

    return wrapper
