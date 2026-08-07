#
# Copyright 2026 Tabsdata Inc.
#

from __future__ import annotations

from tabsdatak.error import (
    ErrorCode,
    ErrorDef,
    TabsdataException,
)


class TableFrameException(TabsdataException): ...


class TableFrameInitException(TableFrameException): ...


class TableFrameRuntimeException(TableFrameException): ...


class TableFrameInternalException(TableFrameException): ...


class DirectConstructionError(TableFrameInitException): ...


class IncompleteVersionError(TableFrameInternalException): ...


class UnsupportedInputError(TableFrameInitException): ...


class MissingRequiredColumnsError(TableFrameInternalException): ...


class NotSupportedError(TableFrameRuntimeException): ...


class SystemColumnWriteError(TableFrameRuntimeException): ...


class MultipleRowsError(TableFrameRuntimeException): ...


class EmptyFrameError(TableFrameRuntimeException): ...


class UnsupportedMetadataError(TableFrameInitException): ...


class TableFrameErrorCode(ErrorCode):
    TF1 = ErrorDef(
        DirectConstructionError,
        "TableFrame cannot be constructed directly; "
        "use from_polars / from_pandas / from_dict / scan.",
    )
    TF2 = ErrorDef(
        IncompleteVersionError,
        "DataVersion requires execution, transaction, version, "
        "and timestamp; got "
        "execution={execution!r} - "
        "transaction={transaction!r} - "
        "version={version!r} - "
        "timestamp={timestamp!r}.",
    )
    TF3 = ErrorDef(
        UnsupportedInputError,
        "{factory} does not support an input of type {type}.",
    )
    TF4 = ErrorDef(
        MissingRequiredColumnsError,
        "TableFrame is missing required system columns: {missing}.",
    )
    TF5 = ErrorDef(
        NotSupportedError,
        "TableFrame does not support the {op} operation.",
    )
    TF6 = ErrorDef(
        SystemColumnWriteError,
        "The {op} operation cannot create, overwrite, or drop the system columns "
        "{columns}; they are managed by TableFrame.",
    )
    TF7 = ErrorDef(
        MultipleRowsError,
        "The {op} operation requires at most one row; the TableFrame has more.",
    )
    TF8 = ErrorDef(
        EmptyFrameError,
        "The {op} operation requires at least one row; the TableFrame is empty.",
    )
    TF9 = ErrorDef(
        UnsupportedMetadataError,
        "The {column} metadata entry has type {type}; connector columns support "
        "only str, int, float, and bool.",
    )
