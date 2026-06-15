#
# Copyright 2026 Tabs Data Inc.
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


class IncompleteVersionPropertiesError(TableFrameInternalException): ...


class UnsupportedInputError(TableFrameInitException): ...


class TableFrameErrorCode(ErrorCode):
    TF1 = ErrorDef(
        DirectConstructionError,
        "TableFrame cannot be constructed directly; "
        "use from_polars / from_pandas / from_dict / scan.",
    )
    TF2 = ErrorDef(
        IncompleteVersionPropertiesError,
        "DataVersionProperties requires execution, transaction, version, "
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
