#
# Copyright 2026 Tabsdata Inc.
#

from __future__ import annotations

from typing import FrozenSet, Type

from tabsdatak.tableframe.datatypes import (
    Boolean,
    Date,
    Datetime,
    Decimal,
    Duration,
    Float16,
    Float32,
    Float64,
    Int8,
    Int16,
    Int32,
    Int64,
    Int128,
    String,
    Time,
    UInt8,
    UInt16,
    UInt32,
    UInt64,
    Utf8,
)

_BOOLEAN_DTYPES: FrozenSet[Type] = frozenset({
    Boolean,
})

_TEMPORAL_DTYPES: FrozenSet[Type] = frozenset({
    Date,
    Datetime,
    Time,
    Duration,
})

_SIGNED_NUMERIC_DTYPES: FrozenSet[Type] = frozenset({
    Int8,
    Int16,
    Int32,
    Int64,
    Int128,
    Float16,
    Float32,
    Float64,
    Decimal,
})

_UNSIGNED_NUMERIC_DTYPES: FrozenSet[Type] = frozenset({
    UInt8,
    UInt16,
    UInt32,
    UInt64,
})

_NUMERIC_DTYPES: FrozenSet[Type] = _SIGNED_NUMERIC_DTYPES | _UNSIGNED_NUMERIC_DTYPES

_SIGNED_INT_DTYPES: FrozenSet[Type] = frozenset({
    Int8,
    Int16,
    Int32,
    Int64,
    Int128,
})

_UNSIGNED_INT_DTYPES: FrozenSet[Type] = frozenset({
    UInt8,
    UInt16,
    UInt32,
    UInt64,
})

_INT_DTYPES: FrozenSet[Type] = _SIGNED_INT_DTYPES | _UNSIGNED_INT_DTYPES

_FLOAT_DTYPES: FrozenSet[Type] = frozenset({
    Float16,
    Float32,
    Float64,
})

_STRING_DTYPES: FrozenSet[Type] = frozenset({
    String,
    Utf8,
})

_ALL_DTYPES: FrozenSet[Type] = (
    _BOOLEAN_DTYPES | _TEMPORAL_DTYPES | _NUMERIC_DTYPES | _STRING_DTYPES
)

_ALL_NON_BOOLEAN_DTYPES: FrozenSet[Type] = (
    _TEMPORAL_DTYPES | _NUMERIC_DTYPES | _STRING_DTYPES
)
