#
# Copyright 2025 Tabsdata Inc.
#

from __future__ import annotations

from typing import FrozenSet, Type, TypeAlias

import polars as pl
import polars.datatypes.classes as polars_datatypes_classes
import polars.datatypes.group as polars_datatypes_group

# noinspection DuplicatedCode
Boolean: TypeAlias = pl.Boolean
Date: TypeAlias = pl.Date
Datetime: TypeAlias = pl.Datetime
Time: TypeAlias = pl.Time
Duration: TypeAlias = pl.Duration
Int8: TypeAlias = pl.Int8
Int16: TypeAlias = pl.Int16
Int32: TypeAlias = pl.Int32
Int64: TypeAlias = pl.Int64
Int128: TypeAlias = pl.Int128
UInt8: TypeAlias = pl.UInt8
# noinspection DuplicatedCode
UInt16: TypeAlias = pl.UInt16
UInt32: TypeAlias = pl.UInt32
UInt64: TypeAlias = pl.UInt64
Float16: TypeAlias = pl.Float16
Float32: TypeAlias = pl.Float32
Float64: TypeAlias = pl.Float64
Decimal: TypeAlias = pl.Decimal
String: TypeAlias = pl.String
Utf8: TypeAlias = pl.Utf8
Null: TypeAlias = pl.Null

Categorical: TypeAlias = pl.Categorical
Enum: TypeAlias = pl.Enum

TEMPORAL_DTYPES = polars_datatypes_group.TEMPORAL_DTYPES
DURATION_DTYPES = polars_datatypes_group.DURATION_DTYPES
SIGNED_INTEGER_DTYPES = polars_datatypes_group.SIGNED_INTEGER_DTYPES
UNSIGNED_INTEGER_DTYPES = polars_datatypes_group.UNSIGNED_INTEGER_DTYPES
INTEGER_DTYPES = polars_datatypes_group.INTEGER_DTYPES
FLOAT_DTYPES = polars_datatypes_group.FLOAT_DTYPES
NUMERIC_DTYPES = polars_datatypes_group.NUMERIC_DTYPES

TemporalType: TypeAlias = polars_datatypes_classes.TemporalType
SignedIntegerType: TypeAlias = polars_datatypes_classes.SignedIntegerType
UnsignedIntegerType: TypeAlias = polars_datatypes_classes.UnsignedIntegerType
IntegerType: TypeAlias = polars_datatypes_classes.IntegerType
FloatType: TypeAlias = polars_datatypes_classes.FloatType
NumericType: TypeAlias = polars_datatypes_classes.NumericType

# Array
# Binary
# Field
# List
# NestedType
# Object
# ObjectType
# Struct
# Unknown

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
