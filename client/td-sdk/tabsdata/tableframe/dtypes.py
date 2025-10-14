#
# Copyright 2025 Tabs Data Inc.
#

from __future__ import annotations

from typing import TypeAlias

import polars as pl
import polars.datatypes.classes as polars_datatypes_classes
import polars.datatypes.group as polars_datatypes_group

# noinspection DuplicatedCode
Boolean: TypeAlias = pl.Boolean
Date: TypeAlias = pl.Date
Datetime: TypeAlias = pl.Datetime
Decimal: TypeAlias = pl.Decimal
Duration: TypeAlias = pl.Duration
Float32: TypeAlias = pl.Float32
Float64: TypeAlias = pl.Float64
Int8: TypeAlias = pl.Int8
Int16: TypeAlias = pl.Int16
Int32: TypeAlias = pl.Int32
# noinspection DuplicatedCode
Int64: TypeAlias = pl.Int64
Int128: TypeAlias = pl.Int128
Null: TypeAlias = pl.Null
String: TypeAlias = pl.String
Time: TypeAlias = pl.Time
UInt8: TypeAlias = pl.UInt8
UInt16: TypeAlias = pl.UInt16
UInt32: TypeAlias = pl.UInt32
UInt64: TypeAlias = pl.UInt64
Utf8: TypeAlias = pl.Utf8

Categorical: TypeAlias = pl.Categorical
Enum: TypeAlias = pl.Enum

FLOAT_DTYPES = polars_datatypes_group.FLOAT_DTYPES
INTEGER_DTYPES = polars_datatypes_group.INTEGER_DTYPES
NUMERIC_DTYPES = polars_datatypes_group.NUMERIC_DTYPES
SIGNED_INTEGER_DTYPES = polars_datatypes_group.SIGNED_INTEGER_DTYPES
TEMPORAL_DTYPES = polars_datatypes_group.TEMPORAL_DTYPES
UNSIGNED_INTEGER_DTYPES = polars_datatypes_group.UNSIGNED_INTEGER_DTYPES
NumericType: TypeAlias = polars_datatypes_classes.NumericType
IntegerType: TypeAlias = polars_datatypes_classes.IntegerType
SignedIntegerType: TypeAlias = polars_datatypes_classes.SignedIntegerType
UnsignedIntegerType: TypeAlias = polars_datatypes_classes.UnsignedIntegerType
FloatType: TypeAlias = polars_datatypes_classes.FloatType
TemporalType: TypeAlias = polars_datatypes_classes.TemporalType

# Array
# Binary
# Field
# List
# NestedType
# Object
# ObjectType
# Struct
# Unknown
