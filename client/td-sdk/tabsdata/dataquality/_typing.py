#
# Copyright 2025 Tabs Data Inc.
#
"""
This module defines common type aliases used throughout the data quality
feature.

These types are used for type hinting and validation to ensure that data
quality operations are performed with the correct data structures and values.
"""

from datetime import date, datetime, time, timedelta
from decimal import Decimal
from typing import Annotated, Literal, TypeAlias, Union

from pydantic import Field, Strict, StrictFloat, StrictInt

# --- Column Specifications ---

ColumnNameSpec: TypeAlias = str
"""A type alias for a single column name, represented as a string."""

ColumnNamesSpec: TypeAlias = Union[
    ColumnNameSpec, Annotated[list[ColumnNameSpec], Strict]
]
"""
A type alias for one or more column names, which can be a single string or a
list.
"""

ColumnNameWithAltSpec: TypeAlias = tuple[ColumnNameSpec, Union[ColumnNameSpec, None]]
"""
A type alias for a tuple representing a column name and an optional alternative
name.

This is useful for operations that involve renaming or aliasing columns.
"""

ColumNamesWithAltSpec: TypeAlias = Union[
    ColumnNameSpec,
    ColumnNameWithAltSpec,
    Annotated[list[ColumnNameSpec], Strict],
    Annotated[list[ColumnNameWithAltSpec], Strict],
]
"""
A flexible type alias for specifying column names, which can be a single column
name, a tuple with an alternative, or a list of either.
"""

# --- Tag Specifications ---

TagSpec: TypeAlias = str
"""A type alias for a single tag, represented as a string."""

TagsSpec: TypeAlias = Union[TagSpec, list[TagSpec]]
"""
A type alias for one or more tags, which can be a single string or a list of
strings.
"""

# --- Invalid Column Specifications ---

OnInvalidColumnSpec: TypeAlias = Literal["ignore", "fail"]
"""
A type alias that specificies what to if a column is invalid for the 
classifier.
"""

# --- General Purpose Types ---

LenSpec: TypeAlias = Annotated[StrictInt, Field(ge=0)]
"""
A type alias for a non-negative integer, typically used for specifying length.
"""

PythonDataTypeSpec: TypeAlias = Union[
    int, float, bool, str, date, time, datetime, timedelta, bytes, Decimal
]
"""
A type alias for a union of common Python data types used in data quality
checks.
"""

# --- Range and Criteria Specifications ---

RangeValueSpec: TypeAlias = PythonDataTypeSpec
"""
A type alias for a value used in defining a range, based on Python data types.
"""

RangeClosedOnSpec: TypeAlias = Literal["none", "lower", "upper", "both"]
"""
A type alias that specifies which side of a range is closed (inclusive).
- 'none': The range is open on both sides.
- 'lower': The range is closed on the lower bound.
- 'upper': The range is closed on the upper bound.
- 'both': The range is closed on both sides.
"""

CriteriaTypeSpec = Literal["all_ok", "any_failed", "in_bins", "not_in_bins"]
"""
A type alias for the type of criteria used in a data quality check.
- 'all_ok': All conditions must be met.
- 'any_failed': At least one condition must fail.
- 'in_bins': The value must fall into specified bins.
- 'not_in_bins': The value must not fall into specified bins.
"""

# --- Bin and Bucket Specifications ---

MAX_BINS = 100
"""The maximum number of bins allowed in a bucketing operation."""

BinIdxSpec: TypeAlias = Annotated[StrictInt, Field(ge=0, le=MAX_BINS)]
"""A type alias for a zero-based bin index, constrained by MAX_BINS."""

BinsSpec: TypeAlias = Annotated[StrictInt, Field(ge=1, le=MAX_BINS)]
"""A type alias for the number of bins, ranging from 1 to MAX_BINS."""

BinsCriteriaSpec: TypeAlias = Annotated[list[BinIdxSpec], Strict]
"""A type alias for a list of bin indices used in criteria."""

# --- Measurement Specifications ---

RowCountSpec: TypeAlias = Annotated[StrictInt, Field(ge=0)]
"""A type alias for a non-negative row count."""

PercentSpec: TypeAlias = Union[
    Annotated[StrictInt, Field(ge=0, le=100)],
    Annotated[StrictFloat, Field(ge=0, le=100)],
]
"""
A type alias for a percentage value, which can be an integer or a float between
0 and 100.
"""

# --- Scale Specifications ---

ScaleIdentityLimitSpec = Annotated[StrictInt, Field(ge=0, le=100)]
"""A type alias for a limit in a scale identity, ranging from 0 to 100."""

ScaleLimitSpec = Union[StrictInt, StrictFloat]
"""
A type alias for a generic limit in a scale, which can be an integer or a
float.
"""

ScalePowerSpec = Annotated[StrictFloat, Field(gt=0)]
"""
A type alias for the power of a scale, which must be a float greater than 0.
"""

ScaleBaseSpec = Annotated[StrictFloat, Field(gt=0)]
"""
A type alias for the base of a scale, which must be a float greater than 0.
"""
