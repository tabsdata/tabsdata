#
# Copyright 2026 Tabsdata Inc.
#

"""
This module defines common type aliases used throughout the data quality
feature.

These types are used for type hinting and validation to ensure that data
quality operations are performed with the correct data structures and values.
Collection-valued fields are declared as plain ``list`` / ``frozenset`` types:
there is no single-value convenience coercion (consistent with the rest of the
``tabsdatak`` API), so a field that can hold several values always takes a
collection in the constructor.
"""

from datetime import date, datetime, time, timedelta
from decimal import Decimal
from typing import Annotated, Literal, TypeAlias, Union

from pydantic import Field, StrictFloat, StrictInt

# noinspection PyProtectedMember
from tabsdatak import _api

# --- Column Specifications ---

ColumnNameSpec: TypeAlias = str
"""A type alias for a single column name, represented as a string."""

ColumnNameWithAltSpec: TypeAlias = Union[
    ColumnNameSpec, tuple[ColumnNameSpec, Union[ColumnNameSpec, None]]
]
"""
A single column reference: either the source column name, or a
``(source, destination)`` tuple naming the column to store the result in
(``destination`` may be ``None`` to auto-generate it).
"""

ColumnNamesWithAltSpec: TypeAlias = list[ColumnNameWithAltSpec]
"""
A list of column references. Each element is a source column name or a
``(source, destination)`` tuple; the two forms may be mixed.
"""

# --- Tag Specifications ---

TagSpec: TypeAlias = str
"""A type alias for a single tag, represented as a string."""

TagsSpec: TypeAlias = list[TagSpec]
"""A list of tags."""

# --- Invalid Column Specifications ---

OnInvalidColumnSpec: TypeAlias = Literal["ignore", "fail"]
"""
A type alias that specifies what to do if a column is invalid for the
classifier.
"""

OnInvalidValueSpec: TypeAlias = Literal["ignore", "fail"]
"""
A type alias that specifies what to do if a classifier value is invalid for
column.
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

# --- Value Set Specifications ---

ValuesSpec: TypeAlias = Annotated[
    frozenset[Union[PythonDataTypeSpec, None]], _api.NON_EMPTY_LIST_FIELD
]
"""
A non-empty set of values to check membership against (``None`` is allowed, to
test for null membership). A list passed to the constructor is coerced to a
``frozenset`` (deduplicated) by pydantic.
"""

# --- Bin and Bucket Specifications ---

MAX_BINS = 100
"""The maximum number of bins allowed in a bucketing operation."""

BinIdxSpec: TypeAlias = Annotated[StrictInt, Field(ge=0, le=MAX_BINS)]
"""A type alias for a zero-based bin index, constrained by MAX_BINS."""

SpecialBinSpec: TypeAlias = Literal["none", "nan", "underflow", "overflow"]
"""A type alias for the bins for None, NaN, underflow and overflow values."""

BinSpec: TypeAlias = Union[BinIdxSpec, SpecialBinSpec]
"""
A type alias for the bin, either an index [0..100] or a special one
[none, nan, underflow, overflow]
"""

BinsCriteriaSpec: TypeAlias = Annotated[frozenset[BinSpec], _api.NON_EMPTY_LIST_FIELD]
"""A non-empty set of bins used in criteria."""

BinCountSpec: TypeAlias = Annotated[StrictInt, Field(ge=1, le=MAX_BINS)]
"""
A type alias for the number of bins a Scale will have, between 1 and MAX_BINS.
"""

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

ScaleIdentityLimitSpec: TypeAlias = StrictInt
"""A type alias for a limit in a scale identity, ranging from 0 to 100."""

ScaleLimitSpec: TypeAlias = Union[StrictInt, StrictFloat]
"""
A type alias for a generic limit in a scale, which can be an integer or a
float.
"""

ScalePowerSpec: TypeAlias = Annotated[StrictFloat, Field(gt=0)]
"""
A type alias for the power of a scale, which must be a float greater than 0.
"""

ScaleBaseSpec: TypeAlias = Annotated[StrictFloat, Field(gt=0)]
"""
A type alias for the base of a scale, which must be a float greater than 0.
"""

IncludeQualityColumnsSpec: TypeAlias = Literal["none", "criteria", "all"]
"""
A type alias for quality columns to included in the Select or Filter target
table.
"""
