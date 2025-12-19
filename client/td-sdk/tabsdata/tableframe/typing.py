#
# Copyright 2025 Tabs Data Inc.
#

from __future__ import annotations

from datetime import date, datetime, time, timedelta
from decimal import Decimal
from typing import TYPE_CHECKING, Literal, Mapping, Sequence, TypeAlias, Union, get_args

import polars as pl

# noinspection PyProtectedMember
import polars._typing as pl_typing

import tabsdata.tableframe.dtypes as td_dtypes
import tabsdata.tableframe.schema as td_schema

if TYPE_CHECKING:
    from tabsdata.tableframe.expr.expr import Expr
    from tabsdata.tableframe.selectors import SelectorProxy

ConcatMethod = Literal[
    "vertical",
    "vertical_relaxed",
    "diagonal",
    "diagonal_relaxed",
]

TableDictionary = Mapping[
    str,
    Union[
        Sequence[object],
        Mapping[
            str,
            Sequence[object],
        ],
    ],
]

SelectorType: TypeAlias = "SelectorProxy"

ColumnNameOrSelector: TypeAlias = Union[
    str,
    SelectorType,
]

UniqueKeepStrategy: TypeAlias = Literal[
    "first",
    "last",
    "any",
    "none",
]

JoinStrategy: TypeAlias = Literal[
    "inner",
    "left",
    "right",
    "full",
    "semi",
    "anti",
    "cross",
    "outer",
]

# noinspection DuplicatedCode
ClosedInterval: TypeAlias = Literal[
    "left",
    "right",
    "both",
    "none",
]
FillNullStrategy: TypeAlias = Literal[
    "forward",
    "backward",
    "min",
    "max",
    "mean",
    "zero",
    "one",
]
NumericLiteral: TypeAlias = Union[int, float, Decimal]
RankMethod: TypeAlias = Literal["average", "min", "max", "dense", "ordinal", "random"]
TemporalLiteral: TypeAlias = Union[date, time, datetime, timedelta]

# noinspection DuplicatedCode
Schema: TypeAlias = td_schema.Schema

Series: TypeAlias = pl.Series

# noinspection PyProtectedMember
Ambiguous: TypeAlias = pl_typing.Ambiguous

# noinspection PyProtectedMember
ArrowSchemaExportable: TypeAlias = pl_typing.ArrowSchemaExportable

# noinspection PyProtectedMember
EpochTimeUnit: TypeAlias = pl_typing.EpochTimeUnit

# noinspection PyProtectedMember
NonExistent: TypeAlias = pl_typing.NonExistent

# noinspection PyProtectedMember
PythonDataType: TypeAlias = pl_typing.PythonDataType

# noinspection PyProtectedMember
Roll: TypeAlias = pl_typing.Roll

# noinspection PyProtectedMember
TimeUnit: TypeAlias = pl_typing.TimeUnit

# Caution: Update type TypeOfDataType below everytime you modify this type.
DataType: TypeAlias = Union[
    td_dtypes.Boolean,
    td_dtypes.Categorical,
    td_dtypes.Date,
    td_dtypes.Datetime,
    td_dtypes.Decimal,
    td_dtypes.Duration,
    td_dtypes.Enum,
    td_dtypes.Float32,
    td_dtypes.Float64,
    td_dtypes.Int8,
    td_dtypes.Int16,
    td_dtypes.Int64,
    td_dtypes.Int32,
    td_dtypes.Int128,
    td_dtypes.Null,
    td_dtypes.String,
    td_dtypes.Time,
    td_dtypes.UInt8,
    td_dtypes.UInt16,
    td_dtypes.UInt32,
    td_dtypes.UInt64,
    td_dtypes.Utf8,
]

# Caution: Update type DataType above everytime you modify this type.
TypeOfDataType: TypeAlias = Union[
    type[td_dtypes.Boolean],
    type[td_dtypes.Categorical],
    type[td_dtypes.Date],
    type[td_dtypes.Datetime],
    type[td_dtypes.Decimal],
    type[td_dtypes.Duration],
    type[td_dtypes.Enum],
    type[td_dtypes.Float32],
    type[td_dtypes.Float64],
    type[td_dtypes.Int8],
    type[td_dtypes.Int16],
    type[td_dtypes.Int64],
    type[td_dtypes.Int32],
    type[td_dtypes.Int128],
    type[td_dtypes.Null],
    type[td_dtypes.String],
    type[td_dtypes.Time],
    type[td_dtypes.UInt8],
    type[td_dtypes.UInt16],
    type[td_dtypes.UInt32],
    type[td_dtypes.UInt64],
]

TF_DATA_TYPES: frozenset[type] = frozenset(get_args(DataType))

SchemaInitDataType: TypeAlias = Union[DataType, PythonDataType]

IntoExprColumn: TypeAlias = Union["Expr", Series, str]

IntoExpr: TypeAlias = Union[pl_typing.PythonLiteral, IntoExprColumn, None]
