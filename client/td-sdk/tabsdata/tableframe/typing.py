#
# Copyright 2025 Tabs Data Inc.
#

from __future__ import annotations

from datetime import date, datetime, time, timedelta
from decimal import Decimal
from typing import TYPE_CHECKING, Literal, Mapping, Sequence, TypeAlias, Union

import polars as pl

# noinspection PyProtectedMember
import polars._typing as pl_typing
import polars.datatypes as pl_data_types

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

DataType: TypeAlias = Union[
    pl_data_types.DataTypeClass,
    pl.DataType,
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

DataTypeClass: TypeAlias = pl_data_types.DataTypeClass

SchemaInitDataType: TypeAlias = Union[DataType, DataTypeClass, PythonDataType]

IntoExprColumn: TypeAlias = Union["Expr", Series, str]

IntoExpr: TypeAlias = Union[pl_typing.PythonLiteral, IntoExprColumn, None]
