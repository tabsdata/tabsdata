#
# Copyright 2025 Tabs Data Inc.
#

from __future__ import annotations

import collections.abc as collections
from datetime import date, datetime, time, timedelta
from decimal import Decimal
from typing import (
    TYPE_CHECKING,
    Iterable,
    Literal,
    Mapping,
    Sequence,
    TypeAlias,
    Union,
    get_args,
)

import polars as pl

# noinspection PyProtectedMember
import polars._typing as pl_typing

import tabsdata.tableframe.dtypes as td_dtypes
import tabsdata.tableframe.schema as td_schema

if TYPE_CHECKING:
    import tabsdata.tableframe.functions.col as td_col
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

RankMethod: TypeAlias = Literal[
    "average",
    "min",
    "max",
    "dense",
    "ordinal",
    "random",
]

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

SchemaItem: TypeAlias = Union[
    tuple[str, TypeOfDataType],
    list,  # Two-element list: [name, dtype]
    "td_col.Column",
]


class SimpleSchema:
    """
    A flexible simple schema class that normalizes various input formats.

    Accepts column name + dtype pairs in multiple formats:
    - Mapping: {"col1": td.Int64, "col2": td.String}
    - List of tuples: [("col1", td.Int64), ("col2", td.String)]
    - List of lists: [["col1", td.Int64], ["col2", td.String]]
    - List of Columns: [td.col("col1", td.Int64), td.col("col2", td.String)]
    - Any other iterable containing the above formats
    """

    def __init__(
        self,
        schema: Union[
            Mapping[str, TypeOfDataType],
            Iterable[SchemaItem],
            None,
        ] = None,
    ) -> None:
        self._schema = schema

    @property
    def schema(self) -> tuple[tuple[str, TypeOfDataType], ...]:
        """
        Normalize the input schema to a standard sequence of (name, dtype) tuples.

        Returns:
            Tuple of (column_name, column_dtype) pairs
        """
        if self._schema is None:
            return ()

        # Handle Mapping (dict-like)
        if isinstance(self._schema, collections.Mapping):
            return tuple((name, dtype) for name, dtype in self._schema.items())

        # Handle Iterable (list, tuple, etc.) but not strings or bytes
        if isinstance(self._schema, collections.Iterable) and not isinstance(
            self._schema, (str, bytes)
        ):
            # Lazy import to avoid circular dependency
            import tabsdata.tableframe.functions.col as td_col

            result = []
            for item in self._schema:
                # Column object
                if isinstance(item, td_col.Column):
                    result.append((item.name, item.dtype))
                # Tuple or list with (name, dtype)
                elif isinstance(item, (tuple, list)):
                    if len(item) == 2:
                        name, dtype = item
                        if isinstance(name, str):
                            result.append((name, dtype))
                        else:
                            raise ValueError(
                                "Invalid schema item: first element must be a string"
                                f" (column name), got {type(name).__name__}"
                            )
                    else:
                        raise ValueError(
                            "Invalid schema item: expected 2 elements (name, dtype),"
                            f" got {len(item)}"
                        )
                else:
                    raise ValueError(
                        "Invalid schema item: expected Column, tuple, or list, got"
                        f" {type(item).__name__}"
                    )
            return tuple(result)

        raise ValueError(
            "Invalid schema type: expected Mapping, Iterable, or None, got"
            f" {type(self._schema).__name__}"
        )
