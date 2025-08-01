#
# Copyright 2025 Tabs Data Inc.
#

from __future__ import annotations

from typing import TYPE_CHECKING, Literal, Mapping, Sequence, TypeAlias, Union

import polars as pl

# noinspection PyProtectedMember
import polars._typing as pl_typing
import polars.datatypes as pl_data_types

if TYPE_CHECKING:
    from tabsdata.tableframe.selectors import SelectorProxy

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

Schema: TypeAlias = pl.Schema

Series: TypeAlias = pl.Series

# noinspection PyProtectedMember
Ambiguous: TypeAlias = pl_typing.Ambiguous

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
