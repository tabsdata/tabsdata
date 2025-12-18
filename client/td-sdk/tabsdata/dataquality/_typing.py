#
# Copyright 2025 Tabs Data Inc.
#

from typing import Annotated, Literal, TypeAlias, Union

from pydantic import Field, Strict, StrictFloat, StrictInt

from tabsdata.tableframe.typing import DataType

ColumnNameSpec: TypeAlias = str

ColumnNamesSpec: TypeAlias = Union[
    ColumnNameSpec, Annotated[list[ColumnNameSpec], Strict]
]

ColumnNameWithAltSpec: TypeAlias = tuple[ColumnNameSpec, Union[ColumnNameSpec, None]]

ColumNamesWithAltSpec: TypeAlias = Union[
    ColumnNameSpec,
    ColumnNameWithAltSpec,
    Annotated[list[ColumnNameSpec], Strict],
    Annotated[list[ColumnNameWithAltSpec], Strict],
]

TagSpec: TypeAlias = str

TagsSpec: TypeAlias = Union[
    TagSpec,
    list[TagSpec],
]

HasColumnsModeSpec: TypeAlias = Literal["at_least", "exact"]

DataTypeSpec: TypeAlias = DataType

ColumnTypeSpec: TypeAlias = tuple[ColumnNameSpec, DataTypeSpec]

ColumnTypesSpec: TypeAlias = Union[
    ColumnTypeSpec,
    Annotated[list[ColumnTypeSpec], Strict],
]

LenSpec: TypeAlias = Annotated[StrictInt, Field(ge=0)]

MinSpec: TypeAlias = Union[StrictInt, StrictFloat]
MaxSpec: TypeAlias = Union[StrictInt, StrictFloat]

RangeClosedOnSpec: TypeAlias = Literal["none", "lower", "upper", "both"]

CriteriaTypeSpec = Literal["all_ok", "any_failed", "in_buckets", "not_in_buckets"]

MAX_BINS = 100
BinIdxSpec: TypeAlias = Annotated[StrictInt, Field(ge=0, le=MAX_BINS)]
BinsSpec: TypeAlias = Annotated[StrictInt, Field(ge=1, le=MAX_BINS)]

BinsCriteriaSpec: TypeAlias = Annotated[list[BinIdxSpec], Strict]

RowCountSpec: TypeAlias = Annotated[StrictInt, Field(ge=0)]
PercentSpec: TypeAlias = Union[
    Annotated[StrictInt, Field(ge=0, le=100)],
    Annotated[StrictFloat, Field(ge=0, le=100)],
]

ScaleIdentityLimitSpec = Annotated[StrictInt, Field(ge=0, le=100)]
ScaleLimitSpec = Union[StrictInt, StrictFloat]
ScalePowerSpec = Annotated[StrictFloat, Field(gt=0)]
ScaleBaseSpec = Annotated[StrictFloat, Field(gt=0)]
