#
# Copyright 2025 Tabs Data Inc.
#

from __future__ import annotations

from polars import (
    Boolean,
    Categorical,
    Date,
    Datetime,
    Decimal,
    Duration,
    Enum,
    Float32,
    Float64,
    Int8,
    Int16,
    Int32,
    Int64,
    Int128,
    Null,
    String,
    Time,
    UInt8,
    UInt16,
    UInt32,
    UInt64,
)
from polars.datatypes.classes import (
    FloatType,
    IntegerType,
    NumericType,
    SignedIntegerType,
    TemporalType,
    UnsignedIntegerType,
)
from polars.datatypes.group import (
    FLOAT_DTYPES,
    INTEGER_DTYPES,
    NUMERIC_DTYPES,
    SIGNED_INTEGER_DTYPES,
    TEMPORAL_DTYPES,
    UNSIGNED_INTEGER_DTYPES,
)

from tabsdata._utils.tableframe._constants import SysCol
from tabsdata._utils.tableframe.builders import (
    empty,
    from_dict,
    from_pandas,
    from_polars,
    to_dict,
    to_pandas,
    to_polars_df,
    to_polars_lf,
)
from tabsdata.tableframe import selectors
from tabsdata.tableframe.functions.col import Column, col
from tabsdata.tableframe.functions.eager import concat
from tabsdata.tableframe.functions.lit import lit
from tabsdata.tableframe.lazyframe.frame import TableFrame
from tabsdata.tableframe.schema import Schema
from tabsdata.tableframe.udf.function import UDF

__all__ = [
    # from tabsdata.tableframe...
    "Column",
    "Schema",
    "SysCol",
    "col",
    "concat",
    "lit",
    "TableFrame",
    "UDF",
    # from tabsdata._utils.tableframe
    "empty",
    "from_dict",
    "from_pandas",
    "from_polars",
    "selectors",
    "to_dict",
    "to_pandas",
    "to_polars_df",
    "to_polars_lf",
    # from polars (basic)...
    Boolean,
    Date,
    Datetime,
    Duration,
    Float32,
    Float64,
    Int8,
    Int16,
    Int32,
    Int64,
    Int128,
    Null,
    String,
    Time,
    UInt8,
    UInt16,
    UInt32,
    UInt64,
    # from polars (advanced)...
    FLOAT_DTYPES,
    INTEGER_DTYPES,
    NUMERIC_DTYPES,
    SIGNED_INTEGER_DTYPES,
    TEMPORAL_DTYPES,
    UNSIGNED_INTEGER_DTYPES,
    NumericType,
    IntegerType,
    SignedIntegerType,
    UnsignedIntegerType,
    FloatType,
    TemporalType,
    # NestedType,
    # ObjectType,
    Decimal,
    # Binary,
    Categorical,
    Enum,
    # Object,
    # Unknown,
    # List,
    # Array,
    # Field,
    # Struct,
]
