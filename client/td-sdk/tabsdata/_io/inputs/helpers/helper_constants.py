#
# Copyright 2025 Tabs Data Inc.
#

from enum import StrEnum

import numpy as np

import tabsdata.tableframe.dtypes as td_dtypes


class _LoaderSpec(StrEnum):
    PANDAS = "pandas"
    POLARS_ADBC = "polars_adbc"
    POLARS_CONNECTORX = "polars_connectorx"
    POLARS_SQLALCHEMY = "polars_sqlalchemy"

    @classmethod
    def values(cls) -> tuple[str, ...]:
        return tuple(item.value for item in cls)


# Mapping from Polars dtypes to NumPy dtypes
POLARS_TO_NUMPY_DTYPE_MAP = {
    # Integers
    td_dtypes.Int8: np.int8,
    td_dtypes.Int16: np.int16,
    td_dtypes.Int32: np.int32,
    td_dtypes.Int64: np.int64,
    td_dtypes.UInt8: np.uint8,
    td_dtypes.UInt16: np.uint16,
    td_dtypes.UInt32: np.uint32,
    td_dtypes.UInt64: np.uint64,
    # Floats
    td_dtypes.Float32: np.float32,
    td_dtypes.Float64: np.float64,
    td_dtypes.Decimal: np.longdouble,
    # Boolean
    td_dtypes.Boolean: np.bool_,
    # Temporal
    td_dtypes.Date: np.dtype("datetime64[us]"),
    td_dtypes.Datetime: np.dtype("datetime64[us]"),
    td_dtypes.Time: np.dtype("O"),
    td_dtypes.Duration: np.dtype("timedelta64[us]"),
    td_dtypes.String: np.dtype("U"),  # Unicode string
}
