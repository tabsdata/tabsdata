#
# Copyright 2026 Tabsdata Inc.
#

from __future__ import annotations

import datetime
import decimal
from dataclasses import dataclass

import polars as pl

from test_tabsdatak.tableframe.support.frames import DataSet, dataset_from_lazyframe


@dataclass(frozen=True)
class DataTypeCase:
    name: str
    datatype: pl.DataType
    values: list

    @property
    def column(self) -> str:
        return f"{self.name}_column"

    @property
    def dtype(self) -> pl.DataType:
        return self.series().dtype

    def series(self) -> pl.Series:
        return pl.Series(self.column, self.values, dtype=self.datatype)

    def lazyframe(self) -> pl.LazyFrame:
        return pl.LazyFrame(self.series())

    def dataset(self) -> DataSet:
        return dataset_from_lazyframe(self.lazyframe())


DATA_TYPE_CASES = [
    DataTypeCase("int8", pl.Int8, [1, 2, 3]),
    DataTypeCase("int16", pl.Int16, [1, 2, 3]),
    DataTypeCase("int32", pl.Int32, [1, 2, 3]),
    DataTypeCase("int64", pl.Int64, [1, 2, 3]),
    DataTypeCase("int128", pl.Int128, [1, 2, 3]),
    DataTypeCase("uint8", pl.UInt8, [1, 2, 3]),
    DataTypeCase("uint16", pl.UInt16, [1, 2, 3]),
    DataTypeCase("uint32", pl.UInt32, [1, 2, 3]),
    DataTypeCase("uint64", pl.UInt64, [1, 2, 3]),
    DataTypeCase("uint128", pl.UInt128, [1, 2, 3]),
    DataTypeCase("float16", pl.Float16, [1.0, 2.0, 3.0]),
    DataTypeCase("float32", pl.Float32, [1.0, 2.0, 3.0]),
    DataTypeCase("float64", pl.Float64, [1.0, 2.0, 3.0]),
    DataTypeCase("boolean", pl.Boolean, [True, False, True]),
    DataTypeCase("string", pl.String, ["a", "b", "c"]),
    DataTypeCase("binary", pl.Binary, [b"a", b"b", b"c"]),
    DataTypeCase(
        "date",
        pl.Date,
        [
            datetime.date(2020, 1, 1),
            datetime.date(2021, 6, 15),
            datetime.date(2022, 12, 31),
        ],
    ),
    DataTypeCase(
        "datetime",
        pl.Datetime,
        [
            datetime.datetime(2020, 1, 1, 10, 0, 0),
            datetime.datetime(2021, 6, 15, 12, 30, 0),
            datetime.datetime(2022, 12, 31, 23, 59, 59),
        ],
    ),
    DataTypeCase(
        "time",
        pl.Time,
        [datetime.time(1, 2, 3), datetime.time(10, 20, 30), datetime.time(23, 59, 59)],
    ),
    DataTypeCase(
        "duration",
        pl.Duration,
        [
            datetime.timedelta(days=1),
            datetime.timedelta(hours=2),
            datetime.timedelta(minutes=3),
        ],
    ),
    DataTypeCase(
        "decimal",
        pl.Decimal,
        [decimal.Decimal("1.5"), decimal.Decimal("2.25"), decimal.Decimal("3.125")],
    ),
    DataTypeCase("categorical", pl.Categorical, ["a", "b", "a"]),
    DataTypeCase("enum", pl.Enum(["a", "b", "c"]), ["a", "b", "c"]),
    DataTypeCase("list", pl.List(pl.Int64), [[1, 2], [3], [4, 5]]),
    DataTypeCase("struct", pl.Struct({"x": pl.Int64}), [{"x": 1}, {"x": 2}, {"x": 3}]),
    DataTypeCase("array", pl.Array(pl.Int64, 2), [[1, 2], [3, 4], [5, 6]]),
]
