#
# Copyright 2026 Tabsdata Inc.
#

from __future__ import annotations

from dataclasses import dataclass

import polars as pl

from tabsdatak.tableframe import TableFrame
from tabsdatak.tableframe._core.hooks import polars_of


@dataclass(frozen=True)
class DataSet:
    lazyframe: pl.LazyFrame
    eagerframe: pl.DataFrame
    tableframe: TableFrame


def tableframe_from_dictionary(data: dict) -> TableFrame:
    return TableFrame.from_dict(data)


def dataset_from_lazyframe(lazyframe: pl.LazyFrame) -> DataSet:
    return DataSet(
        lazyframe=lazyframe,
        eagerframe=lazyframe.collect(),
        tableframe=TableFrame.from_polars(lazyframe),
    )


def dataset_from_dictionary(data: dict) -> DataSet:
    return dataset_from_lazyframe(pl.LazyFrame(data))


def tableframe_to_lazy(tableframe: TableFrame) -> pl.LazyFrame:
    return polars_of(tableframe, pl.LazyFrame)


def tableframe_to_eager(tableframe: TableFrame) -> pl.DataFrame:
    return polars_of(tableframe, pl.LazyFrame).collect()
