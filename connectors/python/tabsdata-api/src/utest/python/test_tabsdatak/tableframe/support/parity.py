#
# Copyright 2026 Tabsdata Inc.
#

from __future__ import annotations

from typing import Callable

import polars as pl

from tabsdatak.tableframe import TableFrame


def normalize_lazyframe_column(
    lazyframe: pl.LazyFrame,
    sort: bool,
    rounded: bool,
    column: str | None,
) -> pl.LazyFrame:
    if rounded:
        lazyframe = lazyframe.with_columns(
            pl.col(column).cast(pl.Float64).fill_nan(0).fill_null(0).abs().round(3)
        )
    if sort:
        lazyframe = lazyframe.sort(pl.first())
    return lazyframe


def equals_lazyframe_tableframe(
    polars_result: pl.LazyFrame,
    tableframe_result: TableFrame,
    sort: bool = True,
    rounded: bool = False,
    column: str | None = None,
) -> bool:
    if not isinstance(polars_result, pl.LazyFrame):
        raise ValueError("Expected a polars LazyFrame as the first argument.")
    if not isinstance(tableframe_result, TableFrame):
        raise ValueError("Expected a tabsdata TableFrame as the second argument.")
    polars_result = normalize_lazyframe_column(polars_result, sort, rounded, column)
    tableframe_result = normalize_lazyframe_column(
        tableframe_result.to_polars_lf(), sort, rounded, column
    )
    return polars_result.collect().equals(tableframe_result.collect())


def assert_lazyframe_tableframe_parity(
    operation: Callable,
    lazyframe: pl.LazyFrame,
    tableframe: TableFrame,
    sort: bool = True,
    rounded: bool = False,
    column: str | None = None,
) -> None:
    import tabsdatak.tableframe.functions as tableframe_functions

    polars_result = operation(pl, lazyframe)
    tableframe_result = operation(tableframe_functions, tableframe)
    assert equals_lazyframe_tableframe(
        polars_result, tableframe_result, sort, rounded, column
    ), "TableFrame result differs from the polars result."
