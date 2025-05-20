#
# Copyright 2025 Tabs Data Inc.
#

from __future__ import annotations

from typing import Any, Iterable, Literal, TypeAlias

import polars as pl

# noinspection PyProtectedMember
import tabsdata.utils.tableframe._constants as td_constants
import tabsdata.utils.tableframe._helpers as td_helpers
from tabsdata.exceptions import ErrorCode, TableFrameError

DropColumnsStrategy: TypeAlias = Literal[
    "drop",
    "batch",
    "select",
]

AddSystemColumnsStrategy: TypeAlias = Literal[
    "map_element",
    "map_batches",
]


def check_column_name(name: str):
    if name.startswith(td_constants.TD_COLUMN_PREFIX):
        raise TableFrameError(ErrorCode.TF10, name)


def check_column(name: str):
    if name.startswith(td_constants.REGEXP_ANCHOR_START) and name.endswith(
        td_constants.REGEXP_ANCHOR_END
    ):
        raise TableFrameError(ErrorCode.TF3, name)
    if name in td_helpers.SYSTEM_COLUMNS:
        raise TableFrameError(ErrorCode.TF4, name)


def check_columns(columns: Any, *more_columns: Any):
    if more_columns:
        if isinstance(columns, str):
            names = [columns]
            names.extend(more_columns)
            for name in names:
                check_column(name)
    elif isinstance(columns, str):
        check_column(columns)
    elif isinstance(columns, Iterable):
        names = list(columns)
        if names:
            item = names[0]
            if isinstance(item, str):
                for name in names:
                    check_column(name)
    return


def add_system_columns(
    lf: pl.LazyFrame,
    idx: int | None = None,
    strategy: AddSystemColumnsStrategy = "map_element",
) -> pl.LazyFrame:
    match strategy:
        case "map_element":
            return add_system_columns_map_element(lf, idx)
        case "map_batches":
            return add_system_columns_map_batches(lf, idx)
        case _:
            raise ValueError(f"Unknown add system column strategy: {strategy}")


def add_system_columns_map_element(
    lf: pl.LazyFrame,
    idx: int | None = None,
) -> pl.LazyFrame:
    current_columns = set(lf.collect_schema().names())
    for column, metadata in td_helpers.SYSTEM_COLUMNS_METADATA.items():
        if column in current_columns:
            continue
        dtype, default, generator = (
            metadata[td_constants.TD_COL_DTYPE],
            metadata[td_constants.TD_COL_DEFAULT],
            metadata[td_constants.TD_COL_GENERATOR],
        )
        generator_instance = generator(idx)

        lf = lf.with_columns(default().alias(column))
        lf = lf.with_columns_seq(
            pl.col(column)
            .map_elements(generator_instance, return_dtype=dtype)
            .alias(column)
        )
    return lf


def add_system_columns_map_batches(
    lf: pl.LazyFrame,
    idx: int | None = None,
) -> pl.LazyFrame:
    def generate_system_column(
        series: pl.Series, series_generator, series_dtype
    ) -> pl.Series:
        series = series.map_elements(series_generator, return_dtype=series_dtype)
        return series.cast(series_dtype)

    dtypes = lf.collect_schema().dtypes()

    current_columns = set(lf.collect_schema().names())
    missing_columns = []

    generators = {}
    dtypes = {}

    for column, metadata in td_helpers.SYSTEM_COLUMNS_METADATA.items():
        if column in current_columns:
            continue
        dtype, default, generator = (
            metadata[td_constants.TD_COL_DTYPE],
            metadata[td_constants.TD_COL_DEFAULT],
            metadata[td_constants.TD_COL_GENERATOR],
        )
        generator_instance = generator(idx)

        missing_columns.append(default().cast(dtype).alias(column))
        generators[column] = generator_instance
        dtypes[column] = dtype

    if missing_columns:
        lf = lf.with_columns(missing_columns)

    if missing_columns:

        def generate_system_columns(batch: pl.DataFrame) -> pl.DataFrame:
            return batch.with_columns(
                [
                    generate_system_column(
                        batch[missing_column],
                        generators[missing_column],
                        dtypes[missing_column],
                    ).alias(missing_column)
                    for missing_column in generators
                ]
            )

        lf = lf.map_batches(
            generate_system_columns,
            streamable=True,
            validate_output_schema=False,
        )
    return lf


def drop_system_columns(
    lf: pl.LazyFrame,
    strategy: DropColumnsStrategy | None = "drop",
    ignore_missing: bool = True,
) -> pl.LazyFrame:
    match strategy:
        case None:
            return lf
        case "drop":
            return drop_system_columns_drop(lf, ignore_missing)
        case "batch":
            return drop_system_columns_drop_in_batch(lf, ignore_missing)
        case "select":
            return drop_system_columns_select_in_batch(lf, ignore_missing)
        case _:
            raise ValueError(f"Unknown drop column strategy: {strategy}")


def drop_system_columns_drop(
    lf: pl.LazyFrame, ignore_missing: bool = True
) -> pl.LazyFrame:
    columns_to_remove = list(td_helpers.SYSTEM_COLUMNS)
    if ignore_missing:
        existing_columns = set(lf.collect_schema().names())
        columns_to_remove = [
            col for col in columns_to_remove if col in existing_columns
        ]
    for column in columns_to_remove:
        lf = lf.drop(column)
    return lf


def drop_system_columns_drop_in_batch(
    lf: pl.LazyFrame, ignore_missing: bool = True
) -> pl.LazyFrame:
    columns_to_remove = list(td_helpers.SYSTEM_COLUMNS)
    columns = lf.collect_schema().names()
    dtypes = lf.collect_schema().dtypes()
    schema = {col: dtype for col, dtype in zip(columns, dtypes)}
    if ignore_missing:
        existing_columns = set(lf.collect_schema().names())
        columns_to_remove = [
            col for col in columns_to_remove if col in existing_columns
        ]
    for column in columns_to_remove:
        schema.pop(column)
    schema = pl.Schema(schema)

    lf = lf.map_batches(
        lambda batch: batch.drop(columns_to_remove),
        predicate_pushdown=True,
        streamable=True,
        schema=schema,
        # ToDo: ⚠️ Dimas: There should be a better way...
        validate_output_schema=False,
    )
    return lf


def drop_system_columns_select_in_batch(
    lf: pl.LazyFrame, ignore_missing: bool = True
) -> pl.LazyFrame:
    system_columns = set(td_helpers.SYSTEM_COLUMNS)
    source_columns = lf.collect_schema().names()
    target_columns = [col for col in source_columns if col not in system_columns]
    if not ignore_missing:
        missing_columns = system_columns - set(source_columns)
        if missing_columns:
            raise ValueError(f"Missing expected system columns: {missing_columns}")
    schema = {col: lf.collect_schema().get(col) for col in target_columns}
    schema = pl.Schema(schema)
    lf = lf.map_batches(
        lambda batch: batch.select(target_columns),
        predicate_pushdown=True,
        streamable=True,
        schema=schema,
        # ToDo: ⚠️ Dimas: There should be a better way...
        validate_output_schema=False,
    )

    return lf
