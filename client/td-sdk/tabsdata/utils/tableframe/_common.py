#
# Copyright 2025 Tabs Data Inc.
#

from __future__ import annotations

from typing import Any, Callable, Iterable, Literal, TypeAlias

import polars as pl
from polars import DataFrame

# noinspection PyProtectedMember
import tabsdata.tableframe._typing as td_typing
import tabsdata.utils.tableframe._constants as td_constants
import tabsdata.utils.tableframe._helpers as td_helpers
from tabsdata.exceptions import ErrorCode, TableFrameError

DropColumnsStrategy: TypeAlias = Literal[
    "drop",
    "select",
]


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


def _generator(fn):
    while True:
        yield fn()


def add_system_columns(ldf: pl.LazyFrame) -> pl.LazyFrame:
    columns = ldf.collect_schema().names()
    dtypes = ldf.collect_schema().dtypes()
    schema_dictionary = {col: dtype for col, dtype in zip(columns, dtypes)}
    existing_columns = set(ldf.collect_schema().names())
    for column, metadata in td_helpers.SYSTEM_COLUMNS_METADATA.items():
        if column in existing_columns:
            continue
        dtype, default, generator = (
            metadata[td_constants.TD_COL_DTYPE],
            metadata[td_constants.TD_COL_DEFAULT],
            metadata[td_constants.TD_COL_GENERATOR],
        )
        ldf = ldf.with_columns(default().alias(column))
        schema_dictionary[column] = dtype
        schema = pl.Schema(schema_dictionary)
        # fmt: off
        ldf = ldf.map_batches(
            lambda
            batch,
            lambda_column=column,
            lambda_dtype=dtype,
            lambda_generator=generator:
            add_system_column(lambda_column,
                              lambda_dtype,
                              lambda_generator,
                              batch),
            predicate_pushdown=True,
            streamable=True,
            schema=schema,
            # ToDo: ⚠️ Dimas: There should be a better way...
            validate_output_schema=False
        )
        # fmt: on
    return ldf


def add_system_column(
    column: str,
    dtype: td_typing.TdDataType,
    generator: Callable[[], Any],
    batch: DataFrame,
) -> DataFrame:
    return batch.with_columns(
        pl.first()
        .map_batches(
            lambda chunk: generate_system_column(column, dtype, generator, len(chunk)),
            is_elementwise=True,
        )
        .alias(column)
    )


def generate_system_column(
    column: str, dtype: td_typing.TdDataType, generator: Callable[[], Any], size: int
):
    values = []
    for i in range(size):
        values.append(generator())
    return pl.Series(column, values, dtype=dtype)


def drop_system_columns(
    ldf: pl.LazyFrame,
    strategy: DropColumnsStrategy = "select",
    ignore_missing: bool = True,
) -> pl.LazyFrame:
    match strategy:
        case "drop":
            return drop_system_columns_drop(ldf, ignore_missing)
        case "select":
            return drop_system_columns_select(ldf, ignore_missing)
        case _:
            raise ValueError(f"Unknown drop column strategy: {strategy}")


def drop_system_columns_drop(
    ldf: pl.LazyFrame, ignore_missing: bool = True
) -> pl.LazyFrame:
    columns_to_remove = list(td_helpers.SYSTEM_COLUMNS)
    columns = ldf.collect_schema().names()
    dtypes = ldf.collect_schema().dtypes()
    schema = {col: dtype for col, dtype in zip(columns, dtypes)}
    if ignore_missing:
        existing_columns = set(ldf.collect_schema().names())
        columns_to_remove = [
            col for col in columns_to_remove if col in existing_columns
        ]
    for column in columns_to_remove:
        schema.pop(column)
    schema = pl.Schema(schema)

    ldf = ldf.map_batches(
        lambda batch: batch.drop(columns_to_remove),
        predicate_pushdown=True,
        streamable=True,
        schema=schema,
        # ToDo: ⚠️ Dimas: There should be a better way...
        validate_output_schema=False,
    )
    return ldf


def drop_system_columns_select(
    ldf: pl.LazyFrame, ignore_missing: bool = True
) -> pl.LazyFrame:
    system_columns = set(td_helpers.SYSTEM_COLUMNS)
    source_columns = ldf.collect_schema().names()
    target_columns = [col for col in source_columns if col not in system_columns]
    if not ignore_missing:
        missing_columns = system_columns - set(source_columns)
        if missing_columns:
            raise ValueError(f"Missing expected system columns: {missing_columns}")
    schema = {col: ldf.collect_schema().get(col) for col in target_columns}
    schema = pl.Schema(schema)
    ldf = ldf.map_batches(
        lambda batch: batch.select(target_columns),
        predicate_pushdown=True,
        streamable=True,
        schema=schema,
        # ToDo: ⚠️ Dimas: There should be a better way...
        validate_output_schema=False,
    )

    return ldf
