#
# Copyright 2025 Tabs Data Inc.
#

from __future__ import annotations

import logging
from typing import Any, Iterable, Literal, TypeAlias

import polars as pl

# noinspection PyProtectedMember
import tabsdata._utils.tableframe._constants as td_constants
import tabsdata._utils.tableframe._helpers as td_helpers
from tabsdata.exceptions import ErrorCode, TableFrameError

# noinspection PyProtectedMember
from tabsdata.extensions._tableframe.extension import TableFrameExtension
from tabsdata.tableframe.lazyframe.properties import TableFrameProperties

logger = logging.getLogger(__name__)
logger.setLevel(logging.DEBUG)

AddSystemColumnsMode: TypeAlias = Literal[
    # TableFrame creates all the system columns, even if existing.
    # (Normally used to create a new TableFrame from scratch. Used also in Source
    #  functions to statically stick their values before processing.)
    "raw",
    # TableFrame applies the inception policy defined in column.
    # (Normally used to store a TableFrame after function execution. Columns to keep are
    #  left with current value, and columns to regenerate are dropped and created back.
    #  In this case, future load will use these lastly generated values.)
    "sys",
    # TableFrame creates only the unexisting system columns.
    # (Normally used to load a TableFrame from a non-Source function, as the stored
    #  columns are assumed to have been stored properly with correct values to use in
    #  future function executions using them).
    "tab",
]


def check_column_name(name: str):
    if name.startswith(td_constants.TD_COLUMN_PREFIX):
        for namespaced_prefix in td_constants.TD_NAMESPACED_VIRTUAL_COLUMN_PREFIXES:
            if name.startswith(namespaced_prefix):
                return
        raise TableFrameError(ErrorCode.TF10, name)


def check_column(name: str):
    if name.startswith(td_constants.REGEXP_ANCHOR_START) and name.endswith(
        td_constants.REGEXP_ANCHOR_END
    ):
        raise TableFrameError(ErrorCode.TF3, name)
    if name in td_helpers.SYSTEM_COLUMNS:
        for namespaced_prefix in td_constants.TD_NAMESPACED_VIRTUAL_COLUMN_PREFIXES:
            if name.startswith(namespaced_prefix):
                return
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
    mode: AddSystemColumnsMode,
    idx: int | None = None,
    properties: TableFrameProperties = None,
) -> pl.LazyFrame:
    if mode == "raw":
        lf = drop_system_columns(
            lf=lf,
            ignore_missing=True,
        )
    elif mode == "sys":
        lf = drop_inception_regenerate_system_columns(
            lf=lf,
            ignore_missing=True,
        )

    current_columns = set(lf.collect_schema().names())

    is_void = False
    if len(current_columns) == 0 and lf.limit(1).collect().height == 0:
        is_void = True

    for column, metadata in td_helpers.SYSTEM_COLUMNS_METADATA.items():
        if column in current_columns:
            continue

        if isinstance(metadata.generator, str):
            lf = TableFrameExtension.instance().apply_system_column(
                lf,
                column,
                metadata.dtype,
                metadata.default,
                metadata.generator,
                properties,
            )
        else:
            # If a lazy frame has 0 rows and 0 columns, polars will create a new
            # single row when assigning a literal to a new column. This tweak
            # creates a lazy frame with the correct schema through a data frane,
            # Which does not have this undesired behavior
            if is_void:
                lf = pl.DataFrame(schema=[(column, metadata.dtype)]).lazy()
            else:
                lf = lf.with_columns(metadata.default().alias(column))
            generator_instance = metadata.generator(idx)
            match metadata.language:
                case td_constants.Language.PYTHON:
                    lf = lf.with_columns_seq(
                        pl.col(column)
                        .map_batches(
                            generator_instance.python,
                            return_dtype=metadata.dtype,
                        )
                        .alias(column)
                    )
                case td_constants.Language.RUST:
                    lf = lf.with_columns_seq(
                        generator_instance.rust(pl.col(column)).alias(column)
                    )
                case _:
                    raise ValueError(
                        "Unsupported function language to generate a new system"
                        f" column: {metadata.language}"
                    )
        is_void = False
    return lf


def drop_system_columns(
    lf: pl.LazyFrame,
    ignore_missing: bool = True,
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


def drop_inception_regenerate_system_columns(
    *,
    lf: pl.LazyFrame,
    ignore_missing: bool = True,
) -> pl.LazyFrame:
    columns_to_remove = [
        column
        for column, metadata in td_helpers.SYSTEM_COLUMNS_METADATA.items()
        if metadata.inception == td_constants.Inception.REGENERATE
    ]

    if ignore_missing:
        existing_columns = set(lf.collect_schema().names())
        columns_to_remove = [
            col for col in columns_to_remove if col in existing_columns
        ]
    for column in columns_to_remove:
        lf = lf.drop(column)
    return lf
