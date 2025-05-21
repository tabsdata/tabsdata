#
# Copyright 2025 Tabs Data Inc.
#

from __future__ import annotations

import logging
from typing import Any, Iterable, Literal, TypeAlias

import polars as pl

# noinspection PyProtectedMember
import tabsdata.utils.tableframe._constants as td_constants
import tabsdata.utils.tableframe._helpers as td_helpers
from tabsdata.exceptions import ErrorCode, TableFrameError
from tabsdata.extensions.tableframe.extension import TableFrameExtension

logger = logging.getLogger(__name__)
logger.setLevel(logging.DEBUG)

AddSystemColumnsMode: TypeAlias = Literal[
    # TableFrame creates all the system columns, even if existing.
    "raw",
    # TableFrame creates only the unexisting system columns.
    "tab",
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
    *,
    lf: pl.LazyFrame,
    mode: AddSystemColumnsMode,
    idx: int | None = None,
) -> pl.LazyFrame:
    if mode == "raw":
        lf = drop_system_columns(
            lf=lf,
            ignore_missing=True,
        )

    current_columns = set(lf.collect_schema().names())
    for column, metadata in td_helpers.SYSTEM_COLUMNS_METADATA.items():
        if column in current_columns:
            continue

        dtype, default, generator = (
            metadata[td_constants.TD_COL_DTYPE],
            metadata[td_constants.TD_COL_DEFAULT],
            metadata[td_constants.TD_COL_GENERATOR],
        )
        if isinstance(generator, str):
            lf = TableFrameExtension.instance().apply_system_column(
                lf,
                column,
                generator,
            )
        else:
            generator_ = generator(idx)
            lf = lf.with_columns(default().alias(column))
            lf = lf.with_columns_seq(
                pl.col(column)
                .map_elements(generator_, return_dtype=dtype)
                .alias(column)
            )

    return lf


def drop_system_columns(
    *,
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
