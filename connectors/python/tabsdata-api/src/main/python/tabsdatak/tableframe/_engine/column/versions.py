#
# Copyright 2026 Tabsdata Inc.
#

from __future__ import annotations

from typing import TYPE_CHECKING

from tabsdatak.tableframe._engine.constant.column.names import (
    TD_EXECUTION_SYSTEM_COLUMN_NAME,
    TD_TIMESTAMP_SYSTEM_COLUMN_NAME,
    TD_TRANSACTION_SYSTEM_COLUMN_NAME,
    TD_VERSION_SYSTEM_COLUMN_NAME,
)

if TYPE_CHECKING:
    import polars as pl_

    from tabsdatak.tableframe._engine.model.frame import DataVersion as DataVersion_

VERSION_COLUMNS = (
    TD_EXECUTION_SYSTEM_COLUMN_NAME,
    TD_TRANSACTION_SYSTEM_COLUMN_NAME,
    TD_VERSION_SYSTEM_COLUMN_NAME,
    TD_TIMESTAMP_SYSTEM_COLUMN_NAME,
)


def latest_version() -> list[pl_.Expr]:
    """A group's version: the latest one among its rows. Version ids are
    time-ordered, and every version column is taken from that same row, so a
    group never mixes the parts of two versions."""
    import polars as pl

    return [
        pl.col(column).sort_by(pl.col(TD_VERSION_SYSTEM_COLUMN_NAME)).last()
        for column in VERSION_COLUMNS
    ]


def fill_missing_version(lf: pl_.LazyFrame, version: DataVersion_) -> pl_.LazyFrame:
    """Give rows with no version the frame's: a row only the right side of a
    join had keeps none of the left's, and a join keeps only the left's."""
    import polars as pl

    schema = lf.collect_schema()
    filled = [
        pl.col(column).fill_null(
            pl.lit(getattr(version, column.rsplit(".", 1)[-1]), dtype=schema[column])
        )
        for column in VERSION_COLUMNS
        if column in schema
    ]
    return lf.with_columns(filled) if filled else lf
