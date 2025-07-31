#
#  Copyright 2025 Tabs Data Inc.
#

from __future__ import annotations

from typing import Any

import polars as pl

# noinspection PyProtectedMember
import tabsdata._utils.tableframe._constants as td_constants
from tabsdata.exceptions import ErrorCode, TableFrameError


class DataFrame:

    @classmethod
    def item(cls, lf: pl.LazyFrame) -> Any:
        schema = lf.collect_schema()
        if schema.len() == 1:
            if lf.limit(1).collect().height == 0:
                return None
            lf = lf.select(
                pl.first().alias(
                    td_constants.StandardVolatileSystemColumns.TD_ITEM_COLUMN.value
                )
            )
            lf = lf.with_columns(
                pl.col(td_constants.StandardVolatileSystemColumns.TD_ITEM_COLUMN.value)
                .min()
                .alias(td_constants.StandardVolatileSystemColumns.TD_MIN_COLUMN.value),
                pl.col(td_constants.StandardVolatileSystemColumns.TD_ITEM_COLUMN.value)
                .max()
                .alias(td_constants.StandardVolatileSystemColumns.TD_MAX_COLUMN.value),
            )
            lf = lf.limit(1)
            df = lf.collect()
            if df[0, 1] == df[0, 2]:
                return df[0, 0]
            raise TableFrameError(ErrorCode.TF9)
        raise TableFrameError(ErrorCode.TF8)
