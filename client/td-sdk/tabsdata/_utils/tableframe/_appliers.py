#
# Copyright 2025 Tabs Data Inc.
#

from __future__ import annotations

import logging
from typing import Any

import polars as pl

# noinspection PyProtectedMember
import tabsdata._utils.tableframe._constants as td_constants
from tabsdata.tableframe.lazyframe.properties import TableFrameProperties

logger = logging.getLogger(__name__)
logger.setLevel(logging.DEBUG)


def apply_constant_system_column(
    lf: pl.LazyFrame,
    column: str,
    dtype: pl.DataType,
    default: Any,
    function: str,
    properties: TableFrameProperties = None,
) -> pl.LazyFrame:
    if function.startswith(td_constants.TD_VER_COLUMN_PREFIX):
        if properties is None:
            property_value = default
        else:
            property_name = function.removeprefix(td_constants.TD_VER_COLUMN_PREFIX)
            property_value = getattr(properties, property_name, None)
            if property_value is None:
                property_value = default
        return lf.with_columns(pl.lit(property_value, dtype=dtype).alias(column))
    else:
        raise ValueError(
            f"Invalid function to generate a new system column: {function}"
        )
