#
# Copyright 2025 Tabs Data Inc.
#

from __future__ import annotations

import polars as pl

import tabsdata.utils.tableframe._helpers as td_helpers
from tabsdata.exceptions import ErrorCode, TableFrameError


def check_required_columns(df: pl.DataFrame | pl.LazyFrame):
    """
    Check if any required column is missing.
    This can depend on the interceptor implementation.
    """
    missing_columns = [
        column
        for column in td_helpers.REQUIRED_COLUMNS
        if column not in df.collect_schema().names()
    ]
    if missing_columns:
        raise TableFrameError(ErrorCode.TF1, missing_columns)
