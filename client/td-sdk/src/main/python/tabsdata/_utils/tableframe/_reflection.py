#
# Copyright 2025 Tabs Data Inc.
#

from __future__ import annotations

import polars as pl

import tabsdata._utils.tableframe._helpers as td_helpers
from tabsdata.exceptions import ErrorCode, TableFrameError


def check_required_columns(df: pl.DataFrame | pl.LazyFrame):
    """
    Check whether any required column is missing.
    The definition of “required” can vary depending on the specific TableFrame
    extension.
    A required column is one that must be present in every TableFrame instance and in
    every table.
    """
    columns = df.collect_schema().names()
    missing_columns = [
        column for column in td_helpers.REQUIRED_COLUMNS if column not in columns
    ]
    if missing_columns:
        raise TableFrameError(ErrorCode.TF1, missing_columns)


def check_non_optional_columns(df: pl.DataFrame | pl.LazyFrame):
    """
    Check whether any non-optional column is missing.
    The definition of “non-optional” can vary depending on the specific TableFrame
    extension.
    A non-optional column is one that must be present in every table.
    Some columns are required but not persistent — these are known as virtual columns.
    They are automatically added to every TableFrame instance upon creation, but are
    removed when the TableFrame is stored as a physical table.
    In contrast, required columns are always present — both in every TableFrame instance
    and in every stored table.
    """
    columns = df.collect_schema().names()
    missing_columns = [
        column for column in td_helpers.NON_OPTIONAL_COLUMNS if column not in columns
    ]
    if missing_columns:
        raise TableFrameError(ErrorCode.TF14, missing_columns)
