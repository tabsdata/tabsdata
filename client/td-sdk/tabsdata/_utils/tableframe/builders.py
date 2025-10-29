#
#  Copyright 2025 Tabs Data Inc.
#

from __future__ import annotations

from typing import TYPE_CHECKING, Any

import polars as pl

import tabsdata.tableframe.lazyframe.frame as td_frame

# noinspection PyProtectedMember
import tabsdata.tableframe.typing as td_typing

if TYPE_CHECKING:
    import pandas as pd


def empty() -> td_frame.TableFrame:
    """
    Creates an empty (no column - no row) TableFrame.
    """
    return td_frame.TableFrame.__build__(
        df=None,
        mode="tab",
        idx=None,
        properties=None,
    )


def from_polars(data: pl.LazyFrame | pl.DataFrame | None = None) -> td_frame.TableFrame:
    """
    Creates tableframe from a polars dataframe or lazyframe, or None.
    `None` produces as an empty (no column - no row) tableframe.

    Args:
        data: Input data.
    """
    return td_frame.TableFrame.from_polars(data=data)


def from_pandas(data: pd.DataFrame | None = None) -> td_frame.TableFrame:
    """
    Creates tableframe from a pandas dataframe, or None.
    `None` produces as an empty (no column - no row) tableframe.

    Args:
        data: Input data.
    """
    return td_frame.TableFrame.from_pandas(data=data)


def from_dict(data: td_typing.TableDictionary | None = None) -> td_frame.TableFrame:
    """
    Creates tableframe from a dictionary, or None.
    `None` produces as an empty (no column - no row) tableframe.

    Args:
        data: Input data.
    """
    return td_frame.TableFrame.from_dict(data=data)


def to_polars_lf(data: td_frame.TableFrame) -> pl.LazyFrame:
    """
    Creates polars lazyframe from a tableframe, or None.
    `None` produces and empty (no column - no row) polars lazyframe.

    Args:
        data: Input data.
    """
    return data.to_polars_lf()


def to_polars_df(data: td_frame.TableFrame) -> pl.DataFrame:
    """
    Creates polars dataframe from a tableframe, or None.
    `None` produces and empty (no column - no row) polars dataframe.

    Args:
        data: Input data.
    """
    return data.to_polars_df()


def to_pandas(data: td_frame.TableFrame) -> pd.DataFrame:
    """
    Creates pandas dataframe from a tableframe, or None.
    `None` produces and empty (no column - no row) pandas dataframe.

    Args:
        data: Input data.
    """
    return data.to_pandas()


def to_dict(data: td_frame.TableFrame) -> dict[str, list[Any]]:
    """
    Creates dictionary from a tableframe, or None.
    `None` produces and empty (no key) dictionary.

    Args:
        data: Input data.
    """
    return data.to_dict()
