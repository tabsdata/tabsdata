#
# Copyright 2024 Tabs Data Inc.
#

import logging
from typing import Optional, Tuple

import polars as pl

import tabsdata as td

# noinspection PyProtectedMember

logger = logging.getLogger(__name__)
logger.setLevel(logging.DEBUG)


def pretty_polars():
    pl.Config.set_tbl_cols(1024)
    pl.Config.set_tbl_rows(1024)
    pl.Config.set_fmt_table_cell_list_len(1024)
    pl.Config.set_tbl_width_chars(4096)
    pl.Config.set_fmt_str_lengths(4096)


def load_simple_dataframe(
    token: Optional[str] = None,
) -> Tuple[pl.LazyFrame, pl.DataFrame, td.TableFrame]:
    data = {
        "intColumn": [1, 2, 3],
        "stringColumn": ["a", "b", "c"],
    }
    lazy_frame = pl.LazyFrame(data)

    data_frame = pl.DataFrame(lazy_frame.collect())
    table_frame = td.TableFrame.__build__(
        df=lazy_frame,
        mode="raw",
        idx=0,
    )
    return lazy_frame, data_frame, table_frame


def load_complex_dataframe(
    token: Optional[str] = None,
) -> Tuple[pl.LazyFrame, pl.DataFrame, td.TableFrame]:
    data = (
        "https://raw.githubusercontent.com/jeroenjanssens/"
        "python-polars-the-definitive-guide/main/data/penguins.csv"
    )

    lazy_frame = pl.scan_csv(data)
    data_frame = pl.DataFrame(lazy_frame.collect())
    table_frame = td.TableFrame.__build__(
        df=lazy_frame,
        mode="raw",
        idx=0,
    )
    return lazy_frame, data_frame, table_frame
