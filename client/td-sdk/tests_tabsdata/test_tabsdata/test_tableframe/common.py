#
# Copyright 2024 Tabs Data Inc.
#

import logging
import tempfile
from typing import Optional, Tuple

import pandas as pd
import polars as pl
import requests

import tabsdata as td
from tabsdata._utils.constants import tabsdata_temp_folder

logger = logging.getLogger(__name__)
logger.setLevel(logging.DEBUG)

POLARS_FILE_CACHE_TTL = 60 * 60 * 24


def pretty_polars():
    pl.Config.set_tbl_cols(1024)
    pl.Config.set_tbl_rows(1024)
    pl.Config.set_fmt_table_cell_list_len(1024)
    pl.Config.set_tbl_width_chars(4096)
    pl.Config.set_fmt_str_lengths(4096)


def pretty_pandas():
    pd.set_option("display.max_columns", None)
    pd.set_option("display.max_rows", None)
    pd.set_option("display.max_colwidth", None)
    pd.set_option("display.width", 0)
    pd.set_option("display.expand_frame_repr", False)


# noinspection PyUnusedLocal
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


# noinspection PyUnusedLocal
def load_complex_dataframe(
    token: Optional[str] = None,
) -> Tuple[pl.LazyFrame, pl.DataFrame, td.TableFrame]:
    data = (
        "https://raw.githubusercontent.com/jeroenjanssens/"
        "python-polars-the-definitive-guide/main/data/penguins.csv"
    )
    with tempfile.NamedTemporaryFile(
        mode="w+b",
        suffix=".csv",
        delete=False,
        dir=tabsdata_temp_folder(),
    ) as penguins:
        response = requests.get(data)
        response.raise_for_status()
        penguins.write(response.content)
    lazy_frame = pl.scan_csv(
        source=penguins.name,
        file_cache_ttl=POLARS_FILE_CACHE_TTL,
    )
    data_frame = pl.DataFrame(lazy_frame.collect())
    table_frame = td.TableFrame.__build__(
        df=lazy_frame,
        mode="raw",
        idx=0,
    )
    return lazy_frame, data_frame, table_frame
