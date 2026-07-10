#
# Copyright 2026 Tabsdata Inc.
#

from __future__ import annotations

import pandas as pd
import polars as pl


def pretty_polars() -> None:
    pl.Config.set_tbl_cols(1024)
    pl.Config.set_tbl_rows(1024)
    pl.Config.set_fmt_table_cell_list_len(1024)
    pl.Config.set_tbl_width_chars(4096)
    pl.Config.set_fmt_str_lengths(4096)


def pretty_pandas() -> None:
    pd.set_option("display.max_columns", None)
    pd.set_option("display.max_rows", None)
    pd.set_option("display.max_colwidth", None)
    pd.set_option("display.width", 0)
    pd.set_option("display.expand_frame_repr", False)
