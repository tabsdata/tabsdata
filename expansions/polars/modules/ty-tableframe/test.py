#
#  Copyright 2025 Tabs Data Inc.
#

import polars as pl

from tabsdata.expansions.tableframe import dummy

lf = pl.LazyFrame(
    {
        "a": ["1", "2", "3", "4", "5"] * 5,
    }
)

print("Starting...")
print(lf.collect())
lf = lf.with_columns(dummy("a"))
print("Done")
print(lf.collect())
