#
# Copyright 2026 Tabsdata Inc.
#

"""
title: Keep the last good snapshot
kind: transformer
connectors: []
tags: [none-output, validation, snapshot]
summary: >-
  Republishes the latest prices snapshot unless it looks broken: when it has
  no rows, or a price that is missing or not positive, the function returns
  None. Its version then stands for the previous one, so readers keep the last
  good prices. None is for this case only, when the output's current data is
  still right; an empty frame would publish no prices at all.
"""

from tabsdatak.api import TableFrameSpec, transformer
from tabsdatak.tableframe.functions import col

SAMPLE = {
    "prices": {"sku": ["a", "b", "c"], "price": [10.0, 22.0, 5.0]},
}


@transformer(
    input_tables=["prices"],
    output_tables=["prices_published"],
)
def keep_last_good(prices: TableFrameSpec) -> TableFrameSpec:
    if prices is None or prices.first().extract_one() is None:
        return None
    broken = prices.filter(col("price").is_null() | (col("price") <= 0))
    if broken.first().extract_one() is not None:
        return None
    return prices
