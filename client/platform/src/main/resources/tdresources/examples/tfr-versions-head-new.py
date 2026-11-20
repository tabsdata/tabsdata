#
# Copyright 2026 Tabsdata Inc.
#

"""
title: Compare table versions with @HEAD, @HEAD^ and @NEW
kind: transformer
connectors: []
tags: [versions, head, new, diff, incremental]
summary: >-
  prices@HEAD is the latest version with data and prices@HEAD^ the one before
  it, each a single TableFrame, or None while there is none. orders@NEW is
  every version committed since this transformer last ran, so it arrives as a
  list and is typed TableFramesSpec. Emits the prices that changed between the
  two latest versions and the new orders priced at the current price. A run
  started by a new prices version may bring no orders: orders@HEAD gives the
  columns, and its clear() an empty frame, so orders_priced gets a version
  with no rows. price_changes is recomputed on every run, orders-driven ones
  included, so it is read at @HEAD. Orders committed before the first prices
  version are never priced.
"""

from tabsdatak.api import TableFrameSpec, TableFramesSpec, transformer
from tabsdatak.tableframe.functions import col, concat

SAMPLE = {
    "prices": {"sku": ["a", "b", "c"], "price": [10.0, 22.0, 5.0]},
    "prices_prev": {"sku": ["a", "b"], "price": [10.0, 20.0]},
    "new_orders": {"order_id": [1, 2], "sku": ["b", "c"], "quantity": [1, 4]},
    "orders_head": {"order_id": [1, 2], "sku": ["b", "c"], "quantity": [1, 4]},
}


@transformer(
    input_tables=["prices@HEAD", "prices@HEAD^", "orders@NEW", "orders@HEAD"],
    output_tables=["price_changes", "orders_priced"],
)
def versions_head_new(
    prices: TableFrameSpec,
    prices_prev: TableFrameSpec,
    new_orders: TableFramesSpec,
    orders_head: TableFrameSpec,
) -> tuple[TableFrameSpec, TableFrameSpec]:
    if prices is None:
        return None, None
    current = prices.select("sku", "price")
    if prices_prev is None:
        changes = current
    else:
        changes = current.join(
            prices_prev.select("sku", col("price").alias("previous_price")),
            on="sku",
            how="left",
        ).filter(
            col("previous_price").is_null() | (col("price") != col("previous_price"))
        )
    if orders_head is None:
        return changes, None
    orders = concat(new_orders)
    if orders is None:
        orders = orders_head.clear()
    priced = orders.join(current, on="sku", how="left").with_columns(
        (col("price") * col("quantity")).alias("amount")
    )
    return changes, priced
