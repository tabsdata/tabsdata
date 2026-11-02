#
# Copyright 2026 Tabsdata Inc.
#

"""
title: Window functions without over()
kind: transformer
connectors: []
tags: [tableframe, window, self-join, running-total, top-n, previous-row]
summary: >-
  TableFrame has no over(), cumulative or rolling functions; a self-join on
  the partition key followed by a group_by gives the same results. Each order
  gets its customer's previous order (a left join keeps the first one, with
  nulls) and its customer's running total, ordered by order_id; top_orders
  keeps the two largest orders of each country, ties included.
"""

from tabsdatak.api import TableFrameSpec, transformer
from tabsdatak.tableframe import datatypes
from tabsdatak.tableframe.functions import col

SAMPLE = {
    "orders": {
        "order_id": [1, 2, 3, 4, 5, 6],
        "customer_id": [10, 11, 10, 10, 11, 12],
        "country": ["ES", "US", "ES", "ES", "US", "US"],
        "amount": [20.0, 5.0, 120.0, 30.0, 50.0, 50.0],
    },
}


@transformer(
    input_tables=["orders"],
    output_tables=["orders_with_history", "top_orders"],
)
def window_workarounds(
    orders: TableFrameSpec,
) -> tuple[TableFrameSpec, TableFrameSpec]:
    if orders is None:
        return None, None
    pairs = orders.select("order_id", "customer_id", "amount").join(
        orders.select("order_id", "customer_id", "amount"), on="customer_id"
    )
    running = (
        pairs
        .filter(col("order_id_right") <= col("order_id"))
        .group_by("order_id")
        .agg(col("amount_right").sum().alias("running_total"))
    )
    previous = (
        pairs
        .filter(col("order_id_right") < col("order_id"))
        .group_by("order_id")
        .agg(col("order_id_right").max().alias("previous_order_id"))
        .join(
            orders.select(
                col("order_id").alias("previous_order_id"),
                col("amount").alias("previous_amount"),
            ),
            on="previous_order_id",
        )
    )
    history = (
        orders
        .join(previous, on="order_id", how="left")
        .join(running, on="order_id")
        .sort("order_id")
    )
    top = (
        orders
        .join(orders.select("country", "amount"), on="country")
        .with_columns(
            (col("amount_right") > col("amount")).cast(datatypes.Int64).alias("larger")
        )
        .group_by("order_id")
        .agg(col("larger").sum().alias("larger_orders"))
        .filter(col("larger_orders") < 2)
        .join(orders, on="order_id")
        .drop("larger_orders")
        .sort("country", "order_id")
    )
    return history, top
