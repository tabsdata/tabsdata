#
# Copyright 2026 Tabsdata Inc.
#

"""
title: Aggregate with group_by
kind: transformer
connectors: []
tags: [tableframe, group-by, aggregate]
summary: >-
  Computes per-country order counts, revenue, average and distinct customers,
  sorted by revenue.
"""

from tabsdatak.api import TableFrameSpec, transformer
from tabsdatak.tableframe.functions import col

SAMPLE = {
    "orders": {
        "order_id": [1, 2, 3, 4],
        "customer_id": [10, 11, 10, 12],
        "country": ["ES", "US", "ES", "US"],
        "amount": [19.9, 5.0, 120.5, 30.0],
    },
}


@transformer(
    input_tables=["orders"],
    output_tables=["revenue_by_country"],
)
def aggregate_groupby(orders: TableFrameSpec) -> TableFrameSpec:
    if orders is None:
        return None
    return (
        orders
        .group_by("country")
        .agg(
            col("order_id").count().alias("orders"),
            col("amount").sum().alias("revenue"),
            col("amount").mean().alias("avg_order"),
            col("customer_id").n_unique().alias("customers"),
        )
        .sort("revenue", descending=True)
    )
