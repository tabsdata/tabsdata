#
# Copyright 2026 Tabsdata Inc.
#

"""
title: Join two tables
kind: transformer
connectors: []
tags: [tableframe, join, enrich]
summary: >-
  Enriches orders with customer attributes via a left join, so orders whose
  customer is unknown are kept with null attributes.
"""

from tabsdatak.api import TableFrameSpec, transformer

SAMPLE = {
    "orders": {
        "order_id": [1, 2, 3],
        "customer_id": [10, 11, 99],
        "amount": [19.9, 5.0, 120.5],
    },
    "customers": {
        "customer_id": [10, 11],
        "name": ["Ada", "Grace"],
        "tier": ["gold", "silver"],
    },
}


@transformer(
    input_tables=["orders", "customers"],
    output_tables=["orders_enriched"],
)
def join_two_tables(
    orders: TableFrameSpec, customers: TableFrameSpec
) -> TableFrameSpec:
    if orders is None or customers is None:
        return None
    return orders.join(
        customers.select("customer_id", "name", "tier"),
        on="customer_id",
        how="left",
    ).select("order_id", "customer_id", "name", "tier", "amount")
