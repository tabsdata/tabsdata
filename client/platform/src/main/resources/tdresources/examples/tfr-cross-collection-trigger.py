#
# Copyright 2026 Tabsdata Inc.
#

"""
title: Cross-collection inputs with a TablesTrigger
kind: transformer
connectors: []
tags: [trigger, tables-trigger, cross-collection]
summary: >-
  Reads tables from two other collections using collection/table references
  and fires only when sales/orders gets a new version; changes to
  crm/customers are picked up on the next orders-driven run.
"""

from tabsdatak.api import TableFrameSpec, TablesTrigger, transformer

SAMPLE = {
    "orders": {"order_id": [1, 2], "customer_id": [10, 11], "amount": [5.0, 7.5]},
    "customers": {"customer_id": [10], "region": ["EMEA"]},
}


@transformer(
    input_tables=["sales/orders", "crm/customers"],
    output_tables=["orders_by_region"],
    trigger_by=TablesTrigger(tables=["sales/orders"]),
)
def cross_collection(
    orders: TableFrameSpec, customers: TableFrameSpec
) -> TableFrameSpec:
    if orders is None or customers is None:
        return None
    return orders.join(
        customers.select("customer_id", "region"), on="customer_id", how="left"
    )
