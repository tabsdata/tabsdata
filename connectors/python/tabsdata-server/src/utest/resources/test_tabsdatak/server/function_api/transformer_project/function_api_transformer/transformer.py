#
# Copyright 2026 Tabsdata Inc.
#

from __future__ import annotations

from tabsdatak.api import TableFrameSpec, transformer


@transformer(
    input_tables=["c/orders", "c/customers"],
    output_tables=["enriched", "untouched"],
)
def enrich_orders(
    orders: TableFrameSpec, customers: TableFrameSpec
) -> tuple[TableFrameSpec, TableFrameSpec]:
    return orders.join(customers, on="customer_id", how="inner"), None
