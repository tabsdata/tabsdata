#
# Copyright 2026 Tabsdata Inc.
#

"""
title: Select, rename and cast columns
kind: transformer
connectors: []
tags: [tableframe, select, rename, cast]
summary: >-
  Keeps only the columns downstream needs, gives them clean names and fixes
  their types, deriving a new column along the way.
"""

from tabsdatak.api import TableFrameSpec, transformer
from tabsdatak.tableframe import datatypes
from tabsdatak.tableframe.functions import col

SAMPLE = {
    "orders": {
        "ORDER_ID": ["1", "2", "3"],
        "CUST": [10, 11, 10],
        "AMT": ["19.90", "5.00", "120.50"],
        "QTY": [1, 2, 3],
        "internal_note": ["a", "b", "c"],
    },
}


@transformer(
    input_tables=["orders"],
    output_tables=["orders_clean"],
)
def select_rename_cast(orders: TableFrameSpec) -> TableFrameSpec:
    if orders is None:
        return None
    return (
        orders
        .select("ORDER_ID", "CUST", "AMT", "QTY")
        .rename({
            "ORDER_ID": "order_id",
            "CUST": "customer_id",
            "AMT": "amount",
            "QTY": "quantity",
        })
        .cast({"order_id": datatypes.Int64, "amount": datatypes.Float64})
        .with_columns((col("amount") / col("quantity")).alias("unit_price"))
    )
