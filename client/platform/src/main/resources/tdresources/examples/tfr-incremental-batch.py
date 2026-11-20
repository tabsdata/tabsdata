#
# Copyright 2026 Tabsdata Inc.
#

"""
title: Transform each new batch of an incremental table
kind: transformer
connectors: []
tags: [incremental, new, head, batch]
summary: >-
  Cleans each new batch of an incremental table into orders_clean, whose
  versions also hold only new rows, so a subscriber appends them at @NEW
  (sub-awsglue). orders@NEW brings every version since the last run; a run
  started by hand may bring none, so the function also reads orders@HEAD and
  runs on its clear(): orders_clean then gets a version with no rows and the
  same columns. Returning None instead would hand an @NEW reader the last
  batch again. It is registered in the sales collection.
"""

from tabsdatak.api import TableFrameSpec, TableFramesSpec, transformer
from tabsdatak.tableframe.functions import col, concat

SAMPLE = {
    "new_orders": {"order_id": [3, 4], "amount": [12.5, -1.0]},
    "orders": {"order_id": [3, 4], "amount": [12.5, -1.0]},
}


@transformer(
    input_tables=["orders_src/orders@NEW", "orders_src/orders@HEAD"],
    output_tables=["orders_clean"],
)
def incremental_batch(
    new_orders: TableFramesSpec, orders: TableFrameSpec
) -> TableFrameSpec:
    if orders is None:
        return None
    batch = concat(new_orders)
    if batch is None:
        batch = orders.clear()
    return batch.filter(col("amount") > 0)
