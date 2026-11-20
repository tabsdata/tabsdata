#
# Copyright 2026 Tabsdata Inc.
#

"""
title: Keep every batch in one table
kind: transformer
connectors: []
tags: [incremental, accumulate, history, new]
summary: >-
  Adds each new batch of orders (read at @NEW) to orders_all, its own output
  read at @HEAD, so orders_all holds every order and a subscriber can replace
  its destination with it; a reader of the incremental orders table at @HEAD
  would see only its last batch. A function is never
  triggered by its own output, so it runs on a new version of orders; a run
  with no new version returns None, and orders_all stays as it is. orders
  must get an empty frame when nothing is new, never None, or its last
  batch is added again.
"""

from tabsdatak.api import TableFrameSpec, TableFramesSpec, transformer
from tabsdatak.tableframe.functions import concat

SAMPLE = {
    "new_orders": {"order_id": [3, 4], "amount": [12.5, 8.0]},
    "orders_all": {"order_id": [1, 2], "amount": [10.0, 20.0]},
}


@transformer(
    input_tables=["orders@NEW", "orders_all@HEAD"],
    output_tables=["orders_all"],
)
def accumulate_batches(
    new_orders: TableFramesSpec, orders_all: TableFrameSpec
) -> TableFrameSpec:
    batch = concat(new_orders)
    if batch is None:
        return None
    if orders_all is None:
        return batch
    return concat([orders_all, batch])
