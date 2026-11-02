#
# Copyright 2026 Tabsdata Inc.
#

"""
title: Medallion append-only event bronze and silver
kind: flow
connectors: []
tags: [medallion, append-only, event, bronze, silver]
summary: >-
  Builds the event silver that medallion-gold-dim-fact reads, for a source
  whose rows are never updated (orders, payments, clicks). Each version of
  orders_src/orders holds only new rows, as an incremental publisher's output
  does (pub-postgres-incremental). build_append_only_cdc_bronze stamps every
  new row as an insert at its event time; build_append_only_silver drops the
  bookkeeping and the rows repeated in the batch or in the previous silver
  version. Each silver version holds only the new events, so consumers read
  it at @NEW. Both transformers are registered in the orders_silver
  collection.
"""

from tabsdatak.api import TableFrameSpec, TableFramesSpec, TablesTrigger, transformer
from tabsdatak.medallion import build_append_only_cdc_bronze, build_append_only_silver
from tabsdatak.tableframe import datatypes


@transformer(
    input_tables=["orders_src/orders@NEW"],
    output_tables=["orders_event_bronze"],
)
def tfr_orders_event_bronze(orders: TableFramesSpec) -> TableFrameSpec:
    return build_append_only_cdc_bronze(
        new=[frame for frame in orders or [] if frame is not None],
        pk=[{"order_id": datatypes.Int64}],
        event_ts_col="ordered_at",
    )


@transformer(
    input_tables=["orders_event_bronze@NEW", "orders_event_silver@HEAD"],
    output_tables=["orders_event_silver"],
    trigger_by=TablesTrigger(tables=["orders_event_bronze"]),
)
def tfr_orders_event_silver(
    bronze: TableFramesSpec, silver_head: TableFrameSpec
) -> TableFrameSpec:
    return build_append_only_silver(bronze_new=bronze or [], silver_head=silver_head)
