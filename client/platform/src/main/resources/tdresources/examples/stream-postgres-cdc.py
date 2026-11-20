#
# Copyright 2026 Tabsdata Inc.
#

"""
title: Stream Postgres changes with CDC
kind: stream_publisher
connectors: [postgres-cdc]
tags: [stream, cdc, logical-replication, replication-slot]
summary: >-
  Captures inserts, updates and deletes from two Postgres tables through a
  logical replication slot, one output table per tracked table. Each row
  carries @td.cdc.meta.* columns (op, commit ts, tx) and the new values
  inlined as columns. A slot admits one consumer and pins WAL until it is
  consumed, so drop it if the stream is retired.
"""

from tabsdatak.api import TableFrameSpec, TableFramesSpec, stream_publisher
from tabsdatak.conn.postgres import PostgresCdcStreamSrc
from tabsdatak.tableframe.functions import concat


@stream_publisher(
    source=PostgresCdcStreamSrc(
        tables=["public.customers", "public.orders"],
        replication_slot="td_customers_orders",
        replication_slot_behavior="create",
        start_from="tail",
    ),
    batch_max_secs=60,
    output_tables=["customers_changes", "orders_changes"],
)
def stream_postgres_cdc(
    customers: TableFramesSpec, orders: TableFramesSpec
) -> tuple[TableFrameSpec, TableFrameSpec]:
    return concat(customers), concat(orders)
