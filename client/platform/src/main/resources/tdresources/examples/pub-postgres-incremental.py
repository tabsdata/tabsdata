#
# Copyright 2026 Tabsdata Inc.
#

"""
title: Incremental Postgres publisher with a watermark
kind: publisher
connectors: [postgres-sql-in]
tags: [sql, incremental, watermark, initial-values]
summary: >-
  Reads only the rows inserted since the last run: an increasing id is the
  watermark, so updates to existing rows are not picked up. The :since bind
  is seeded by initial_values on the first run; afterwards the value saved
  with ctx.set_attr is used. The watermark is read off the rows actually
  published, and the query orders by it so paging never skips rows. A run
  without new rows publishes an empty frame and keeps the watermark.
"""

from tabsdatak.api import TableFrameSpec, TransactionCtx, publisher
from tabsdatak.conn.postgres import PostgresSrc


@publisher(
    source=PostgresSrc(
        queries=[
            "SELECT * FROM public.orders WHERE id > :since ORDER BY id LIMIT 10000"
        ],
        initial_values={"since": 0},
    ),
    output_tables=["orders"],
)
def pub_postgres_incremental(
    orders: TableFrameSpec, ctx: TransactionCtx
) -> TableFrameSpec:
    if orders is None:
        return None
    top = orders.max_for("id")
    if top is not None:
        ctx.set_attr("since", int(top["id"]))
    return orders
