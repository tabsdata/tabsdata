#
# Copyright 2026 Tabsdata Inc.
#

"""
title: Publish in-line development data
kind: publisher
connectors: [dev-datagen-in]
tags: [dev, datagen, testing, chunked]
summary: >-
  Emits in-line dataframes without any external system, handy for building
  and testing downstream functions. The second table is chunked: each run
  emits the next rows, and an empty frame with the same columns once the frame
  is exhausted.
"""

import polars as pl

from tabsdatak.api import TableFrameSpec, publisher
from tabsdatak.conn.dev import Data, DataGenSrc

CUSTOMERS = pl.DataFrame({
    "customer_id": [1, 2, 3],
    "name": ["Ada", "Grace", "Linus"],
    "country": ["UK", "US", "FI"],
})

ORDERS = pl.DataFrame({
    "order_id": list(range(1, 21)),
    "customer_id": [1 + i % 3 for i in range(20)],
    "amount": [10.0 + i for i in range(20)],
})


@publisher(
    source=DataGenSrc(data=[Data(CUSTOMERS), Data(ORDERS, chunk_size=5)]),
    output_tables=["customers", "orders"],
)
def pub_datagen_dev(
    customers: TableFrameSpec, orders: TableFrameSpec
) -> tuple[TableFrameSpec, TableFrameSpec]:
    return customers, orders
