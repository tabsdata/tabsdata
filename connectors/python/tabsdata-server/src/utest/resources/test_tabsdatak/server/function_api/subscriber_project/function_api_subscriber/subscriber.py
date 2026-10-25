#
# Copyright 2026 Tabsdata Inc.
#

from __future__ import annotations

import polars as pl

from tabsdatak.api import TableFrameSpec, subscriber
from tabsdatak.conn.localfile import LocalFileDest

DESTINATION = LocalFileDest(paths=["sink.parquet"])


@subscriber(destination=DESTINATION, input_tables=["c/orders"])
def deliver_orders(orders: TableFrameSpec) -> TableFrameSpec:
    return orders.filter(pl.col("amount") > 10.0)
