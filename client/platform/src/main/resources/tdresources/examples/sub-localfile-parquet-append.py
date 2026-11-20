#
# Copyright 2026 Tabsdata Inc.
#

"""
title: Append new batches to a folder of Parquet files
kind: subscriber
connectors: [local-file-out]
tags: [file, parquet, append, new, path-tokens]
summary: >-
  Adds each new batch of orders to a folder as its own Parquet file, named
  with ${EXEC_PLAN_ID}. It reads orders at @NEW, so every version is written
  once; a run with no new rows writes a file without rows.
"""

from tabsdatak.api import TableFrameSpec, TableFramesSpec, subscriber
from tabsdatak.conn.localfile import DestFileFormat, LocalFileDest
from tabsdatak.tableframe.functions import concat


@subscriber(
    destination=LocalFileDest(
        paths=["exports/orders/orders_${EXEC_PLAN_ID}.parquet"],
        format=DestFileFormat.PARQUET,
    ),
    input_tables=["sales/orders@NEW"],
)
def sub_localfile_parquet_append(orders: TableFramesSpec) -> TableFrameSpec:
    return concat(orders)
