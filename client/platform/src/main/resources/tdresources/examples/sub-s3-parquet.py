#
# Copyright 2026 Tabsdata Inc.
#

"""
title: Write tables to S3 as Parquet
kind: subscriber
connectors: [s3-file-out]
tags: [file, parquet, multi-table]
summary: >-
  Writes two tables to S3 as Parquet objects, one path per input table in
  order. The format is inferred from the .parquet extension.
"""

from tabsdatak.api import TableFrameSpec, subscriber
from tabsdatak.conn.s3 import S3Dest


@subscriber(
    destination=S3Dest(paths=["lake/orders.parquet", "lake/customers.parquet"]),
    input_tables=["sales/orders", "crm/customers"],
)
def sub_s3_parquet(
    orders: TableFrameSpec, customers: TableFrameSpec
) -> tuple[TableFrameSpec, TableFrameSpec]:
    return orders, customers
