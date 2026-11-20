#
# Copyright 2026 Tabsdata Inc.
#

"""
title: Stream MySQL changes with CDC, starting from a snapshot
kind: stream_publisher
connectors: [mysql-cdc]
tags: [stream, cdc, binlog, initial-load]
summary: >-
  Reads the MySQL binlog for two tables; the server must run with
  gtid_mode=ON. start_from="full" first emits a full snapshot of each table,
  opened by a load marker, and then captures changes from the exact snapshot
  position. A batch closes every 10000 rows or, failing that, every five
  minutes (batch_max_rows, batch_max_secs).
"""

from tabsdatak.api import TableFrameSpec, TableFramesSpec, stream_publisher
from tabsdatak.conn.mysql import MySQLCdcStreamSrc
from tabsdatak.tableframe.functions import concat


@stream_publisher(
    source=MySQLCdcStreamSrc(
        tables=["shop.products", "shop.inventory"],
        start_from="full",
    ),
    batch_max_rows=10000,
    batch_max_secs=300,
    output_tables=["products_changes", "inventory_changes"],
)
def stream_mysql_cdc(
    products: TableFramesSpec, inventory: TableFramesSpec
) -> tuple[TableFrameSpec, TableFrameSpec]:
    return concat(products), concat(inventory)
