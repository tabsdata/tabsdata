#
# Copyright 2026 Tabsdata Inc.
#

"""
title: Publish several MySQL queries as several tables
kind: publisher
connectors: [mysql-sql-in]
tags: [sql, multi-output, transactional]
summary: >-
  Runs three queries in one transactional session, so the tables are a
  consistent snapshot, and publishes one table per query in order.
"""

from tabsdatak.api import TableFrameSpec, publisher
from tabsdatak.conn.mysql import MySQLSrc


@publisher(
    source=MySQLSrc(
        queries=[
            "SELECT * FROM customers",
            "SELECT * FROM orders",
            "SELECT order_id, product_id, quantity FROM order_items",
        ],
        transactional=True,
    ),
    output_tables=["customers", "orders", "order_items"],
)
def pub_mysql_multi_query(
    customers: TableFrameSpec,
    orders: TableFrameSpec,
    order_items: TableFrameSpec,
) -> tuple[TableFrameSpec, TableFrameSpec, TableFrameSpec]:
    return customers, orders, order_items
