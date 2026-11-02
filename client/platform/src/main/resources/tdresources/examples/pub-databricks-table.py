#
# Copyright 2026 Tabsdata Inc.
#

"""
title: Publish a Databricks table
kind: publisher
connectors: [databricks-bulk-in]
tags: [warehouse, sql, unity-catalog]
summary: >-
  Reads a Unity Catalog table through the connection's SQL warehouse using a
  fully qualified catalog.schema.table name.
"""

from tabsdatak.api import TableFrameSpec, publisher
from tabsdatak.conn.databricks import DatabricksSrc


@publisher(
    source=DatabricksSrc(
        queries=["SELECT * FROM main.sales.transactions"],
    ),
    output_tables=["transactions"],
)
def pub_databricks_table(transactions: TableFrameSpec) -> TableFrameSpec:
    return transactions
