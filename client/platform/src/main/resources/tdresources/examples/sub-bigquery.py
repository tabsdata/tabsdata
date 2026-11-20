#
# Copyright 2026 Tabsdata Inc.
#

"""
title: Append to BigQuery with schema evolution
kind: subscriber
connectors: [bigquery-bulk-out]
tags: [warehouse, append, schema-evolution]
summary: >-
  Appends each new version of an orders table to a BigQuery table, so the
  input has to hold only new rows, as an incremental publisher's output does
  (pub-postgres-incremental). It reads @NEW, so every version lands once even
  when several commit between two runs, and a run with no new version
  writes nothing. watermark_column makes a rerun of the same transaction a
  no-op. A bare table name is completed from the connection's
  project and dataset; schema_evolution update adds columns that appear in
  the incoming data instead of rejecting the write.
"""

from tabsdatak.api import TableFrameSpec, TableFramesSpec, subscriber
from tabsdatak.conn.bigquery import BigQueryDest
from tabsdatak.tableframe.functions import concat


@subscriber(
    destination=BigQueryDest(
        tables=["`orders`"],
        if_table_exists="append",
        schema_evolution="update",
        watermark_column="td_trx_id",
    ),
    input_tables=["sales/orders@NEW"],
)
def sub_bigquery(orders: TableFramesSpec) -> TableFrameSpec:
    return concat(orders)
