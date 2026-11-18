#
# Copyright 2026 Tabsdata Inc.
#

"""
title: Publish a gold model to Postgres in one transaction
kind: subscriber
connectors: [postgres-sql-out]
tags: [sql, replace, model, transactional, multi-table]
summary: >-
  One subscriber writes a model's dimensions and fact, one destination table
  per input, so a reader gets them all from the same versions and
  transactional=True commits them together. It runs when any input changes and
  reads the latest version of each, so it rewrites the whole set with replace.
  Until every table has been built it returns None for every slot, and the
  destination keeps its last complete set: a slot returned as None leaves its
  table as it is.
"""

from tabsdatak.api import TableFrameSpec, subscriber
from tabsdatak.conn.postgres import PostgresDest


@subscriber(
    destination=PostgresDest(
        tables=[
            "analytics.dim_customer",
            "analytics.dim_customer_history",
            "analytics.fct_orders",
        ],
        if_table_exists="replace",
        transactional=True,
    ),
    input_tables=[
        "gold/dim_customer",
        "gold/dim_customer_history",
        "gold/fct_orders",
    ],
)
def publish_customer_model(
    dim_customer: TableFrameSpec,
    dim_customer_history: TableFrameSpec,
    fct_orders: TableFrameSpec,
) -> tuple[TableFrameSpec, TableFrameSpec, TableFrameSpec]:
    if dim_customer is None or dim_customer_history is None or fct_orders is None:
        return None, None, None
    return dim_customer, dim_customer_history, fct_orders
