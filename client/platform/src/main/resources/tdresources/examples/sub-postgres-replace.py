#
# Copyright 2026 Tabsdata Inc.
#

"""
title: Mirror a table into Postgres with replace
kind: subscriber
connectors: [postgres-sql-out]
tags: [sql, replace, sync]
summary: >-
  PostgresDest has no merge or upsert mode; if_table_exists is append or
  replace. To mirror the current state of a table, subscribe to its latest
  version and write it with replace, which truncates and reloads the target
  in one transaction. A subscriber with one input runs whenever that table
  gets a new version.
"""

from tabsdatak.api import TableFrameSpec, subscriber
from tabsdatak.conn.postgres import PostgresDest


@subscriber(
    destination=PostgresDest(
        tables=["public.customers_current"],
        if_table_exists="replace",
    ),
    input_tables=["crm/customers"],
)
def sub_postgres_replace(customers: TableFrameSpec) -> TableFrameSpec:
    return customers
