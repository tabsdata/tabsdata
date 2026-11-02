#
# Copyright 2026 Tabsdata Inc.
#

"""
title: Publish a full Postgres table
kind: publisher
connectors: [postgres-sql-in]
tags: [sql, full-load, snapshot]
summary: >-
  Reads the whole customers table on every run, publishing a new full
  snapshot version each time. One output table per query, in order.
"""

from tabsdatak.api import TableFrameSpec, publisher
from tabsdatak.conn.postgres import PostgresSrc


@publisher(
    source=PostgresSrc(
        queries=["SELECT id, name, email, country, created_at FROM public.customers"],
    ),
    output_tables=["customers"],
)
def pub_postgres_full(customers: TableFrameSpec) -> TableFrameSpec:
    return customers
