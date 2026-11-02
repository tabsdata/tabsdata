#
# Copyright 2026 Tabsdata Inc.
#

"""
title: Publish a SQL Server query
kind: publisher
connectors: [mssql-sql-in]
tags: [sql, query, bind]
summary: >-
  Publishes the result of a filtered SQL Server query; the filter value is a
  typed bind seeded by initial_values rather than string interpolation.
"""

from tabsdatak.api import TableFrameSpec, publisher
from tabsdatak.conn.mssql import MSSQLSrc


@publisher(
    source=MSSQLSrc(
        queries=[
            "SELECT id, name, region, status FROM dbo.accounts "
            "WHERE status = :status ORDER BY id"
        ],
        initial_values={"status": "active"},
    ),
    output_tables=["active_accounts"],
)
def pub_mssql_query(accounts: TableFrameSpec) -> TableFrameSpec:
    return accounts
