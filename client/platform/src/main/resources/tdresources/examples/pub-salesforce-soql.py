#
# Copyright 2026 Tabsdata Inc.
#

"""
title: Publish Salesforce records with SOQL
kind: publisher
connectors: [salesforce-sosql-in]
tags: [saas, soql, crm]
summary: >-
  Runs one SOQL query against the org. $lastModified in the query is replaced
  with initial_last_modified, a fixed floor that keeps the org from scanning
  its whole history.
"""

from datetime import datetime, timezone

from tabsdatak.api import TableFrameSpec, publisher
from tabsdatak.conn.salesforce import SalesforceSrc


@publisher(
    source=SalesforceSrc(
        queries=[
            "SELECT Id, AccountId, Name, StageName, Amount, CloseDate, "
            "LastModifiedDate FROM Opportunity "
            "WHERE LastModifiedDate > $lastModified ORDER BY LastModifiedDate"
        ],
        initial_last_modified=datetime(2025, 1, 1, tzinfo=timezone.utc),
    ),
    output_tables=["opportunities"],
)
def pub_salesforce_soql(opportunities: TableFrameSpec) -> TableFrameSpec:
    return opportunities
