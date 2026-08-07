#
# Copyright 2026 Tabsdata Inc.
#

from __future__ import annotations

from tabsdatak.conn.common.constants import TD_CONNECTOR_COLUMN_PREFIX

SALESFORCE_META_PREFIX: str = TD_CONNECTOR_COLUMN_PREFIX + "salesforce."
"""Name-prefix (`"@td.salesforce."`) shared by the metadata the Salesforce
sources report for the files they produce."""

SALESFORCE_META_SOQL: str = SALESFORCE_META_PREFIX + "soql"
"""Metadata column (`"@td.salesforce.soql"`) added to every table
`SalesforceSrc` publishes, holding the SOQL query that produced the row as
it was sent to Salesforce -- any `$lastModified` token already substituted
with the value reported in `SALESFORCE_META_LAST_MODIFIED`."""

SALESFORCE_META_REPORT_ID: str = SALESFORCE_META_PREFIX + "reportId"
"""Metadata column (`"@td.salesforce.reportId"`) added to every table
`SalesforceReportSrc` publishes, holding the Salesforce id of the report
that produced the row -- the resolved id even when the source named the
report by name."""

SALESFORCE_META_LAST_MODIFIED: str = SALESFORCE_META_PREFIX + "last_modified"
"""Metadata column (`"@td.salesforce.last_modified"`) added to every table
either Salesforce source publishes, holding the source's
`initial_last_modified` cutoff formatted the way Salesforce expects it --
empty when the source declares no cutoff."""
