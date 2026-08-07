#
# Copyright 2026 Tabsdata Inc.
#

from __future__ import annotations

from tabsdatak.conn.common.constants import TD_CONNECTOR_COLUMN_PREFIX

SQL_META_PREFIX: str = TD_CONNECTOR_COLUMN_PREFIX + "sql."
"""Name-prefix (`"@td.sql."`) shared by the metadata every SQL source
reports for the parquet files it produces."""

SQL_META_QUERY: str = SQL_META_PREFIX + "query"
"""Metadata column (`"@td.sql.query"`) added to every table a SQL source
publishes, holding the text of the query that produced the row."""

SQL_META_INITIAL_VALUES: str = SQL_META_PREFIX + "initial_values"
"""Metadata column (`"@td.sql.initial_values"`) added to every table a SQL
source publishes, holding the JSON serialization of the bind values the
query ran with -- an empty JSON object when the query has no binds."""
