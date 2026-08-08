#
# Copyright 2026 Tabsdata Inc.
#

from __future__ import annotations

from tabsdatak.tableframe._engine.constant.column.prefixes import (
    TD_CONNECTOR_SYSTEM_COLUMN_PREFIX,
)

TD_CONNECTOR_COLUMN_PREFIX: str = TD_CONNECTOR_SYSTEM_COLUMN_PREFIX
"""Reserved name-prefix (`"@td."`) for the metadata a connector reports
alongside the data it reads.

Every metadata entry a source plugin attaches to a file it produces is
named with this prefix, so the metadata columns added to the published
table never collide with a column of the source data itself."""

SRC_META_DROP_KEY: str = "tabsdata.src_metadata.drop"
"""`src_cfg` key (`"tabsdata.src_metadata.drop"`) every source that
reports metadata accepts.

Set it to `True` and `read_in` returns its files with no metadata at
all, so the published table carries only the source's own columns.
Defaults to `False`."""
