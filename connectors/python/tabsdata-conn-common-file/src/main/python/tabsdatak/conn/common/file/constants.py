#
# Copyright 2026 Tabsdata Inc.
#

from __future__ import annotations

from tabsdatak.conn.common.constants import TD_CONNECTOR_COLUMN_PREFIX

FILE_META_PREFIX: str = TD_CONNECTOR_COLUMN_PREFIX + "file."
"""Name-prefix (`"@td.file."`) shared by the metadata every file source
reports for the files it imports."""

FILE_META_PATH: str = FILE_META_PREFIX + "path"
"""Metadata column (`"@td.file.path"`) added to every table a file source
publishes, holding the location the row was read from -- a local path for
`LocalFileSrc`, the object URI (`s3://`, `gs://`, `az://`) for a cloud
source. One imported file per path, so a slot that globbed several files
reports the specific file each row came from. An `EXCEL` source keeps the
path's `#<n>` worksheet fragment on the value, so two slots reading
different sheets of one workbook stay distinguishable."""
