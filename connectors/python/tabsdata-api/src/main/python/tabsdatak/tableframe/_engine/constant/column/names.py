#
# Copyright 2026 Tabsdata Inc.
#

from __future__ import annotations

TD_IDENTIFIER_SYSTEM_COLUMN_NAME = "$td.id"
TD_OFFSET_SYSTEM_COLUMN_NAME = "$td.offset"
TD_ROW_INDEX_SYSTEM_COLUMN_NAME = "$td.row"
TD_PROVENANCE_SYSTEM_COLUMN_NAME = "$td.src"

TD_EXECUTION_SYSTEM_COLUMN_NAME = "$td.ver.execution"
TD_TRANSACTION_SYSTEM_COLUMN_NAME = "$td.ver.transaction"
TD_VERSION_SYSTEM_COLUMN_NAME = "$td.ver.version"
TD_TIMESTAMP_SYSTEM_COLUMN_NAME = "$td.ver.timestamp"

TD_SERVER_TIMESTAMP_SYSTEM_COLUMN_NAME = "$td.opt.tst.srv"
TD_USER_TIMESTAMP_SYSTEM_COLUMN_NAME = "$td.opt.tst.usr"
TD_VALUE_TIMESTAMP_SYSTEM_COLUMN_NAME = "$td.opt.tst.ts"
TD_FUNCTION_TIMESTAMP_SYSTEM_COLUMN_NAME = "$td.opt.tst.fn"

# The origin-file connector columns are retired: connector columns are now
# injected per input file from source-plugin metadata rather than declared as
# a fixed set. The former names are kept for reference:
#
# TD_FILE_PATH_SYSTEM_COLUMN_NAME = "@td.origin.file.path"
# TD_FILE_LINE_SYSTEM_COLUMN_NAME = "@td.origin.file.line"

TD_INDEX_SYSTEM_COLUMN_NAME = "$td._index"
TD_ITEM_SYSTEM_COLUMN_NAME = "$td._item"
TD_MINIMUM_SYSTEM_COLUMN_NAME = "$td._min"
TD_MAXIMUM_SYSTEM_COLUMN_NAME = "$td._max"
