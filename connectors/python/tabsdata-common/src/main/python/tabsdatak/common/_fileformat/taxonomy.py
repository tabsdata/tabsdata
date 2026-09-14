#
# Copyright 2026 Tabsdata Inc.
#

from __future__ import annotations

from enum import Enum

# The worksheet number rides into the reader inside ``format_cfg``'s EXCEL
# entry. It is per *path* while ``format_cfg`` is per source, so it is held
# there as a list parallel to ``paths`` and narrowed to that slot's own
# number on the way into each request; ``src_format_to_transporter`` then
# takes it back out. A carrier, never a user-facing option --
# ``validate_format_cfg`` still rejects it on a user-supplied ``format_cfg``.
EXCEL_SHEET_KEY = "sheet"


class SrcFileFormat(Enum):
    AUTO = "AUTO"
    CSV = "CSV"
    JSON = "JSON"
    AVRO = "AVRO"
    PARQUET = "PARQUET"
    LOG = "LOG"
    EXCEL = "EXCEL"


class DestFileFormat(Enum):
    AUTO = "AUTO"
    CSV = "CSV"
    JSON = "JSON"
    AVRO = "AVRO"
    PARQUET = "PARQUET"
