#
# Copyright 2026 Tabsdata Inc.
#

from __future__ import annotations

from enum import Enum

# Per *path* while ``format_cfg`` is per source: held as a list parallel to
# ``paths``, narrowed per slot, then taken back out by
# ``src_format_to_transporter``. A carrier, never a user-facing option --
# ``validate_format_cfg`` rejects it on a user-supplied ``format_cfg``.
EXCEL_SHEET_KEY = "sheet"

# The copybook an EBCDIC source reads each path against.
EBCDIC_COPYBOOK_KEYS = ("copybook",)

# The EBCDIC code pages the binary's `Codepage` enum deserializes, spelled
# exactly as it spells them. A value outside this set fails the run when the
# request reaches the binary, so it is refused at declaration time instead.
EBCDIC_CODEPAGES = ("cp037", "cp273", "cp500", "cp1140")


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
