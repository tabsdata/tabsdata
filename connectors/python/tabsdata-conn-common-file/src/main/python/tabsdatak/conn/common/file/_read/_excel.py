#
# Copyright 2026 Tabsdata Inc.
#

from __future__ import annotations

from collections.abc import Callable, Mapping
from typing import Any

import polars as pl

from tabsdata.expansions.foundation.readers import excel  # noqa: TID251
from tabsdatak.common._fileformat.taxonomy import EXCEL_SHEET_KEY
from tabsdatak.conn.common.file._read._blocks import Seekable, close
from tabsdatak.spi import LazyFrameInput

BATCH_ROWS = 65536
FIRST_SHEET = 1
HAS_HEADER = "has_header"


class Excel:
    def __init__(
        self,
        options: Mapping[str, Any] | None = None,
        *,
        batch_rows: int = BATCH_ROWS,
    ) -> None:
        options = options or {}
        sheet = options.get(EXCEL_SHEET_KEY)
        self.sheet = FIRST_SHEET if sheet is None else int(sheet)
        self.has_header = bool(options.get(HAS_HEADER, True))
        self.batch_rows = batch_rows

    def read(self, open_: Callable[[], Seekable]) -> LazyFrameInput:
        source = open_()
        try:
            batches = excel(source, self.sheet, self.has_header, self.batch_rows)
        except BaseException:
            close(source)
            raise
        return LazyFrameInput(pl.scan_arrow_c_stream(batches))
