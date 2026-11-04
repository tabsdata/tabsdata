#
# Copyright 2026 Tabsdata Inc.
#

from __future__ import annotations

from collections.abc import Callable

import polars as pl

from tabsdata.expansions.foundation.readers import avro  # noqa: TID251
from tabsdatak.conn.common.file._read._blocks import Readable, close
from tabsdatak.spi import LazyFrameInput

BATCH_ROWS = 65536


class Avro:
    def __init__(self, *, batch_rows: int = BATCH_ROWS) -> None:
        self.batch_rows = batch_rows

    def read(self, open_: Callable[[], Readable]) -> LazyFrameInput:
        stream = open_()
        try:
            batches = avro(stream, self.batch_rows)
        except BaseException:
            close(stream)
            raise
        return LazyFrameInput(pl.scan_arrow_c_stream(batches))
