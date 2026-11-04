#
# Copyright 2026 Tabsdata Inc.
#

"""LOG: one row per line, columns `file` (the source name) and `message`,
served as they are."""

from __future__ import annotations

from collections.abc import Callable, Iterator

import polars as pl
import pyarrow as pa

from tabsdatak.common._storage.failures import recorded
from tabsdatak.conn.common.file._read._blocks import (
    BLOCK_SIZE,
    EOL,
    Readable,
    blocks,
    close,
    lines,
)
from tabsdatak.spi import LazyFrameInput

FILE_COLUMN = "file"
MESSAGE_COLUMN = "message"


class Log:
    def __init__(self, name: str, *, block_size: int = BLOCK_SIZE) -> None:
        self.name = name
        self.block_size = block_size

    def read(self, open_: Callable[[], Readable]) -> LazyFrameInput:
        return LazyFrameInput(self.data(open_))

    def data(self, open_: Callable[[], Readable]) -> pl.LazyFrame:
        def batches() -> Iterator[pa.RecordBatch]:
            stream = open_()
            try:
                for block in blocks(stream, size=self.block_size, eol=EOL):
                    frame = pl.select(
                        pl.lit(self.name, pl.String).alias(FILE_COLUMN),
                        lines(block).alias(MESSAGE_COLUMN),
                    )
                    yield from frame.to_arrow().to_batches()
            finally:
                close(stream)

        arrow = pa.schema({
            FILE_COLUMN: pa.large_string(),
            MESSAGE_COLUMN: pa.large_string(),
        })
        reader = pa.RecordBatchReader.from_batches(arrow, recorded(batches()))
        return pl.scan_arrow_c_stream(reader)
