#
# Copyright 2026 Tabsdata Inc.
#

from __future__ import annotations

from collections.abc import Callable, Iterator

import polars as pl
import pyarrow as pa
import pyarrow.parquet as pq

from tabsdatak.common._storage.failures import recorded
from tabsdatak.conn.common.file._read._blocks import Seekable, close
from tabsdatak.spi import LazyFrameInput


class Parquet:
    def read(self, open_: Callable[[], Seekable]) -> LazyFrameInput:
        source = open_()
        try:
            parquet = pq.ParquetFile(source)
        except BaseException:
            close(source)
            raise

        def batches() -> Iterator[pa.RecordBatch]:
            try:
                yield from parquet.iter_batches()
            finally:
                parquet.close()
                close(source)

        reader = pa.RecordBatchReader.from_batches(
            parquet.schema_arrow, recorded(batches())
        )
        return LazyFrameInput(pl.scan_arrow_c_stream(reader))
