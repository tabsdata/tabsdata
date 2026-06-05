#
#  Copyright 2026 Tabs Data Inc.
#

from __future__ import annotations

import logging
import time
from pathlib import Path

import pyarrow as pa
import pyarrow.parquet as pq

from tabsdatak.conn.common.sql.cdc._model import CdcDataChange, TrackedTable
from tabsdatak.conn.common.sql.cdc._sentinel import CdcBuffer, row_to_record
from tabsdatak.conn.common.sql.cdc._utils import encode_id, sanitize_file_name
from tabsdatak.conn.common.sql.cdc.typing import CdcFormat

logger = logging.getLogger(__name__)
logger.setLevel(logging.DEBUG)


class DiskSentinel:
    def __init__(
        self,
        landing: Path,
        max_rows: int,
        max_bytes: int | None,
        max_secs: int,
        tables: dict[str, TrackedTable],
        cdc_format: CdcFormat,
    ):
        self._landing = landing
        self._max_rows = max_rows
        self._max_bytes = max_bytes
        self._max_secs = max_secs
        self._tables = tables
        self._cdc_format = cdc_format
        self._files: dict[str, list[Path]] = {table: [] for table in tables}
        self._total_rows: int = 0
        self._total_bytes: int = 0
        self._last_flushed: float = time.time()

    @property
    def is_empty(self) -> bool:
        return self._total_rows == 0

    @property
    def has_data(self) -> bool:
        return self._total_rows > 0

    def reset_timer(self) -> None:
        self._last_flushed = time.time()

    def process_buffers(
        self,
        buffers: dict[str, CdcBuffer],
    ) -> None:
        for table, buffer in buffers.items():
            if len(buffer.rows) == 0:
                continue
            self.process_buffer(table, buffer.rows)

    def process_buffer(
        self,
        table: str,
        rows: list[CdcDataChange],
    ) -> None:
        start = 0
        current_hash = rows[0].schema.hash
        for i in range(1, len(rows)):
            row_hash = rows[i].schema.hash
            if row_hash != current_hash:
                self.process_section(table, rows, start, i)
                start = i
                current_hash = row_hash
        self.process_section(table, rows, start, len(rows))

    def process_section(
        self,
        table: str,
        rows: list[CdcDataChange],
        start: int,
        stop: int,
    ) -> None:
        pa_schema = rows[start].schema.pa_schema
        records = [
            row_to_record(
                rows[i],
                self._cdc_format,
            )
            for i in range(start, stop)
        ]
        # noinspection PyArgumentList
        pa_table = pa.Table.from_pylist(
            mapping=records,
            schema=pa_schema,
        )

        _, file_suffix = encode_id()
        file_prefix = sanitize_file_name(table)
        file_name = f"{file_prefix}_{file_suffix}.parquet"
        file_path = self._landing / file_name
        pq.write_table(pa_table, file_path)

        file_bytes = file_path.stat().st_size
        file_rows = stop - start

        self._files[table].append(file_path)
        self._total_rows += file_rows
        self._total_bytes += file_bytes

        logger.debug(
            f"Buffer flush: writen {table} → {file_name} "
            f"({file_rows} rows & {file_bytes} bytes)"
        )

    def should_flush(
        self,
        force: bool = False,
    ) -> bool:
        """Whether the accumulated on-disk files are due to be staged.

        Triggers on ``force`` (shutdown flush) or any of the row / byte /
        time thresholds. Staging itself is driven by the plugin, which
        pulls :meth:`build_manifest` and publishes it through
        :meth:`~tabsdatak.spi.StreamSrcPluginCtx.stage`.
        """
        rows_hit = self._max_rows is not None and self._total_rows >= self._max_rows
        bytes_hit = self._max_bytes is not None and self._total_bytes >= self._max_bytes
        time_hit = (time.time() - self._last_flushed) >= self._max_secs
        return self.has_data and (force or rows_hit or bytes_hit or time_hit)

    def build_manifest(self) -> list[list[Path]]:
        """Positional per-output-table file lists for
        :meth:`~tabsdatak.spi.StreamSrcPluginCtx.stage`.

        One entry per tracked table, in tracked-table (== output-slot)
        order; entry ``i`` holds the parquet files accumulated for table
        ``i`` since the last :meth:`clear`, or an empty list when that
        table produced no files this batch.
        """
        return [list(self._files.get(table_key, [])) for table_key in self._tables]

    def clear(self) -> None:
        """Drop the staged file bookkeeping and reset the counters /
        timer. Called by the plugin once a batch has been committed."""
        for files in self._files.values():
            files.clear()
        self._total_rows = 0
        self._total_bytes = 0
        self._last_flushed = time.time()
