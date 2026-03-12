#
#  Copyright 2026 Tabs Data Inc.
#

from __future__ import annotations

import logging
import time
from collections.abc import Iterable

from tabsdata._tabsserver.function.global_utils import TRACE
from tabsdata.connector.cdc.common._model import (
    CdcChange,
    CdcDataChange,
    TrackedTable,
)
from tabsdata.connector.cdc.common._sentinel import CdcBuffer, estimate_row_bytes
from tabsdata.connector.cdc.common._sentinel._disk import DiskSentinel

logger = logging.getLogger(__name__)
logger.setLevel(logging.DEBUG)


class BufferSentinel:
    def __init__(
        self,
        disk_sentinel: DiskSentinel,
        max_rows: int,
        max_bytes: int | None,
        max_seconds: float,
        tables: dict[str, TrackedTable],
    ):
        self._disk_sentinel = disk_sentinel
        self._max_rows = max_rows
        self._max_bytes = max_bytes
        self._max_seconds = max_seconds
        self._buffers: dict[str, CdcBuffer] = {table: CdcBuffer() for table in tables}
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

    def process_changes(self, changes: Iterable[CdcChange]) -> None:
        for change in changes:
            match change:
                case CdcDataChange():
                    self.process_data_change(change)
                case _:
                    raise NotImplementedError(
                        f"Event type {type(change).__name__} is not supported yet"
                    )

    def process_data_change(self, change: CdcDataChange) -> None:
        table = change.table

        if table not in self._buffers:
            logger.log(
                TRACE, f"Received change for untracked table {table!r}; skipping it."
            )
            return

        buffer = self._buffers[table]
        buffer.rows.append(change)
        self._total_rows += 1
        if self._max_bytes is not None:
            row_bytes = estimate_row_bytes(change)
            buffer.bytes += row_bytes
            self._total_bytes += row_bytes

        rows_hit = self._max_rows is not None and self._total_rows >= self._max_rows
        bytes_hit = self._max_bytes is not None and self._total_bytes >= self._max_bytes
        time_hit = (time.time() - self._last_flushed) >= self._max_seconds
        if self.has_data and (rows_hit or bytes_hit or time_hit):
            self.flush()

    def flush(self) -> None:
        self._disk_sentinel.process_buffers(self._buffers)
        for buffer in self._buffers.values():
            buffer.rows.clear()
            buffer.bytes = 0
        self._total_rows = 0
        self._total_bytes = 0
        self._last_flushed = time.time()
