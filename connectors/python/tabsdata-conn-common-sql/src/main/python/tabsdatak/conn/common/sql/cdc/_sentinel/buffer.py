#
#  Copyright 2026 Tabsdata Inc.
#

from __future__ import annotations

import logging
import sys
import time
from collections.abc import Iterable

from tabsdatak.conn.common.sql.cdc._model import (
    CdcChange,
    CdcDataChange,
    CdcOffset,
    TrackedTable,
)
from tabsdatak.conn.common.sql.cdc._sentinel import CdcBuffer
from tabsdatak.conn.common.sql.cdc._sentinel.disk import DiskSentinel
from tabsdatak.conn.common.sql.cdc._utils import TRACE
from tabsdatak.conn.common.sql.cdc.error import SqlCdcErrorCode

logger = logging.getLogger(__name__)
logger.setLevel(logging.DEBUG)


class BufferSentinel:
    def __init__(
        self,
        disk_sentinel: DiskSentinel,
        max_rows: int,
        max_bytes: int | None,
        max_secs: int,
        tables: dict[str, TrackedTable],
        offset: CdcOffset,
    ):
        self._disk_sentinel = disk_sentinel
        self._max_rows = max_rows
        self._max_bytes = max_bytes
        self._max_secs = max_secs
        self._buffers: dict[str, CdcBuffer] = {table: CdcBuffer() for table in tables}
        self._total_rows: int = 0
        self._total_bytes: int = 0
        self._last_flushed: float = time.time()
        self._offset: CdcOffset = offset

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

    @property
    def offset(self) -> CdcOffset:
        return self._offset

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
            row_bytes = sys.getsizeof(change)
            buffer.bytes += row_bytes
            self._total_bytes += row_bytes

        if self._offset is None:
            raise SqlCdcErrorCode.CDC_10.exception()
        if change.offset is None:
            raise SqlCdcErrorCode.CDC_11.exception(table=change.table)
        self._offset.update(change.offset)

        rows_hit = self._max_rows is not None and self._total_rows >= self._max_rows
        bytes_hit = self._max_bytes is not None and self._total_bytes >= self._max_bytes
        time_hit = (time.time() - self._last_flushed) >= self._max_secs
        if self.has_data and (rows_hit or bytes_hit or time_hit):
            self.flush()

    def expire(self) -> None:
        if self.has_data and (time.time() - self._last_flushed) >= self._max_secs:
            self.flush()

    def flush(self) -> None:
        self._disk_sentinel.process_buffers(self._buffers)
        for buffer in self._buffers.values():
            buffer.rows.clear()
            buffer.bytes = 0
        self._total_rows = 0
        self._total_bytes = 0
        self._last_flushed = time.time()
