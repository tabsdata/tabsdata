"""In-memory buffer sentinel.

Accumulates ``CdcChange`` events per-table; on row / byte / time
threshold, flushes the staged batches to the disk sentinel."""

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

logger = logging.getLogger(__name__)


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
        self._buffers: dict[str, CdcBuffer] = {
            table: CdcBuffer() for table in tables
        }
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

    @property
    def offset(self) -> CdcOffset:
        return self._offset

    def reset_timer(self) -> None:
        self._last_flushed = time.time()

    def process_changes(self, changes: Iterable[CdcChange]) -> None:
        for change in changes:
            match change:
                case CdcDataChange():
                    self.process_data_change(change)
                case _:
                    # ``CdcSchemaChange`` etc. land here. The
                    # registry already absorbed them at the ingester
                    # level; nothing extra to do at the buffer.
                    raise NotImplementedError(
                        f"Event type {type(change).__name__} is not "
                        f"supported yet"
                    )

    def process_data_change(self, change: CdcDataChange) -> None:
        table = change.table

        if table not in self._buffers:
            # Untracked tables can appear when the source emits
            # changes for objects outside the connector's scope
            # (other schemas, system tables). Silently drop them.
            logger.debug("Untracked table %r; skipping change", table)
            return

        buffer = self._buffers[table]
        buffer.rows.append(change)
        self._total_rows += 1
        if self._max_bytes is not None:
            row_bytes = sys.getsizeof(change)
            buffer.bytes += row_bytes
            self._total_bytes += row_bytes

        # The change must carry an offset for the sentinel to
        # advance its commit point; ingesters that don't supply
        # one signal a bug.
        if self._offset is None:  # pragma: no cover - constructed with one
            raise RuntimeError(
                "BufferSentinel has no base offset to merge into"
            )
        if change.offset is None:
            raise RuntimeError(
                f"CDC change for table {change.table!r} has no offset"
            )
        self._offset.update(change.offset)

        rows_hit = self._max_rows is not None and self._total_rows >= self._max_rows
        bytes_hit = (
            self._max_bytes is not None and self._total_bytes >= self._max_bytes
        )
        time_hit = (time.time() - self._last_flushed) >= self._max_secs
        if self.has_data and (rows_hit or bytes_hit or time_hit):
            self.flush()

    def expire(self) -> None:
        """Time-based flush probe. The runner calls this once per
        poll cycle so a slow change stream still flushes within
        ``max_secs``."""
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
