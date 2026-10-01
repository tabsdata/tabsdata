#
# Copyright 2026 Tabsdata Inc.
#

from __future__ import annotations

import time
from collections.abc import Callable

import pyarrow as pa

from tabsdatak.conn.common._stream._ledger import FileLedger


class ChunkSentinel:
    def __init__(
        self,
        *,
        ledger: FileLedger,
        max_rows: int | None,
        max_bytes: int | None,
    ):
        self._ledger = ledger
        self._max_rows = max_rows
        self._max_bytes = max_bytes
        self._chunks: dict[str, list[pa.Table]] = {}
        self._rows: dict[str, int] = {}
        self._bytes: dict[str, int] = {}
        self._keys: dict[str, str] = {}

    def add(self, slot: str, key: str, table: pa.Table) -> None:
        if self._keys.get(slot) not in (None, key):
            self.flush(slot)
        self._keys[slot] = key
        self._chunks.setdefault(slot, []).append(table)
        self._rows[slot] = self._rows.get(slot, 0) + table.num_rows
        self._bytes[slot] = self._bytes.get(slot, 0) + table.nbytes
        if self.due(slot):
            self.flush(slot)

    def due(self, slot: str) -> bool:
        rows_hit = self._max_rows is not None and self._rows[slot] >= self._max_rows
        bytes_hit = self._max_bytes is not None and self._bytes[slot] >= self._max_bytes
        return rows_hit or bytes_hit

    def flush(self, slot: str | None = None) -> None:
        slots = [slot] if slot is not None else list(self._chunks)
        for name in slots:
            chunks = self._chunks.get(name)
            if not chunks:
                continue
            self._ledger.write(name, pa.concat_tables(chunks))
            self._chunks[name] = []
            self._rows[name] = 0
            self._bytes[name] = 0
            self._keys.pop(name, None)


class DiskSentinel:
    def __init__(
        self,
        *,
        ledger: FileLedger,
        max_rows: int | None,
        max_bytes: int | None,
        max_secs: float,
        clock: Callable[[], float] = time.time,
    ):
        self._ledger = ledger
        self._max_rows = max_rows
        self._max_bytes = max_bytes
        self._max_secs = max_secs
        self._clock = clock
        self._last_flushed: float = clock()

    def reset_timer(self) -> None:
        self._last_flushed = self._clock()

    def due(self, force: bool = False) -> bool:
        rows_hit = self._max_rows is not None and self._ledger.rows >= self._max_rows
        bytes_hit = (
            self._max_bytes is not None and self._ledger.bytes >= self._max_bytes
        )
        time_hit = (self._clock() - self._last_flushed) >= self._max_secs
        return self._ledger.has_data and (force or rows_hit or bytes_hit or time_hit)
