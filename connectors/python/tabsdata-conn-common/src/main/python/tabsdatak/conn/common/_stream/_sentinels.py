#
# Copyright 2026 Tabsdata Inc.
#

from __future__ import annotations

import time
from collections.abc import Callable, Iterable

import pyarrow as pa

from tabsdatak.conn.common._stream._encoder import StreamEncoder
from tabsdatak.conn.common._stream._ledger import FileLedger
from tabsdatak.conn.common._stream._offset import StreamOffset
from tabsdatak.conn.common._stream.constants import MANIFEST_FLOOR_SECS
from tabsdatak.conn.common.error import ConnErrorCode


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
        buffer: BufferSentinel | None = None,
        max_rows: int | None,
        max_bytes: int | None,
        max_secs: float,
        floor_secs: float = MANIFEST_FLOOR_SECS,
        clock: Callable[[], float] = time.time,
    ):
        self._ledger = ledger
        self._buffer = buffer
        self._max_rows = max_rows
        self._max_bytes = max_bytes
        self._max_secs = max_secs
        self._floor_secs = min(floor_secs, max_secs)
        self._clock = clock
        self._last_flushed: float = clock()

    @property
    def rows(self) -> int:
        buffered = self._buffer.rows if self._buffer is not None else 0
        return self._ledger.rows + buffered

    @property
    def bytes(self) -> int:
        buffered = self._buffer.bytes if self._buffer is not None else 0
        return self._ledger.bytes + buffered

    @property
    def has_data(self) -> bool:
        buffered = self._buffer is not None and self._buffer.has_data
        return self._ledger.has_data or buffered

    def reset_timer(self) -> None:
        self._last_flushed = self._clock()

    def due(self, force: bool = False) -> bool:
        if not self.has_data:
            return False
        if force:
            return True
        elapsed = self._clock() - self._last_flushed
        if elapsed < self._floor_secs:
            return False
        rows_hit = self._max_rows is not None and self.rows >= self._max_rows
        bytes_hit = self._max_bytes is not None and self.bytes >= self._max_bytes
        return elapsed >= self._max_secs or rows_hit or bytes_hit


class BufferSentinel[R, O: StreamOffset]:
    def __init__(
        self,
        *,
        ledger: FileLedger,
        encoder: StreamEncoder[R, O],
        slots: Iterable[str],
        max_rows: int | None,
        max_bytes: int | None,
        max_secs: float,
        offset: O,
        measure_bytes: bool = False,
        clock: Callable[[], float] = time.time,
    ):
        self._ledger = ledger
        self._encoder = encoder
        self._max_rows = max_rows
        self._max_bytes = max_bytes
        self._measure_bytes = measure_bytes or max_bytes is not None
        self._max_secs = max_secs
        self._clock = clock
        self._records: dict[str, list[R]] = {slot: [] for slot in slots}
        self._rows: int = 0
        self._bytes: int = 0
        self._last_flushed: float = clock()
        self._offset: O = offset
        self._pending: list[O] = []

    @property
    def rows(self) -> int:
        return self._rows

    @property
    def bytes(self) -> int:
        return self._bytes

    @property
    def has_data(self) -> bool:
        return self._rows > 0

    @property
    def offset(self) -> O:
        return self._offset

    def reset_timer(self) -> None:
        self._last_flushed = self._clock()

    def add_all(self, records: Iterable[R]) -> int:
        count = 0
        for record in records:
            self.add(record)
            count += 1
        return count

    def add(self, record: R) -> None:
        slot = self._encoder.slot(record)
        if slot is None:
            return
        self._records[slot].append(record)
        self._rows += 1
        if self._measure_bytes:
            self._bytes += self._encoder.size(record)
        if self._offset is None:
            raise ConnErrorCode.CONN_33.exception()
        self._pending.append(self._encoder.offset(record))
        if self.due():
            self.flush()

    def due(self) -> bool:
        rows_hit = self._max_rows is not None and self._rows >= self._max_rows
        bytes_hit = self._max_bytes is not None and self._bytes >= self._max_bytes
        time_hit = (self._clock() - self._last_flushed) >= self._max_secs
        return self.has_data and (rows_hit or bytes_hit or time_hit)

    def flush(self) -> None:
        for slot, records in self._records.items():
            if not records:
                continue
            for table in self._encoder.encode(slot, records):
                self._ledger.write(slot, table)
        for offset in self._pending:
            self._offset.update(offset)
        self._pending.clear()
        for records in self._records.values():
            records.clear()
        self._rows = 0
        self._bytes = 0
        self._last_flushed = self._clock()
