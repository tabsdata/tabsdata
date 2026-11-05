#
# Copyright 2026 Tabsdata Inc.
#

from __future__ import annotations

import contextlib
import io
import queue
import threading
from collections.abc import Buffer, Iterator
from typing import IO, TYPE_CHECKING, cast

if TYPE_CHECKING:
    import polars as pl

PIPE_DEPTH = 4
PUT_WAIT_SECONDS = 0.1


class _End:
    pass


_END = _End()


class ParquetPipe(io.RawIOBase):
    def __init__(self, depth: int = PIPE_DEPTH) -> None:
        super().__init__()
        self._chunks: queue.Queue[bytes | BaseException | _End] = queue.Queue(depth)
        self._abandoned = threading.Event()
        self._pending = memoryview(b"")
        self._ended = False

    def readable(self) -> bool:
        return True

    def readinto(self, buffer: Buffer) -> int:
        target = memoryview(buffer).cast("B")
        filled = 0
        while filled < len(target):
            if not self._pending:
                if self._ended:
                    break
                item = self._chunks.get()
                if isinstance(item, _End):
                    self._ended = True
                    break
                if isinstance(item, BaseException):
                    self._ended = True
                    raise item
                self._pending = memoryview(item)
            size = min(len(target) - filled, len(self._pending))
            target[filled : filled + size] = self._pending[:size]
            self._pending = self._pending[size:]
            filled += size
        return filled

    def produce(self, frame: pl.LazyFrame) -> None:
        try:
            frame.sink_parquet(cast(IO[bytes], _Sink(self)))
        except BaseException as error:
            self._put(error)
        else:
            self._put(_END)

    def abandon(self) -> None:
        self._abandoned.set()
        while True:
            try:
                self._chunks.get_nowait()
            except queue.Empty:
                return

    def _put(self, item: bytes | BaseException | _End) -> None:
        while not self._abandoned.is_set():
            try:
                self._chunks.put(item, timeout=PUT_WAIT_SECONDS)
                return
            except queue.Full:
                continue
        if isinstance(item, bytes):
            raise BrokenPipeError("the reader of the parquet stream has gone")


class _Sink:
    def __init__(self, pipe: ParquetPipe) -> None:
        self._pipe = pipe

    def write(self, data: Buffer) -> int:
        chunk = bytes(data)
        if chunk:
            self._pipe._put(chunk)
        return len(chunk)

    def flush(self) -> None:
        pass


@contextlib.contextmanager
def piped_parquet(frame: pl.LazyFrame) -> Iterator[ParquetPipe]:
    pipe = ParquetPipe()
    producer = threading.Thread(target=pipe.produce, args=(frame,), daemon=True)
    producer.start()
    try:
        yield pipe
    finally:
        pipe.abandon()
        producer.join()
