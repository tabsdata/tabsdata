#
# Copyright 2026 Tabsdata Inc.
#

from __future__ import annotations

import collections
import contextlib
import io
import queue
import struct
import threading
from collections.abc import Buffer, Iterator
from typing import IO, TYPE_CHECKING, cast

if TYPE_CHECKING:
    import polars as pl
    import pyarrow.parquet as pq

PIPE_DEPTH = 4
PUT_WAIT_SECONDS = 0.1
# The most of the stream's end kept to read its Parquet footer from: footers
# run to kilobytes, and one too large to fit is simply not read.
TAIL_BYTES = 8 * 1024 * 1024


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
        self._tail: collections.deque[bytes] = collections.deque()
        self._tail_size = 0
        self._produced = False

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
            self._produced = True
            self._put(_END)

    def _keep(self, chunk: bytes) -> None:
        self._tail.append(chunk)
        self._tail_size += len(chunk)
        while self._tail_size - len(self._tail[0]) >= TAIL_BYTES:
            self._tail_size -= len(self._tail.popleft())

    def footer(self) -> pq.FileMetaData | None:
        """The Parquet footer of the stream, once it was produced whole: its
        last bytes, kept as they went by, so reading it costs nothing. ``None``
        when the stream did not end or its footer did not fit the kept tail."""
        import pyarrow.parquet as pq

        if not self._produced:
            return None
        tail = b"".join(self._tail)
        if len(tail) < 8 or tail[-4:] != b"PAR1":
            return None
        length = struct.unpack("<I", tail[-8:-4])[0]
        if length + 8 > len(tail):
            return None
        return pq.read_metadata(io.BytesIO(b"PAR1" + tail[-(length + 8) :]))

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
            self._pipe._keep(chunk)
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
