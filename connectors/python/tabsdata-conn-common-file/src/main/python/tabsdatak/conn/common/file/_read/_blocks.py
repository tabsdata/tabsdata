#
# Copyright 2026 Tabsdata Inc.
#

from __future__ import annotations

from collections.abc import Iterator
from typing import Protocol

import polars as pl

from tabsdata.expansions.foundation.readers import CsvCutter  # noqa: TID251

BLOCK_SIZE = 16 << 20
EOL = b"\n"
LINE_COLUMN = "line"


class Readable(Protocol):
    def read(self, size: int = ..., /) -> bytes: ...


class Seekable(Readable, Protocol):
    def seek(self, offset: int, whence: int = ..., /) -> int: ...


def close(stream: Readable) -> None:
    closer = getattr(stream, "close", None)
    if closer is not None:
        closer()


def lines(block: bytes) -> pl.Series:
    return pl.scan_lines(block).collect().get_column(LINE_COLUMN)


def blocks(
    stream: Readable,
    *,
    size: int = BLOCK_SIZE,
    eol: bytes = EOL,
) -> Iterator[bytes]:
    rest = b""
    while True:
        data = stream.read(size)
        buffer = rest + data
        if not data:
            if buffer:
                yield buffer
            return
        cut = buffer.rfind(eol)
        if cut < 0:
            rest = buffer
            continue
        cut += len(eol)
        yield buffer[:cut]
        rest = buffer[cut:]


def records(
    stream: Readable,
    cutter: CsvCutter,
    *,
    size: int = BLOCK_SIZE,
) -> Iterator[tuple[bytes, int]]:
    rest = b""
    while True:
        data = stream.read(size)
        buffer = rest + data
        last = not data
        if not buffer:
            return
        end, widest = cutter.cut(buffer, last)
        if end:
            yield buffer[:end], widest
        rest = buffer[end:]
        if last:
            return
