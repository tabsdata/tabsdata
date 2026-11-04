#
# Copyright 2026 Tabsdata Inc.
#

from __future__ import annotations

from collections.abc import Iterator
from typing import Protocol

import polars as pl

BLOCK_SIZE = 16 << 20
EOL = b"\n"
LINE_COLUMN = "line"


class Readable(Protocol):
    def read(self, size: int = ..., /) -> bytes: ...


def close(stream: Readable) -> None:
    closer = getattr(stream, "close", None)
    if closer is not None:
        closer()


def lines(block: bytes) -> pl.Series:
    return pl.scan_lines(block).collect().get_column(LINE_COLUMN)


# ToDo: replace by a CSV state machine (csv-core) once it is wired in. Until
# then a newline is a record end when the quote characters before it pair up:
# - exact for fields quoted whole, with quotes escaped by doubling;
# - wrong for a quote inside an unquoted field, or in a comment line.
def blocks(
    stream: Readable,
    *,
    size: int = BLOCK_SIZE,
    eol: bytes = b"\n",
    quote: bytes | None = None,
) -> Iterator[bytes]:
    rest = b""
    while True:
        data = stream.read(size)
        buffer = rest + data
        if not data:
            if buffer:
                yield buffer
            return
        cut = record_end(buffer, eol, quote)
        if cut is None:
            rest = buffer
            continue
        yield buffer[:cut]
        rest = buffer[cut:]


def record_end(buffer: bytes, eol: bytes, quote: bytes | None) -> int | None:
    end = len(buffer)
    while True:
        end = buffer.rfind(eol, 0, end)
        if end < 0:
            return None
        if quote is None or buffer.count(quote, 0, end) % 2 == 0:
            return end + len(eol)
