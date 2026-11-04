#
# Copyright 2026 Tabsdata Inc.
#

from __future__ import annotations

import lzma
import zlib

import pyarrow as pa

from tabsdatak.conn.common.file._read._blocks import Readable, close

CHUNK = 4 * 1024 * 1024

ARROW_CODECS = {
    ".br": "brotli",
    ".bz2": "bz2",
    ".gz": "gzip",
    ".lz4": "lz4",
    ".zst": "zstd",
    ".zstd": "zstd",
}


class ArrowCodecStream:
    def __init__(self, source: Readable, codec: str) -> None:
        self.source = source
        self.stream = pa.CompressedInputStream(pa.PythonFile(source, mode="r"), codec)

    def read(self, size: int = -1) -> bytes:
        return self.stream.read(None if size < 0 else size)

    def close(self) -> None:
        self.stream.close()
        close(self.source)


class XzCodecStream:
    def __init__(self, source: Readable) -> None:
        self.source = source
        self.stream = lzma.LZMAFile(source)

    def read(self, size: int = -1) -> bytes:
        return self.stream.read(size)

    def close(self) -> None:
        self.stream.close()
        close(self.source)


class ZlibCodecStream:
    def __init__(self, source: Readable, *, chunk: int = CHUNK) -> None:
        self.source = source
        self.chunk = chunk
        self.decompressor = zlib.decompressobj()
        self.pending = b""
        self.ended = False

    def read(self, size: int = -1) -> bytes:
        wanted = None if size < 0 else size
        parts = []
        while not self.ended and (wanted is None or wanted > 0):
            if not self.pending:
                self.pending = self.source.read(self.chunk)
                if not self.pending:
                    parts.append(self.decompressor.flush())
                    self.ended = True
                    break
            limit = self.chunk if wanted is None else min(wanted, self.chunk)
            part = self.decompressor.decompress(self.pending, limit)
            self.pending = self.decompressor.unconsumed_tail
            self.ended = self.decompressor.eof
            parts.append(part)
            if wanted is not None:
                wanted -= len(part)
        return b"".join(parts)

    def close(self) -> None:
        close(self.source)


def decompressed(source: Readable, extension: str) -> Readable:
    match extension:
        case ".xz":
            return XzCodecStream(source)
        case ".zlib":
            return ZlibCodecStream(source)
        case codec if codec in ARROW_CODECS:
            return ArrowCodecStream(source, ARROW_CODECS[codec])
        case _:
            raise ValueError(f"The compression extension '{extension}' is unknown")
