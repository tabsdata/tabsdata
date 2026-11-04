#
# Copyright 2026 Tabsdata Inc.
#

from __future__ import annotations

import bz2
import gzip
import io
import lzma
import zlib

import lz4.frame
import pyarrow as pa
import pytest
import zstandard

# noinspection PyProtectedMember
from tabsdatak.conn.common.file._codecs import decompressed

# noinspection PyProtectedMember
from tabsdatak.conn.common.file._select import CODECS

pytestmark = [pytest.mark.unit]

PLAIN = b"".join(f"{value},row-{value}\n".encode() for value in range(50000))


def arrow(data: bytes, codec: str) -> bytes:
    sink = pa.BufferOutputStream()
    with pa.CompressedOutputStream(sink, codec) as stream:
        stream.write(data)
    return sink.getvalue().to_pybytes()


COMPRESSORS = {
    ".br": lambda data: arrow(data, "brotli"),
    ".bz2": bz2.compress,
    ".gz": gzip.compress,
    ".lz4": lz4.frame.compress,
    ".xz": lzma.compress,
    ".zlib": zlib.compress,
    ".zst": lambda data: zstandard.ZstdCompressor().compress(data),
    ".zstd": lambda data: zstandard.ZstdCompressor().compress(data),
}


class Source(io.BytesIO):
    pass


def read_all(extension: str, content: bytes, read_size: int) -> bytes:
    stream = decompressed(Source(content), extension)
    parts = []
    while part := stream.read(read_size):
        parts.append(part)
    stream.close()
    return b"".join(parts)


def test_every_codec_the_selection_knows_has_a_decoder():
    assert set(COMPRESSORS) == set(CODECS)


@pytest.mark.parametrize("read_size", [1, 7, 4096, 1 << 20])
@pytest.mark.parametrize("extension", sorted(COMPRESSORS))
def test_reads_of_any_size_return_the_plain_bytes(extension, read_size):
    plain = PLAIN if read_size > 1 else PLAIN[:20000]
    content = COMPRESSORS[extension](plain)
    assert read_all(extension, content, read_size) == plain


@pytest.mark.parametrize("extension", sorted(COMPRESSORS))
def test_a_read_to_the_end_returns_everything(extension):
    stream = decompressed(Source(COMPRESSORS[extension](PLAIN)), extension)
    assert stream.read() == PLAIN


@pytest.mark.parametrize("extension", [".bz2", ".gz", ".lz4", ".xz", ".zst"])
def test_concatenated_streams_decode_whole(extension):
    compress = COMPRESSORS[extension]
    content = compress(b"first part\n") + compress(b"second part\n")
    assert read_all(extension, content, 1 << 20) == b"first part\nsecond part\n"


@pytest.mark.parametrize("extension", sorted(COMPRESSORS))
def test_closing_the_stream_closes_its_source(extension):
    source = Source(COMPRESSORS[extension](PLAIN))
    stream = decompressed(source, extension)
    stream.read(10)
    stream.close()
    assert source.closed


def test_a_zlib_read_holds_no_more_than_it_was_asked_for():
    stream = decompressed(Source(zlib.compress(bytes(50 << 20))), ".zlib")
    assert len(stream.read(10)) == 10
    assert stream.pending


def test_an_unknown_extension_is_refused():
    with pytest.raises(ValueError, match="'.zip' is unknown"):
        decompressed(Source(b""), ".zip")
