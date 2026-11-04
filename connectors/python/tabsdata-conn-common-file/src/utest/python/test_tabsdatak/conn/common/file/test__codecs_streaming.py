#
# Copyright 2026 Tabsdata Inc.
#

from __future__ import annotations

import bz2
import gzip
import json
import lzma
import os
import subprocess
import sys
import uuid
import zlib
from pathlib import Path

import pyarrow as pa
import pytest
from tabsdatax.itest.fixture.backends.s3local import S3LocalDomain, s3local_domain

# noinspection PyProtectedMember
from tabsdatak.conn.common.file._select import CODECS

# noinspection PyProtectedMember
from tabsdatak.conn.common.file._store import FileStore

pytestmark = [pytest.mark.unit, pytest.mark.slow]

MEBIBYTE = 1 << 20
PLAIN_SIZE = 1024 * MEBIBYTE
PEAK_LIMIT = 512 * MEBIBYTE
BLOCK = b"".join(
    f"{value},customer-{value % 977},{value * 0.25},free text {value}\n".encode()
    for value in range(20000)
)
CHILD_SECONDS = 600
DRAIN = Path(__file__).parent / "children" / "drain.py"


def blocks():
    for _ in range(PLAIN_SIZE // len(BLOCK)):
        yield BLOCK


def plain_total() -> int:
    return (PLAIN_SIZE // len(BLOCK)) * len(BLOCK)


def arrow_writer(codec: str):
    def write(path: Path) -> None:
        with pa.CompressedOutputStream(str(path), codec) as sink:
            for block in blocks():
                sink.write(block)

    return write


def stdlib_writer(opener):
    def write(path: Path) -> None:
        with opener(path) as sink:
            for block in blocks():
                sink.write(block)

    return write


def zlib_writer(path: Path) -> None:
    compressor = zlib.compressobj(1)
    with open(path, "wb") as sink:
        for block in blocks():
            sink.write(compressor.compress(block))
        sink.write(compressor.flush())


WRITERS = {
    ".br": arrow_writer("brotli"),
    ".bz2": stdlib_writer(lambda path: bz2.open(path, "wb", compresslevel=1)),
    ".gz": stdlib_writer(lambda path: gzip.open(path, "wb", compresslevel=1)),
    ".lz4": arrow_writer("lz4"),
    ".xz": stdlib_writer(lambda path: lzma.open(path, "wb", preset=0)),
    ".zlib": zlib_writer,
    ".zst": arrow_writer("zstd"),
    ".zstd": arrow_writer("zstd"),
}


def test_every_codec_has_a_streaming_case():
    assert set(WRITERS) == set(CODECS)


@s3local_domain("s3local")
@pytest.mark.parametrize("extension", sorted(WRITERS))
def test_a_large_compressed_object_decompresses_in_bounded_memory(
    domain_s3local: S3LocalDomain, tmp_path, extension
):
    local = tmp_path / f"plain{extension}"
    WRITERS[extension](local)
    root = f"{domain_s3local.uri.rstrip('/')}/codecs-{uuid.uuid4().hex}"
    options = dict(domain_s3local.storage_options)
    store = FileStore(root, options)
    name = local.name
    with open(local, "rb") as content:
        store.store().put(name, content)
    local.unlink()
    try:
        drained = subprocess.run(
            [sys.executable, str(DRAIN), root, json.dumps(options), name, extension],
            capture_output=True,
            encoding="utf-8",
            env={**os.environ, "PYTHONPATH": os.pathsep.join(sys.path)},
            timeout=CHILD_SECONDS,
            check=True,
        )
    finally:
        store.delete([name])
    report = json.loads(drained.stdout.strip().splitlines()[-1])
    assert report["total"] == plain_total()
    assert report["peak"] < PEAK_LIMIT, f"peak {report['peak'] / MEBIBYTE:.0f} MiB"
