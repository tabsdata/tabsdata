#
# Copyright 2026 Tabsdata Inc.
#

from __future__ import annotations

import itertools
import json
import os
import subprocess
import sys
import uuid
from pathlib import Path

import fastavro
import pytest
from tabsdatax.itest.fixture.backends.azurebloblocal import AzureBlobLocalDomain
from tabsdatax.itest.fixture.backends.gcslocal import GcsLocalDomain
from tabsdatax.itest.fixture.backends.s3local import S3LocalDomain
from tabsdatax.itest.fixture.emulators import EMULATORS, emulated

# noinspection PyProtectedMember
from tabsdatak.conn.common.file._store import FileStore

pytestmark = [pytest.mark.unit, pytest.mark.slow]

MEBIBYTE = 1 << 20
GIBIBYTE = 1 << 30
# A reader holds a share of each polars thread's work in flight, so its peak
# follows the size of the thread pool (the runner's cores), never the file's.
PEAK_BASE = 1280 * MEBIBYTE
PEAK_PER_THREAD = 48 * MEBIBYTE
GROWTH_LIMIT = 256 * MEBIBYTE
CHILD_SECONDS = 900
LAND = Path(__file__).parent / "children" / "land.py"

ROWS_PER_BLOCK = 20000


def text_block() -> bytes:
    return b"".join(
        f"{value},customer-{value % 977},{value * 0.25},free text {value}\n".encode()
        for value in range(ROWS_PER_BLOCK)
    )


def ndjson_block() -> bytes:
    return b"".join(
        json.dumps({
            "id": value,
            "name": f"c-{value % 977}",
            "amount": value * 0.25,
        }).encode()
        + b"\n"
        for value in range(ROWS_PER_BLOCK)
    )


def repeated(path: Path, header: bytes, block: bytes, size: int) -> int:
    blocks = size // len(block)
    with open(path, "wb") as sink:
        sink.write(header)
        for _ in range(blocks):
            sink.write(block)
    return blocks * ROWS_PER_BLOCK


def csv_file(path: Path, size: int) -> int:
    return repeated(path, b"id,name,amount,note\n", text_block(), size)


def log_file(path: Path, size: int) -> int:
    return repeated(path, b"", text_block(), size)


def ndjson_file(path: Path, size: int) -> int:
    return repeated(path, b"", ndjson_block(), size)


def avro_file(path: Path, size: int) -> int:
    schema = {
        "type": "record",
        "name": "r",
        "fields": [
            {"name": "id", "type": "long"},
            {"name": "name", "type": "string"},
            {"name": "amount", "type": "double"},
            {"name": "note", "type": "string"},
        ],
    }
    records = [
        {
            "id": value,
            "name": f"customer-{value % 977}",
            "amount": value * 0.25,
            "note": f"free text {value}",
        }
        for value in range(ROWS_PER_BLOCK)
    ]
    rows = 0
    with open(path, "wb") as sink:
        writer = fastavro.write.Writer(sink, schema, codec="null")
        for record in itertools.cycle(records):
            writer.write(record)
            rows += 1
            if rows % ROWS_PER_BLOCK == 0:
                writer.flush()
                if sink.tell() >= size:
                    break
        writer.flush()
    return rows


FORMATS = {
    "AVRO": (".avro", avro_file),
    "CSV": (".csv", csv_file),
    "JSON": (".ndjson", ndjson_file),
    "LOG": (".log", log_file),
}


def landed(domain, tmp_path: Path, file_format: str, size: int):
    extension, write = FORMATS[file_format]
    local = tmp_path / f"in-{size}{extension}"
    rows = write(local, size)
    root = f"{domain.uri.rstrip('/')}/readers-{uuid.uuid4().hex}"
    options = dict(domain.storage_options)
    store = FileStore(root, options)
    with open(local, "rb") as content:
        store.store().put(local.name, content)
    local.unlink()
    target = tmp_path / f"landed-{size}.parquet"
    try:
        child = subprocess.run(
            [
                sys.executable,
                str(LAND),
                root,
                json.dumps(options),
                local.name,
                file_format,
                str(target),
            ],
            capture_output=True,
            encoding="utf-8",
            env={**os.environ, "PYTHONPATH": os.pathsep.join(sys.path)},
            timeout=CHILD_SECONDS,
            check=True,
        )
    finally:
        store.delete([local.name])
        target.unlink(missing_ok=True)
    report = json.loads(child.stdout.strip().splitlines()[-1])
    assert report["rows"] == rows
    return report["peak"], report["threads"]


@emulated
@pytest.mark.parametrize("emulator", EMULATORS)
@pytest.mark.parametrize("file_format", sorted(FORMATS))
def test_a_large_file_lands_in_bounded_memory_that_does_not_grow_with_it(
    domain_azurebloblocal: AzureBlobLocalDomain,
    domain_gcslocal: GcsLocalDomain,
    domain_s3local: S3LocalDomain,
    tmp_path,
    emulator,
    file_format,
):
    domain = locals()[f"domain_{emulator}"]
    smaller, threads = landed(domain, tmp_path, file_format, GIBIBYTE)
    larger, _ = landed(domain, tmp_path, file_format, 2 * GIBIBYTE)
    limit = PEAK_BASE + PEAK_PER_THREAD * threads
    report = (
        f"peaks {smaller / MEBIBYTE:.0f} and {larger / MEBIBYTE:.0f} MiB"
        f" with {threads} threads, limit {limit / MEBIBYTE:.0f} MiB"
    )
    assert max(smaller, larger) < limit, report
    assert larger - smaller < GROWTH_LIMIT, report
