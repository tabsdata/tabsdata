#
# Copyright 2026 Tabsdata Inc.
#

from __future__ import annotations

import io
import json
import math
import os
import subprocess
import sys
import time
import uuid
from collections.abc import Iterator
from dataclasses import dataclass
from pathlib import Path

import fastavro
import pytest
from obstore.exceptions import GenericError
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
# A leak grows with the file, three times over between these sizes; the work in
# flight at the peak varies between landings, more so the more threads hold it.
SMALLER = GIBIBYTE
LARGER = 4 * GIBIBYTE
GROWTH_BASE = 128 * MEBIBYTE
GROWTH_PER_THREAD = 12 * MEBIBYTE
ROUNDS = 2
# Every upload and landing of a test shares one budget, which ends before the
# test's own timeout, so a slow step fails naming itself.
BUDGET_SECONDS = 1200
CLEANUP_SECONDS = 120
UPLOAD_ATTEMPTS = 3
# An upload request can stall for good: the emulator stops receiving with the
# client's bytes all sent (Docker Desktop's port forwarding loses them). One
# request (a part, or the join of a large upload's parts) is given this long,
# so a stalled one leaves the budget another attempt.
REQUEST_SECONDS = 300
# Every emulator group starts uploading in the session's first second, with the
# rest of the suite starting beside them: many parts in flight at once can run
# the machine out of socket buffers (ENOBUFS), so each upload sends a few at a
# time, and a failed attempt waits for the burst to pass.
UPLOAD_CONCURRENCY = 4
RETRY_PAUSE_SECONDS = 30
LAND = Path(__file__).parent / "children" / "land.py"

ROWS_PER_BLOCK = 20000

AVRO_SCHEMA = {
    "type": "record",
    "name": "r",
    "fields": [
        {"name": "id", "type": "long"},
        {"name": "name", "type": "string"},
        {"name": "amount", "type": "double"},
        {"name": "note", "type": "string"},
    ],
}


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


def avro_header_and_block() -> tuple[bytes, bytes]:
    # The writer puts the header, with the file's sync marker, on construction,
    # and a flush closes a block with that marker: the block repeats into a
    # valid file.
    sink = io.BytesIO()
    writer = fastavro.write.Writer(sink, AVRO_SCHEMA, codec="null")
    header = sink.tell()
    for value in range(ROWS_PER_BLOCK):
        writer.write({
            "id": value,
            "name": f"customer-{value % 977}",
            "amount": value * 0.25,
            "note": f"free text {value}",
        })
    writer.flush()
    encoded = sink.getvalue()
    return encoded[:header], encoded[header:]


def repeated(path: Path, header: bytes, block: bytes, size: int) -> int:
    blocks = size // len(block)
    with open(path, "wb") as sink:
        sink.write(header)
        for _ in range(blocks):
            sink.write(block)
    return blocks * ROWS_PER_BLOCK


def avro_file(path: Path, size: int) -> int:
    return repeated(path, *avro_header_and_block(), size)


def csv_file(path: Path, size: int) -> int:
    return repeated(path, b"id,name,amount,note\n", text_block(), size)


def log_file(path: Path, size: int) -> int:
    return repeated(path, b"", text_block(), size)


def ndjson_file(path: Path, size: int) -> int:
    return repeated(path, b"", ndjson_block(), size)


FORMATS = {
    "AVRO": (".avro", avro_file),
    "CSV": (".csv", csv_file),
    "JSON": (".ndjson", ndjson_file),
    "LOG": (".log", log_file),
}


@dataclass(frozen=True)
class Source:
    path: Path
    rows: int


class Sources:
    """The files to land, written once per worker and kept across rounds and
    the emulators the worker runs: only one format's, a few GiB, at a time."""

    def __init__(self, folder: Path) -> None:
        self.folder = folder
        self.files: dict[tuple[str, int], Source] = {}

    def file(self, file_format: str, size: int) -> Source:
        key = (file_format, size)
        if key not in self.files:
            for other in [other for other in self.files if other[0] != file_format]:
                self.files.pop(other).path.unlink(missing_ok=True)
            extension, write = FORMATS[file_format]
            path = self.folder / f"in-{size}{extension}"
            self.files[key] = Source(path, write(path, size))
        return self.files[key]

    def clear(self) -> None:
        for source in self.files.values():
            source.path.unlink(missing_ok=True)
        self.files.clear()


@pytest.fixture(scope="session")
def sources(tmp_path_factory) -> Iterator[Sources]:
    files = Sources(tmp_path_factory.mktemp("readers"))
    yield files
    files.clear()


class Budget:
    def __init__(self, seconds: int) -> None:
        self.seconds = seconds
        self.deadline = time.monotonic() + seconds

    def left(self, step: str) -> float:
        seconds = self.deadline - time.monotonic()
        if seconds <= 0:
            pytest.fail(
                f"No time is left for {step}: the test's {self.seconds}s ran out"
            )
        return seconds


@dataclass(frozen=True)
class Staged:
    root: str
    name: str
    rows: int


def uploaded(domain, options: dict, source: Source, budget: Budget) -> Staged:
    for attempt in range(UPLOAD_ATTEMPTS):
        step = f"uploading {source.path.name} (attempt {attempt + 1})"
        timeout = f"{math.ceil(min(REQUEST_SECONDS, budget.left(step)))}s"
        root = f"{domain.uri.rstrip('/')}/readers-{uuid.uuid4().hex}"
        store = FileStore(root, {**options, "timeout": timeout})
        try:
            with open(source.path, "rb") as content:
                store.store().put(
                    source.path.name, content, max_concurrency=UPLOAD_CONCURRENCY
                )
        except GenericError:
            if attempt == UPLOAD_ATTEMPTS - 1:
                raise
            time.sleep(RETRY_PAUSE_SECONDS)
        else:
            return Staged(root, source.path.name, source.rows)
    raise AssertionError("unreachable")


def landed(
    staged: Staged, options: dict, file_format: str, target: Path, budget: Budget
) -> tuple[int, int]:
    step = f"landing {staged.name}"
    try:
        child = subprocess.run(
            [
                sys.executable,
                str(LAND),
                staged.root,
                json.dumps(options),
                staged.name,
                file_format,
                str(target),
            ],
            capture_output=True,
            encoding="utf-8",
            env={**os.environ, "PYTHONPATH": os.pathsep.join(sys.path)},
            timeout=budget.left(step),
            check=True,
        )
    except subprocess.TimeoutExpired:
        pytest.fail(f"{step} did not end within the test's {budget.seconds}s")
    finally:
        target.unlink(missing_ok=True)
    report = json.loads(child.stdout.strip().splitlines()[-1])
    assert report["rows"] == staged.rows
    return report["peak"], report["threads"]


@emulated
@pytest.mark.timeout(BUDGET_SECONDS + CLEANUP_SECONDS)
@pytest.mark.parametrize(
    "emulator",
    [
        pytest.param(emulator, marks=pytest.mark.xdist_group(f"readers-{emulator}"))
        for emulator in EMULATORS
    ],
)
@pytest.mark.parametrize("file_format", sorted(FORMATS))
def test_a_large_file_lands_in_bounded_memory_that_does_not_grow_with_it(
    domain_azurebloblocal: AzureBlobLocalDomain,
    domain_gcslocal: GcsLocalDomain,
    domain_s3local: S3LocalDomain,
    sources: Sources,
    tmp_path,
    emulator,
    file_format,
):
    domain = locals()[f"domain_{emulator}"]
    options = dict(domain.storage_options)
    budget = Budget(BUDGET_SECONDS)
    peaks: dict[int, list[int]] = {SMALLER: [], LARGER: []}
    staged: dict[int, Staged] = {}
    try:
        for size in peaks:
            staged[size] = uploaded(
                domain, options, sources.file(file_format, size), budget
            )
        # A burst of work in flight only ever raises a landing's peak, while a
        # leak raises every landing of the larger file: a failing comparison
        # lands both sizes again and judges the lower peak of each.
        for _ in range(ROUNDS):
            for size, landings in peaks.items():
                target = tmp_path / f"landed-{size}.parquet"
                peak, threads = landed(
                    staged[size], options, file_format, target, budget
                )
                landings.append(peak)
            smaller, larger = min(peaks[SMALLER]), min(peaks[LARGER])
            peak_limit = PEAK_BASE + PEAK_PER_THREAD * threads
            growth_limit = GROWTH_BASE + GROWTH_PER_THREAD * threads
            if max(smaller, larger) < peak_limit and larger - smaller < growth_limit:
                break
    finally:
        for file in staged.values():
            FileStore(file.root, options).delete([file.name])
    report = (
        f"peaks {smaller / MEBIBYTE:.0f} and {larger / MEBIBYTE:.0f} MiB"
        f" ({len(peaks[SMALLER])} landing(s) each) with {threads} threads,"
        f" limits {peak_limit / MEBIBYTE:.0f} MiB peak,"
        f" {growth_limit / MEBIBYTE:.0f} MiB growth"
    )
    assert max(smaller, larger) < peak_limit, report
    assert larger - smaller < growth_limit, report
