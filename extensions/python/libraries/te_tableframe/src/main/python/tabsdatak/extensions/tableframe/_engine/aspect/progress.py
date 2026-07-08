#
# Copyright 2026 Tabs Data Inc.
#

"""Execution-progress aspect: report streaming-sink progress.

The observer inserts a side-effect node into the streaming plan that tallies the
rows and in-memory bytes flowing through each batch, and logs progress at a fixed
wall-clock cadence: rows written, elapsed time, in-memory bytes processed, and
rows-per-second and bytes-per-second throughput. The total row count is not known
until the query runs, so no completion percentage is reported.

The bytes figure is each batch's in-memory size (``estimated_size``); it is not
the on-disk file size, which differs by format and compression.
"""

from __future__ import annotations

import logging
import time
from typing import TYPE_CHECKING

if TYPE_CHECKING:
    import polars as pl_

logger = logging.getLogger(__name__)

REPORT_INTERVAL_SECONDS = 2.0


def observe(lazyframe: "pl_.LazyFrame", method: str, path) -> "pl_.LazyFrame":
    rows = 0
    memory_bytes = 0
    started_at = time.perf_counter()
    last_report_at = started_at

    def report(batch: "pl_.DataFrame") -> "pl_.DataFrame":
        nonlocal rows, memory_bytes, last_report_at
        rows += batch.height
        memory_bytes += batch.estimated_size()
        now = time.perf_counter()
        if now - last_report_at >= REPORT_INTERVAL_SECONDS:
            last_report_at = now
            _log(method, path, rows, memory_bytes, now - started_at)
        return batch

    return lazyframe.map_batches(report, streamable=True)


def _log(method, path, rows, memory_bytes, elapsed) -> None:
    rows_per_second = rows / elapsed if elapsed else 0.0
    bytes_per_second = memory_bytes / elapsed if elapsed else 0.0
    logger.info(
        "%s %s: rows=%d elapsed=%.1fs %.0f rows/s memory=%s (%s/s)",
        method,
        path,
        rows,
        elapsed,
        rows_per_second,
        _human_bytes(memory_bytes),
        _human_bytes(bytes_per_second),
    )


def _human_bytes(count: float) -> str:
    size = float(count)
    for unit in ("B", "KB", "MB", "GB", "TB"):
        if size < 1024 or unit == "TB":
            return f"{size:.1f}{unit}"
        size /= 1024
    return f"{size:.1f}TB"
