from __future__ import annotations

from dataclasses import dataclass

# noinspection PyProtectedMember
from tabsdatak.conn.clickhouse._statuses import BulkLoadStatus


@dataclass(frozen=True)
class BulkLoadResult:
    status: BulkLoadStatus
    job_handle: str | None = None
    error: str | None = None
    rows_loaded: int | None = None
