#
# Copyright 2026 Tabs Data Inc.
#

from __future__ import annotations

from dataclasses import dataclass

from tabsdata._io.cloud_staging.common.statuses import BulkLoadStatus


@dataclass(frozen=True)
class BulkLoadResult:
    status: BulkLoadStatus
    job_handle: str | None = None
    error: str | None = None
    rows_loaded: int | None = None
