#
# Copyright 2026 Tabs Data Inc.
#

from __future__ import annotations

from enum import Enum


class BulkLoadStatus(Enum):
    COMPLETED = "completed"
    FAILED = "failed"
    PENDING = "pending"
