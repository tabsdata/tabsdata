#
# Copyright 2026 Tabs Data Inc.
#

from __future__ import annotations

from enum import Enum, auto


class Aspect(Enum):
    # Validate and protect the system columns ($td.*) across operations.
    SYSTEM_COLUMNS = auto()
    # Report progress while writing a frame to a file.
    WRITE_PROGRESS = auto()
