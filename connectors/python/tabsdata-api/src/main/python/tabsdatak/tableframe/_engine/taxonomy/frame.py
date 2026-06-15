#
# Copyright 2026 Tabs Data Inc.
#

from __future__ import annotations

from enum import Enum


class CreationMode(Enum):
    # Create every system column from scratch (drop any existing first).
    RAW = "raw"
    # Apply each column's inception policy: keep PROPAGATE, regenerate REGENERATE.
    SYS = "sys"
    # Create only the system columns that are missing; keep existing ones.
    TAB = "tab"
    # Regenerate the pristine columns (fresh per-function view); keep the rest.
    MUT = "mut"
