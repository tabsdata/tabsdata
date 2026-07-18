#
# Copyright 2026 Tabsdata Inc.
#

"""Python stand-in for the native ``TableType`` symbol.

The authoritative ``TableType`` is a pyo3/Rust enum (a replica of the
``td-objects`` table type). This stand-in mirrors its variants and string
values so the data-quality code can classify derived tables without the native
binding; it is swapped for the native symbol when the Rust extensions are ported.
"""

from __future__ import annotations

from enum import Enum


class TableType(Enum):
    Regular = "R"
    DQData = "DQD"
    DQSummary = "DQS"

    def __str__(self) -> str:
        return self.value
