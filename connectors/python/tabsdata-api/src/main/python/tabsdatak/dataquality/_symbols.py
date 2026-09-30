#
# Copyright 2026 Tabsdata Inc.
#

"""Python stand-ins for the native ``TableType`` and ``FunctionType`` symbols.

The authoritative versions are pyo3/Rust enums (replicas of the ``td-objects``
types). These stand-ins mirror their variants and string values so the
data-quality code can classify derived tables, and the SDK can register
platform-owned functions, without the native binding; they are swapped for the
native symbols when the Rust extensions are ported.
"""

from __future__ import annotations

from enum import Enum


class TableType(Enum):
    Regular = "R"
    DQData = "DQD"
    DQSummary = "DQS"
    DQSummaryTrends = "DQST"

    def __str__(self) -> str:
        return self.value


class FunctionType(Enum):
    """What kind of function a registration records, alongside the
    ``decorator`` that records how it was declared.

    ``Regular`` is what the server assumes when a registration omits the
    field, so only the platform ever sends anything else.
    """

    Regular = "R"
    DQSummaryTrends = "DQST"

    def __str__(self) -> str:
        return self.value
