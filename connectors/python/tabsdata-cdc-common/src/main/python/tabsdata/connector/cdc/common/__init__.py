#
# Copyright 2026 Tabs Data Inc.
#

import importlib.metadata

from tabsdata.connector.cdc.common._connection import CdcConn
from tabsdata.connector.cdc.common._connector import CdcTrigger
from tabsdata.connector.cdc.common.typing import (
    CdcFormatSpec,
    CdcValuesFormatSpec,
    QualifiedTableSpec,
    QualifiedTablesSpec,
    RelativePositionSpec,
    TimestampPositionSpec,
)

__all__ = [
    # Connections
    "CdcConn",
    "CdcTrigger",
    # Specs
    "QualifiedTableSpec",
    "QualifiedTablesSpec",
    "RelativePositionSpec",
    "TimestampPositionSpec",
    "CdcFormatSpec",
    "CdcValuesFormatSpec",
]

# noinspection PyBroadException
try:
    __version__ = importlib.metadata.version("tabsdata-cdc-common")
except Exception:
    __version__ = "unknown"

version = __version__
