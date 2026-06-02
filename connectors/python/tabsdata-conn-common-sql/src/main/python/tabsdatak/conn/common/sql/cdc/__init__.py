#
# Copyright 2026 Tabs Data Inc.
#

import importlib.metadata

from tabsdatak.conn.common.sql.cdc._connection import CdcConn
from tabsdatak.conn.common.sql.cdc._runner import CdcPlugin, CdcRunConfig
from tabsdatak.conn.common.sql.cdc.typing import (
    CdcFormat,
    CdcColumnsFormatSpec,
    QualifiedTableSpec,
    QualifiedTablesSpec,
    RelativePositionSpec,
    TimestampPosition,
)

__all__ = [
    # Connections
    "CdcConn",
    # Runner
    "CdcPlugin",
    "CdcRunConfig",
    # Specs
    "QualifiedTableSpec",
    "QualifiedTablesSpec",
    "RelativePositionSpec",
    "TimestampPosition",
    "CdcFormat",
    "CdcColumnsFormatSpec",
]

# noinspection PyBroadException
try:
    __version__ = importlib.metadata.version("tabsdata-conn-common-sql")
except Exception:
    __version__ = "unknown"

version = __version__
