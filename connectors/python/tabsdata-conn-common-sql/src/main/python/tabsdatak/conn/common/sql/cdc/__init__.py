#
# Copyright 2026 Tabs Data Inc.
#

import importlib.metadata

from tabsdatak.conn.common.sql.cdc._connection import CdcConn
from tabsdatak.conn.common.sql.cdc._runner import CdcPlugin, CdcRunConfig
from tabsdatak.conn.common.sql.cdc._stage_plugin import (
    CDC_STAGE_SRC,
    CdcStageSrc,
    CdcStageSrcPlugin,
)
from tabsdatak.conn.common.sql.cdc.typing import (
    CdcColumnsFormatSpec,
    CdcFormat,
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
    # Stream-stage publisher
    "CDC_STAGE_SRC",
    "CdcStageSrc",
    "CdcStageSrcPlugin",
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
