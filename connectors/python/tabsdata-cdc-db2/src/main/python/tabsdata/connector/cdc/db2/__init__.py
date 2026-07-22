#
# Copyright 2026 Tabsdata Inc.
#

import importlib.metadata

from tabsdata.connector.cdc.db2._connection import Db2CdcConn
from tabsdata.connector.cdc.db2._connector import Db2CdcTrigger
from tabsdata.connector.cdc.db2.typing import Db2CdcStartFromSpec

__all__ = [
    # Connections
    "Db2CdcConn",
    "Db2CdcTrigger",
    # Specs
    "Db2CdcStartFromSpec",
]


# noinspection PyBroadException
try:
    __version__ = importlib.metadata.version("tabsdata-cdc-db2")
except Exception:
    __version__ = "unknown"

version = __version__
