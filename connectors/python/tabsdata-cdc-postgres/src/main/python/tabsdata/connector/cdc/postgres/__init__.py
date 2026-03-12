#
# Copyright 2026 Tabs Data Inc.
#

import importlib.metadata

from tabsdata.connector.cdc.postgres._connection import PostgresCdcConn
from tabsdata.connector.cdc.postgres._connector import PostgresCdcTrigger
from tabsdata.connector.cdc.postgres.typing import PostgresCdcStartFromSpec

__all__ = [
    # Connections
    "PostgresCdcConn",
    "PostgresCdcTrigger",
    # Specs
    "PostgresCdcStartFromSpec",
]


# noinspection PyBroadException
try:
    __version__ = importlib.metadata.version("tabsdata-cdc-postgres")
except Exception:
    __version__ = "unknown"

version = __version__
