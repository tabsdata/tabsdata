#
# Copyright 2026 Tabsdata Inc.
#

import importlib.metadata

from tabsdata.connector.cdc.mysql._connection import MySQLCdcConn
from tabsdata.connector.cdc.mysql._connector import MySQLCdcTrigger
from tabsdata.connector.cdc.mysql.typing import MySQLCdcStartFromSpec

__all__ = [
    # Connections
    "MySQLCdcConn",
    "MySQLCdcTrigger",
    # Specs
    "MySQLCdcStartFromSpec",
]


# noinspection PyBroadException
try:
    __version__ = importlib.metadata.version("tabsdata-cdc-mysql")
except Exception:
    __version__ = "unknown"

version = __version__
