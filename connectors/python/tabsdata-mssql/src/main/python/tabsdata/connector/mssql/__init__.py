#
# Copyright 2025 Tabs Data Inc.
#

import importlib.metadata

from tabsdata.connector.mssql._connector import MSSQLDestination, MSSQLSource
from tabsdata.connector.mssql.connection.mssql_connection import MSSQLConn
from tabsdata.connector.mssql.constants import MSSQLLoaderSpec
from tabsdata.connector.mssql.input.mssql_input import MSSQLSrc

__all__ = [
    "MSSQLDestination",
    "MSSQLSource",
    "MSSQLSrc",
    "MSSQLLoaderSpec",
    "MSSQLConn",
]

# noinspection PyBroadException
try:
    __version__ = importlib.metadata.version("tabsdata-mssql")
except Exception:
    __version__ = "unknown"

version = __version__
