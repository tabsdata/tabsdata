#
# Copyright 2025 Tabs Data Inc.
#

import importlib.metadata

from tabsdata_mssql._connector import MSSQLDestination, MSSQLSource
from tabsdata_mssql.connection.mssql_connection import MSSQLConn
from tabsdata_mssql.constants import MSSQLLoaderSpec
from tabsdata_mssql.input.mssql_input import MSSQLSrc

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
