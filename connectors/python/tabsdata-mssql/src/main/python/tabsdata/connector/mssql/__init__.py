#
# Copyright 2025 Tabsdata Inc.
#

import importlib.metadata

from tabsdata.connector.mssql._connector import MSSQLDestination, MSSQLSource
from tabsdata.connector.mssql.connection.mssql_connection import MSSQLConn
from tabsdata.connector.mssql.constants import MSSQLDestWriterSpec, MSSQLSrcLoaderSpec
from tabsdata.connector.mssql.input.mssql_input import MSSQLSrc
from tabsdata.connector.mssql.output.mssql_output import MSSQLDest

__all__ = [
    "MSSQLDest",
    "MSSQLDestination",
    "MSSQLDestWriterSpec",
    "MSSQLSource",
    "MSSQLSrc",
    "MSSQLSrcLoaderSpec",
    "MSSQLConn",
]

# noinspection PyBroadException
try:
    __version__ = importlib.metadata.version("tabsdata-mssql")
except Exception:
    __version__ = "unknown"

version = __version__
