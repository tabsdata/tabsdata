#
# Copyright 2025 Tabs Data Inc.
#

import importlib.metadata

from tabsdata_mssql._connector import MSSQLDestination, MSSQLSource

__all__ = ["MSSQLDestination", "MSSQLSource"]

# noinspection PyBroadException
try:
    __version__ = importlib.metadata.version("tabsdata_mssql")
except Exception:
    __version__ = "unknown"

version = __version__
