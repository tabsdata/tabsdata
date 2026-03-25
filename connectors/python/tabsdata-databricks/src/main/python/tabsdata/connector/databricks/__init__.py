#
# Copyright 2025 Tabs Data Inc.
#

import importlib.metadata

from tabsdata.connector.databricks._connector import (
    DatabricksDestination,
    DatabricksSource,
)

__all__ = [
    "DatabricksDestination",
    "DatabricksSource",
]

# noinspection PyBroadException
try:
    __version__ = importlib.metadata.version("tabsdata-databricks")
except Exception:
    __version__ = "unknown"

version = __version__
