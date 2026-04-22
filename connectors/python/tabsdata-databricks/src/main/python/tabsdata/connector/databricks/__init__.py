#
# Copyright 2025 Tabs Data Inc.
#

import importlib.metadata

from tabsdata.connector.databricks._connection import DatabricksConn
from tabsdata.connector.databricks._connector import (
    DatabricksDestination,
    DatabricksSource,
)
from tabsdata.connector.databricks.input.databricks_input import DatabricksSrc
from tabsdata.connector.databricks.output.databricks_output import DatabricksDest

__all__ = [
    "DatabricksConn",
    "DatabricksDest",
    "DatabricksDestination",
    "DatabricksSource",
    "DatabricksSrc",
]

# noinspection PyBroadException
try:
    __version__ = importlib.metadata.version("tabsdata-databricks")
except Exception:
    __version__ = "unknown"

version = __version__
