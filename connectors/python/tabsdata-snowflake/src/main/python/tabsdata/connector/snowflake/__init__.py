#
# Copyright 2025 Tabsdata Inc.
#

import importlib.metadata

from tabsdata.connector.snowflake._connection import SnowflakeConn
from tabsdata.connector.snowflake._connector import SnowflakeDestination
from tabsdata.connector.snowflake.output.snowflake_output import SnowflakeDest

__all__ = ["SnowflakeConn", "SnowflakeDest", "SnowflakeDestination"]

# noinspection PyBroadException
try:
    __version__ = importlib.metadata.version("tabsdata-snowflake")
except Exception:
    __version__ = "unknown"

version = __version__
