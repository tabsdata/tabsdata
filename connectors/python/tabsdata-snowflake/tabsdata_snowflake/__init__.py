#
# Copyright 2025 Tabs Data Inc.
#

import importlib.metadata

from tabsdata_snowflake._connector import SnowflakeDestination

__all__ = ["SnowflakeDestination"]

# noinspection PyBroadException
try:
    __version__ = importlib.metadata.version("tabsdata_snowflake")
except Exception:
    __version__ = "unknown"

version = __version__
