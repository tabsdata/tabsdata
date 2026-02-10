#
# Copyright 2025 Tabs Data Inc.
#

import importlib.metadata

from tabsdata_mongodb._connector import MongoDBDestination

__all__ = ["MongoDBDestination"]

# noinspection PyBroadException
try:
    __version__ = importlib.metadata.version("tabsdata_mongodb")
except Exception:
    __version__ = "unknown"

version = __version__
