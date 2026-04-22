#
# Copyright 2025 Tabs Data Inc.
#

import importlib.metadata

from tabsdata.connector.mongodb._connection import MongoDBConn
from tabsdata.connector.mongodb._connector import MongoDBDestination
from tabsdata.connector.mongodb.output.mongodb_output import MongoDBDest

__all__ = ["MongoDBConn", "MongoDBDest", "MongoDBDestination"]

# noinspection PyBroadException
try:
    __version__ = importlib.metadata.version("tabsdata-mongodb")
except Exception:
    __version__ = "unknown"

version = __version__
