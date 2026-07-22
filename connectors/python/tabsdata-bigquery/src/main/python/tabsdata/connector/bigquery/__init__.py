#
# Copyright 2025 Tabsdata Inc.
#

import importlib.metadata

from tabsdata.connector.bigquery._connector import BigQueryDest

__all__ = ["BigQueryDest"]

# noinspection PyBroadException
try:
    __version__ = importlib.metadata.version("tabsdata-bigquery")
except Exception:
    __version__ = "unknown"

version = __version__
