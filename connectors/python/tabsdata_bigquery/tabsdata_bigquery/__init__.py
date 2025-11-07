#
# Copyright 2025 Tabs Data Inc.
#

import importlib.metadata

from tabsdata_bigquery._connector import BigQueryDest

__all__ = ["BigQueryDest"]

# noinspection PyBroadException
try:
    __version__ = importlib.metadata.version("tabsdata_bigquery")
except Exception:
    __version__ = "unknown"

version = __version__
