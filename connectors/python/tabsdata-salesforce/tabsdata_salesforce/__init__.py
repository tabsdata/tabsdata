#
# Copyright 2025 Tabs Data Inc.
#

import importlib.metadata

from tabsdata_salesforce._connector import SalesforceSource

__all__ = ["SalesforceSource"]

# noinspection PyBroadException
try:
    __version__ = importlib.metadata.version("tabsdata_salesforce")
except Exception:
    __version__ = "unknown"

version = __version__
