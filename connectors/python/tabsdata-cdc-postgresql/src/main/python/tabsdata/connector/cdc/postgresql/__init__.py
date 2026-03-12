#
# Copyright 2026 Tabs Data Inc.
#

import importlib.metadata

__all__ = []

# noinspection PyBroadException
try:
    __version__ = importlib.metadata.version("tabsdata-cdc-postgresql")
except Exception:
    __version__ = "unknown"

version = __version__
