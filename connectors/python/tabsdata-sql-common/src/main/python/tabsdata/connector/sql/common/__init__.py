#
# Copyright 2026 Tabsdata Inc.
#

import importlib.metadata

__all__ = []

# noinspection PyBroadException
try:
    __version__ = importlib.metadata.version("tabsdata-sql-common")
except Exception:
    __version__ = "unknown"

version = __version__
