#
# Copyright {c.year} Tabs Data Inc.
#

import importlib.metadata

__all__ = []

# noinspection PyBroadException
try:
    __version__ = importlib.metadata.version("tabsdata-{c.package}")
except Exception:
    __version__ = "unknown"

version = __version__
