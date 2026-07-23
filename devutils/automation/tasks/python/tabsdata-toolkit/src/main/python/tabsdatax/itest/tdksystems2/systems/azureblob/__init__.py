#
# Copyright 2026 Tabsdata Inc.
#

# azureblob object-store system. Only the storage backend + create command are
# system-specific; every other verb runs the shared command in _common/commands.

from tabsdatax.itest.tdksystems2.systems.azureblob.backend import Azureblob

__all__ = ["Azureblob"]
