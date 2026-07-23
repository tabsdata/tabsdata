#
# Copyright 2026 Tabsdata Inc.
#

# gcs object-store system. Only the storage backend + create command are
# system-specific; every other verb runs the shared command in _common/commands.

from tabsdatax.itest.tdksystems2.systems.gcs.backend import Gcs

__all__ = ["Gcs"]
