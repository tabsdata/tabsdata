#
# Copyright 2026 Tabsdata Inc.
#

# IBM Db2 system: docker-run container, one schema per domain in a shared XP
# database. Only the dialect backend is system-specific; every verb runs the
# shared command in systems/_common/commands.

from tabsdatax.itest.tdksystems2.systems.db2.backend import Db2

__all__ = ["Db2"]
