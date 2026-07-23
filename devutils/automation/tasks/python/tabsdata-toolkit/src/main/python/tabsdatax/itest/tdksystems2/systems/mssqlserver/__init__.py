#
# Copyright 2026 Tabsdata Inc.
#

# mssqlserver system: docker-run container. Only the dialect backend is system-specific;
# every verb runs the shared command in systems/_common/commands.

from tabsdatax.itest.tdksystems2.systems.mssqlserver.backend import Mssqlserver

__all__ = ["Mssqlserver"]
