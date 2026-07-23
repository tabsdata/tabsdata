#
# Copyright 2026 Tabsdata Inc.
#

# MySQL system: docker-run container + per-domain database (with a dedicated
# owner user). Only the dialect backend is system-specific; every verb runs the
# shared command in systems/_common/commands.

from tabsdatax.itest.tdksystems2.systems.mysql.backend import Mysql

__all__ = ["Mysql"]
