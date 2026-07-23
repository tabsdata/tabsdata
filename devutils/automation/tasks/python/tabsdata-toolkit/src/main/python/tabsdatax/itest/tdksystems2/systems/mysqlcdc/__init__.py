#
# Copyright 2026 Tabsdata Inc.
#

# mysqlcdc system: a CDC variant of the plain SQL system; reuses that system's model
# DDL via ddl_from_slug. Every verb runs the shared command in _common/commands.

from tabsdatax.itest.tdksystems2.systems.mysqlcdc.backend import Mysqlcdc

__all__ = ["Mysqlcdc"]
