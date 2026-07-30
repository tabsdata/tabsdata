#
# Copyright 2026 Tabsdata Inc.
#

# AWS RDS PostgreSQL system: a managed remote instance; domains are databases
# with a dedicated owner role. Only the backend is system-specific; every verb
# runs the shared command in _common/commands.

from tabsdatax.itest.tdksystems2.systems.rdspostgres.backend import RdsPostgres

__all__ = ["RdsPostgres"]
