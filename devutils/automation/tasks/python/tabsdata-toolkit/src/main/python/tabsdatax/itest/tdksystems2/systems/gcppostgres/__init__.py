#
# Copyright 2026 Tabsdata Inc.
#

# GCP Cloud SQL PostgreSQL system: a managed remote instance; domains are
# databases with a dedicated owner role. Only the backend is system-specific;
# every verb runs the shared command in _common/commands.

from tabsdatax.itest.tdksystems2.systems.gcppostgres.backend import GcpPostgres

__all__ = ["GcpPostgres"]
