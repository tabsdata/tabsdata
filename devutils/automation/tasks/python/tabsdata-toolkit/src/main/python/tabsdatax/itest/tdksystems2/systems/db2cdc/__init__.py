#
# Copyright 2026 Tabsdata Inc.
#

# IBM Db2 configured for CDC (ASN SQL replication): a variant of the db2 system
# that reuses its DDL via ddl_from_slug. Only the backend is system-specific;
# every verb runs the shared command in _common/commands.

from tabsdatax.itest.tdksystems2.systems.db2cdc.backend import Db2Cdc

__all__ = ["Db2Cdc"]
