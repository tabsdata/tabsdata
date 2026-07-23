#
# Copyright 2026 Tabsdata Inc.
#

# ClickHouse system: docker container; a domain is a database + owner user. Only
# the dialect backend + per-model DDL are system-specific; every verb runs the
# shared command in _common/commands.

from tabsdatax.itest.tdksystems2.systems.clickhouse.backend import Clickhouse

__all__ = ["Clickhouse"]
