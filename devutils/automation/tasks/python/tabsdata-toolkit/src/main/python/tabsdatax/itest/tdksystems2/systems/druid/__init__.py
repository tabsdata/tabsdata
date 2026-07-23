#
# Copyright 2026 Tabsdata Inc.
#

# Apache Druid system: a docker-compose stack; a domain is a datasource. Only the
# backend + compose file + create command are system-specific; every other verb
# runs the shared command in _common/commands.

from tabsdatax.itest.tdksystems2.systems.druid.backend import Druid

__all__ = ["Druid"]
