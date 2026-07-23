#
# Copyright 2026 Tabsdata Inc.
#

# Databricks system: a remote warehouse; domains are schemas in the Unity catalog.
# Only the backend + create command are system-specific; every other verb runs the
# shared command in _common/commands.

from tabsdatax.itest.tdksystems2.systems.databricks.backend import Databricks

__all__ = ["Databricks"]
