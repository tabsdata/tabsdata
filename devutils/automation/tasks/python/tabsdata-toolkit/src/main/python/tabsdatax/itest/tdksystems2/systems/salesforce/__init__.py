#
# Copyright 2026 Tabsdata Inc.
#

# Salesforce system: a credential-only org (a domain clones the org creds). Only
# the backend + create command are system-specific; every other verb runs the
# shared command in _common/commands.

from tabsdatax.itest.tdksystems2.systems.salesforce.backend import Salesforce

__all__ = ["Salesforce"]
