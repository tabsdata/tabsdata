#
# Copyright 2026 Tabsdata Inc.
#

# MongoDB system: docker container; a domain is a database + dbOwner user. Only
# the backend is system-specific; every verb runs the shared command in
# _common/commands.

from tabsdatax.itest.tdksystems2.systems.mongodb.backend import MongoDB

__all__ = ["MongoDB"]
