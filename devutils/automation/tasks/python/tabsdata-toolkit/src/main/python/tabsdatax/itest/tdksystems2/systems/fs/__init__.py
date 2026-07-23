#
# Copyright 2026 Tabsdata Inc.
#

# Local-filesystem system: domains are directories under a staging root. Only the
# storage primitives + fs create command are system-specific; every other verb
# runs the shared command in _common/commands.

from tabsdatax.itest.tdksystems2.systems.fs.backend import Fs

__all__ = ["Fs"]
