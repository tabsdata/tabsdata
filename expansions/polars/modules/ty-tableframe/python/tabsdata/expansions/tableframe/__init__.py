import pkgutil

# noinspection PyUnboundLocalVariable
__path__ = pkgutil.extend_path(__path__, __name__)

# The lines above must appear at the top of this file to ensure
# PyCharm correctly recognizes namespace packages.

#
# Copyright 2025 Tabs Data Inc.
#

# Required import to ensure dynamic library with native components is loaded.
# noinspection PyProtectedMember
import tabsdata.expansions.tableframe._expressions  # noqa: F401

# Required to register the 'logs' namespace.
import tabsdata.expansions.tableframe.features.grok  # noqa: F401
