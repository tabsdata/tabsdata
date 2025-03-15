#
# Copyright 2025 Tabs Data Inc.
#

import importlib.metadata

from tabsdata.utils.constants import TABSDATA_MODULE_NAME


def version() -> str:
    return importlib.metadata.version(TABSDATA_MODULE_NAME)
