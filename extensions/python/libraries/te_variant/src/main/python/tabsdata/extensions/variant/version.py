#
# Copyright 2025 Tabsdata Inc.
#

import importlib.metadata

from tabsdatak.common._constants import NO_VERSION, TABSDATA_MODULE_NAME


def version() -> str:
    try:
        return importlib.metadata.version(TABSDATA_MODULE_NAME)
    except importlib.metadata.PackageNotFoundError:
        return NO_VERSION
