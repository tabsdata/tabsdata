#
# Copyright 2026 Tabsdata Inc.
#

from __future__ import annotations

from importlib.metadata import PackageNotFoundError, version as installed_version

DISTRIBUTION = "tabsdata-server"
UNKNOWN = "unknown"


def version() -> str:
    try:
        return installed_version(DISTRIBUTION)
    except PackageNotFoundError:
        return UNKNOWN
