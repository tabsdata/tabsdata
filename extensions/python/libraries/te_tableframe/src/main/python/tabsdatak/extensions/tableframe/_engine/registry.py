#
# Copyright 2026 Tabs Data Inc.
#

from __future__ import annotations

from functools import cache

from tabsdatak.tableframe._engine.system.registry import Registry


@cache
def registry() -> Registry:
    from tabsdatak.extensions.tableframe._engine.constants.columns.system import (
        CONNECTOR,
        OPTIONAL,
        STANDARD,
    )

    instance = Registry()
    instance.register(*STANDARD, *OPTIONAL, *CONNECTOR)
    return instance
