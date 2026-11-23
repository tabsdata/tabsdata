#
# Copyright 2026 Tabsdata Inc.
#

from __future__ import annotations

from functools import cache

from tabsdatak.tableframe._engine.column.registry import Registry


@cache
def registry() -> Registry:
    from tabsdatak.extensions.tableframe._engine.constant.column.system import (
        META,
        OPTIONAL,
        STANDARD,
    )

    instance = Registry()
    instance.register(*STANDARD, *OPTIONAL, *META)
    return instance
