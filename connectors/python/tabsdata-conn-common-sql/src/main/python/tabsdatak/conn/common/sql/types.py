#
# Copyright 2026 Tabsdata Inc.
#

from __future__ import annotations

from typing import TypeAlias

from tabsdatak.conn.common.types import DataType

ColumnNameSpec: TypeAlias = str  # PLACEHOLDER it should be a non-empty str at least
SchemaOverrideSpec: TypeAlias = list[dict[ColumnNameSpec, DataType] | None]
