#
# Copyright 2026 Tabsdata Inc.
#
"""Shared type shapes for the SQL connector family.

Imported by every `tabsdatak.conn.<sql-db>` package so the SQL
sources agree on how per-query dtype hints are spelled.
"""

from __future__ import annotations

from typing import TypeAlias

from tabsdatak.conn.common.types import DataType

ColumnNameSpec: TypeAlias = str  # PLACEHOLDER it should be a non-empty str at least
"""A column name. Placeholder alias for `str`; not yet constrained
(eventually a non-empty string)."""

SchemaOverrideSpec: TypeAlias = list[dict[ColumnNameSpec, DataType] | None]
"""Per-query column dtype hints for a SQL source's `schema_overrides`.

One entry per query, in the same order (and of the same length) as the
source's `queries`. Each entry is either a `{column_name: DataType}`
mapping overriding the inferred dtype of the named columns, or `None`
to leave that query's schema fully inferred. `DataType` is a
placeholder enumeration and subject to change.
"""
