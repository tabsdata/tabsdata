#
# Copyright 2026 Tabs Data Inc.
#

from __future__ import annotations

from dataclasses import dataclass, field

from tabsdatak.tableframe._engine.constants.columns.prefixes import (
    TD_CONNECTOR_SYSTEM_COLUMN_PREFIX,
    TD_NAMESPACED_SYSTEM_COLUMN_PREFIXES,
    TD_SYSTEM_COLUMN_PREFIX,
    TD_VOLATILE_SYSTEM_COLUMN_PREFIX,
)
from tabsdatak.tableframe._engine.model.columns import (
    Inception,
    SystemColumn,
    SystemColumnCategory,
)


@dataclass(slots=True)
class Registry:
    columns: dict[str, SystemColumn] = field(default_factory=dict)

    def register(self, *system_columns: SystemColumn) -> None:
        for system_column in system_columns:
            self.columns[system_column.name] = system_column

    def get(self, name: str) -> SystemColumn | None:
        return self.columns.get(name)

    def names(self, *categories: SystemColumnCategory) -> list[str]:
        if not categories:
            return list(self.columns)
        selected = set(categories)
        return [
            name
            for name, system_column in self.columns.items()
            if system_column.category in selected
        ]

    def metadata(self, *categories: SystemColumnCategory) -> dict[str, SystemColumn]:
        if not categories:
            return dict(self.columns)
        selected = set(categories)
        return {
            name: system_column
            for name, system_column in self.columns.items()
            if system_column.category in selected
        }

    def required(self) -> list[str]:
        return [
            name
            for name, system_column in self.columns.items()
            if system_column.required
        ]

    def pristine(self) -> list[str]:
        return [
            name
            for name, system_column in self.columns.items()
            if system_column.pristine
        ]

    def by_inception(self, inception: Inception) -> list[str]:
        return [
            name
            for name, system_column in self.columns.items()
            if system_column.inception is inception
        ]

    def is_system_column(self, name: str) -> bool:
        return (
            name in self.columns
            or name.startswith(TD_SYSTEM_COLUMN_PREFIX)
            or name.startswith(TD_CONNECTOR_SYSTEM_COLUMN_PREFIX)
        )

    def category_of(self, name: str) -> SystemColumnCategory | None:
        system_column = self.get(name)
        if system_column is not None:
            return system_column.category
        if name.startswith(TD_VOLATILE_SYSTEM_COLUMN_PREFIX):
            return SystemColumnCategory.VOLATILE
        if name.startswith(TD_CONNECTOR_SYSTEM_COLUMN_PREFIX):
            return SystemColumnCategory.CONNECTOR
        if name.startswith(TD_SYSTEM_COLUMN_PREFIX):
            return SystemColumnCategory.STANDARD
        return None


def is_namespaced_column(name: str) -> bool:
    return any(
        name.startswith(prefix) for prefix in TD_NAMESPACED_SYSTEM_COLUMN_PREFIXES
    )
