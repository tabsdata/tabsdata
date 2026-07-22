#
# Copyright 2026 Tabsdata Inc.
#

from __future__ import annotations

from dataclasses import dataclass, field

from tabsdatak.tableframe._engine.constant.column.prefixes import (
    TD_CONNECTOR_SYSTEM_COLUMN_PREFIX,
    TD_NAMESPACED_SYSTEM_COLUMN_PREFIXES,
    TD_SYSTEM_COLUMN_PREFIX,
    TD_VOLATILE_SYSTEM_COLUMN_PREFIX,
)
from tabsdatak.tableframe._engine.model.column import SystemColumn
from tabsdatak.tableframe._engine.taxonomy.column import (
    ColumnKind,
    Inception,
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

    def optional(self) -> list[str]:
        return self.names(SystemColumnCategory.OPTIONAL)

    def row_index_column(self) -> str | None:
        for name, system_column in self.columns.items():
            if system_column.category is SystemColumnCategory.EXTENDED and (
                system_column.inception is Inception.REGENERATE
            ):
                return name
        return None

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
        by_prefix = (
            (TD_VOLATILE_SYSTEM_COLUMN_PREFIX, SystemColumnCategory.VOLATILE),
            (TD_CONNECTOR_SYSTEM_COLUMN_PREFIX, SystemColumnCategory.CONNECTOR),
            (TD_SYSTEM_COLUMN_PREFIX, SystemColumnCategory.STANDARD),
        )
        for prefix, category in by_prefix:
            if name.startswith(prefix):
                return category
        return None

    def kind_of(self, name: str) -> ColumnKind:
        category = self.category_of(name)
        if category is None:
            return ColumnKind.USER
        if category is SystemColumnCategory.CONNECTOR:
            return ColumnKind.CONNECTOR
        return ColumnKind.INTERNAL

    def keep(self, names, *, with_internal: bool, with_connector: bool) -> list[str]:
        kept = {ColumnKind.USER}
        if with_internal:
            kept.add(ColumnKind.INTERNAL)
        if with_connector:
            kept.add(ColumnKind.CONNECTOR)
        return [name for name in names if self.kind_of(name) in kept]


def is_namespaced_column(name: str) -> bool:
    return any(
        name.startswith(prefix) for prefix in TD_NAMESPACED_SYSTEM_COLUMN_PREFIXES
    )
