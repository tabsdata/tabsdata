#
# Copyright 2026 Tabs Data Inc.
#

from __future__ import annotations

from dataclasses import dataclass, field
from typing import TYPE_CHECKING

from tabsdatak.tableframe._engine.system.constants.columns.names import (
    TD_EXECUTION_SYSTEM_COLUMN_NAME,
    TD_FILE_LINE_SYSTEM_COLUMN_NAME,
    TD_FILE_PATH_SYSTEM_COLUMN_NAME,
    TD_FUNCTION_TIMESTAMP_SYSTEM_COLUMN_NAME,
    TD_IDENTIFIER_SYSTEM_COLUMN_NAME,
    TD_OFFSET_SYSTEM_COLUMN_NAME,
    TD_SERVER_TIMESTAMP_SYSTEM_COLUMN_NAME,
    TD_TIMESTAMP_SYSTEM_COLUMN_NAME,
    TD_TRANSACTION_SYSTEM_COLUMN_NAME,
    TD_USER_TIMESTAMP_SYSTEM_COLUMN_NAME,
    TD_VALUE_TIMESTAMP_SYSTEM_COLUMN_NAME,
    TD_VERSION_SYSTEM_COLUMN_NAME,
)
from tabsdatak.tableframe._engine.system.constants.columns.prefixes import (
    TD_CONNECTOR_SYSTEM_COLUMN_PREFIX,
    TD_NAMESPACED_SYSTEM_COLUMN_PREFIXES,
    TD_SYSTEM_COLUMN_PREFIX,
    TD_VOLATILE_SYSTEM_COLUMN_PREFIX,
)
from tabsdatak.tableframe._engine.system.constants.empties import (
    EMPTY_EXECUTION,
    EMPTY_TIMESTAMP,
    EMPTY_TRANSACTION,
    EMPTY_VERSION,
)
from tabsdatak.tableframe._engine.system.model import (
    Inception,
    Language,
    SystemColumn,
    SystemColumnCategory,
)

if TYPE_CHECKING:
    import polars as pl

    from tabsdatak.tableframe._engine.system.model import SystemColumnDataType


def dtype_string() -> SystemColumnDataType:
    import polars as pl

    return pl.String


def dtype_timestamp() -> SystemColumnDataType:
    from datetime import timezone

    import polars as pl

    return pl.Datetime(time_unit="us", time_zone=timezone.utc)


def default_empty_string() -> pl.Expr:
    import polars as pl

    return pl.lit("", pl.String)


def no_default() -> None:
    return None


STANDARD: tuple[SystemColumn, ...] = (
    SystemColumn(
        name=TD_IDENTIFIER_SYSTEM_COLUMN_NAME,
        category=SystemColumnCategory.STANDARD,
        dtype=dtype_string,
        default=default_empty_string,
        language=Language.RUST,
        inception=Inception.REGENERATE,
    ),
    SystemColumn(
        name=TD_OFFSET_SYSTEM_COLUMN_NAME,
        category=SystemColumnCategory.STANDARD,
        dtype=dtype_string,
        default=no_default,
        language=Language.PYTHON,
        inception=Inception.PROPAGATE,
    ),
    SystemColumn(
        name=TD_EXECUTION_SYSTEM_COLUMN_NAME,
        category=SystemColumnCategory.STANDARD,
        dtype=dtype_string,
        default=lambda: EMPTY_EXECUTION,
        language=Language.PYTHON,
        inception=Inception.PROPAGATE,
    ),
    SystemColumn(
        name=TD_TRANSACTION_SYSTEM_COLUMN_NAME,
        category=SystemColumnCategory.STANDARD,
        dtype=dtype_string,
        default=lambda: EMPTY_TRANSACTION,
        language=Language.PYTHON,
        inception=Inception.PROPAGATE,
    ),
    SystemColumn(
        name=TD_VERSION_SYSTEM_COLUMN_NAME,
        category=SystemColumnCategory.STANDARD,
        dtype=dtype_string,
        default=lambda: EMPTY_VERSION,
        language=Language.PYTHON,
        inception=Inception.PROPAGATE,
    ),
    SystemColumn(
        name=TD_TIMESTAMP_SYSTEM_COLUMN_NAME,
        category=SystemColumnCategory.STANDARD,
        dtype=dtype_timestamp,
        default=lambda: EMPTY_TIMESTAMP,
        language=Language.PYTHON,
        inception=Inception.PROPAGATE,
    ),
)

OPTIONAL: tuple[SystemColumn, ...] = (
    SystemColumn(
        name=TD_SERVER_TIMESTAMP_SYSTEM_COLUMN_NAME,
        category=SystemColumnCategory.OPTIONAL,
        dtype=dtype_string,
        default=no_default,
        language=Language.PYTHON,
        inception=Inception.PROPAGATE,
    ),
    SystemColumn(
        name=TD_USER_TIMESTAMP_SYSTEM_COLUMN_NAME,
        category=SystemColumnCategory.OPTIONAL,
        dtype=dtype_string,
        default=no_default,
        language=Language.PYTHON,
        inception=Inception.PROPAGATE,
    ),
    SystemColumn(
        name=TD_VALUE_TIMESTAMP_SYSTEM_COLUMN_NAME,
        category=SystemColumnCategory.OPTIONAL,
        dtype=dtype_timestamp,
        default=no_default,
        language=Language.PYTHON,
        inception=Inception.PROPAGATE,
    ),
    SystemColumn(
        name=TD_FUNCTION_TIMESTAMP_SYSTEM_COLUMN_NAME,
        category=SystemColumnCategory.OPTIONAL,
        dtype=dtype_string,
        default=no_default,
        language=Language.PYTHON,
        inception=Inception.PROPAGATE,
    ),
)

CONNECTOR: tuple[SystemColumn, ...] = (
    SystemColumn(
        name=TD_FILE_PATH_SYSTEM_COLUMN_NAME,
        category=SystemColumnCategory.CONNECTOR,
        dtype=dtype_string,
        default=no_default,
        language=Language.PYTHON,
        inception=Inception.PROPAGATE,
    ),
    SystemColumn(
        name=TD_FILE_LINE_SYSTEM_COLUMN_NAME,
        category=SystemColumnCategory.CONNECTOR,
        dtype=dtype_string,
        default=no_default,
        language=Language.PYTHON,
        inception=Inception.PROPAGATE,
    ),
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


REGISTRY = Registry()
REGISTRY.register(*STANDARD, *OPTIONAL, *CONNECTOR)


def is_system_column(name: str) -> bool:
    return (
        name in REGISTRY.columns
        or name.startswith(TD_SYSTEM_COLUMN_PREFIX)
        or name.startswith(TD_CONNECTOR_SYSTEM_COLUMN_PREFIX)
    )


def category_of(name: str) -> SystemColumnCategory | None:
    system_column = REGISTRY.get(name)
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
