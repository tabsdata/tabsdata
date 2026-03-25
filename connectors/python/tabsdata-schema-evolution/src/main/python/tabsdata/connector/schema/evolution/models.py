#
# Copyright 2026 Tabs Data Inc.
#

from __future__ import annotations

from dataclasses import dataclass, field
from enum import Enum


class Action(Enum):
    ADD_COLUMN = "ADD_COLUMN"
    DROP_COLUMN = "DROP_COLUMN"
    ALTER_TYPE = "ALTER_TYPE"
    SET_NULLABLE = "SET_NULLABLE"
    DROP_NULLABLE = "DROP_NULLABLE"
    SET_DEFAULT = "SET_DEFAULT"
    DROP_DEFAULT = "DROP_DEFAULT"
    REORDER_COLUMN = "REORDER_COLUMN"


class ReorderPolicy(Enum):
    APPLY = "APPLY"
    IGNORE = "IGNORE"
    FAIL = "FAIL"


@dataclass(frozen=True)
class ColumnDef:
    name: str
    type: str  # normalized type token, e.g. "INT", "VARCHAR(255)"
    nullable: bool = True
    default: str | None = None


@dataclass(frozen=True)
class TableSchema:
    table_name: str
    columns: list[ColumnDef] = field(default_factory=list)


# TypeWideningMap: dict mapping new_type -> set of current_types that can
# safely widen to it
TypeWideningMap = dict[str, set[str]]


@dataclass(frozen=True)
class SchemaChange:
    action: Action
    column: str
    safe: bool
    detail: dict = field(default_factory=dict)


@dataclass
class SchemaDelta:
    table_name: str
    is_create: bool = False
    changes: list[SchemaChange] = field(default_factory=list)


@dataclass(frozen=True)
class RenderOptions:
    reorder_policy: ReorderPolicy = ReorderPolicy.IGNORE


class UnsafeSchemaChangeError(Exception):
    """Raised when strict validation encounters unsafe changes."""

    def __init__(self, unsafe_changes: list[SchemaChange]):
        self.unsafe_changes = unsafe_changes
        details = "; ".join(
            f"{c.action.value} on '{c.column}' ({c.detail})" for c in unsafe_changes
        )
        super().__init__(f"Unsafe schema changes detected: {details}")
