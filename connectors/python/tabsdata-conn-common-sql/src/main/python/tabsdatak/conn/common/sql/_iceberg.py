#
# Copyright 2026 Tabsdata Inc.
#

"""Iceberg-style schema evolution infrastructure shared by SQL
connectors.

Provides the dialect-agnostic pieces:

- :class:`Action` / :class:`ReorderPolicy` enums.
- Model dataclasses: :class:`ColumnDef`, :class:`TableSchema`,
  :class:`SchemaChange`, :class:`SchemaDelta`, :class:`RenderOptions`,
  :class:`UnsafeSchemaChangeError`, :data:`TypeWideningMap`.
- :func:`compute_delta` / :func:`validate_delta` (core algorithm).
- :class:`DialectRenderer` abstract base.

Each SQL connector that supports Iceberg-style schema evolution ships
its own concrete :class:`DialectRenderer` subclass plus the dialect's
type-widening map (e.g.
``tabsdatak.conn.clickhouse._iceberg.ClickHouseRenderer``).
"""

from __future__ import annotations

from abc import ABC, abstractmethod
from collections.abc import Callable
from dataclasses import dataclass, field
from enum import Enum

# --- Models ----------------------------------------------------------


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
    type: str  # normalized type token
    nullable: bool = True
    default: str | None = None


@dataclass(frozen=True)
class TableSchema:
    table_name: str
    columns: list[ColumnDef] = field(default_factory=list)


# Map new_type -> set of current_types that can safely widen to it.
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


# --- Core: compute / validate delta ----------------------------------


def compute_delta(
    s1: TableSchema | None,
    s2: TableSchema,
    widen_map: TypeWideningMap,
    can_safely_widen: Callable[[str, str], bool] | None = None,
) -> SchemaDelta:
    """Compute the schema delta between ``s1`` (current) and ``s2``
    (desired).

    A retype is marked safe when ``widen_map`` allows it. A dialect whose
    answer cannot be spelled as a set of type names -- one whose types carry
    parameters, say, so that the safe conversions are not a fixed list -- passes
    ``can_safely_widen`` as well, called ``(old_type, new_type)`` and consulted
    only when the map has not already said yes. Either saying so is enough.
    """
    delta = SchemaDelta(table_name=s2.table_name)

    # CASE 1: Table does not exist.
    if s1 is None or not s1.columns:
        delta.is_create = True
        for i, col in enumerate(s2.columns):
            delta.changes.append(
                SchemaChange(
                    action=Action.ADD_COLUMN,
                    column=col.name,
                    safe=True,
                    detail={
                        "type": col.type,
                        "nullable": col.nullable,
                        "default": col.default,
                        "position": i,
                    },
                )
            )
        return delta

    # CASE 2: Both schemas have columns.
    delta.is_create = False

    s1_map = {col.name: col for col in s1.columns}
    s2_map = {col.name: col for col in s2.columns}
    s1_names = [col.name for col in s1.columns]
    s2_names = [col.name for col in s2.columns]
    s1_name_set = set(s1_names)
    s2_name_set = set(s2_names)

    # 2a: Dropped columns (in S1 but not in S2).
    for name in s1_names:
        if name not in s2_name_set:
            delta.changes.append(
                SchemaChange(
                    action=Action.DROP_COLUMN,
                    column=name,
                    safe=True,
                    detail={"old_type": s1_map[name].type},
                )
            )

    # 2b: Added columns (in S2 but not in S1).
    for i, name in enumerate(s2_names):
        if name not in s1_name_set:
            col = s2_map[name]
            # Iceberg rule: new columns must be optional or have a default.
            safe = col.nullable or col.default is not None
            delta.changes.append(
                SchemaChange(
                    action=Action.ADD_COLUMN,
                    column=name,
                    safe=safe,
                    detail={
                        "type": col.type,
                        "nullable": col.nullable,
                        "default": col.default,
                        "position": i,
                    },
                )
            )

    # 2c: Modified columns (in both S1 and S2).
    for name in s1_names:
        if name not in s2_name_set:
            continue
        old = s1_map[name]
        new = s2_map[name]

        # 2c-i: Type change.
        if old.type != new.type:
            safe = (new.type in widen_map and old.type in widen_map[new.type]) or (
                can_safely_widen(old.type, new.type) if can_safely_widen else False
            )
            delta.changes.append(
                SchemaChange(
                    action=Action.ALTER_TYPE,
                    column=name,
                    safe=safe,
                    detail={
                        "old_type": old.type,
                        "new_type": new.type,
                        "nullable": new.nullable,
                    },
                )
            )

        # 2c-ii: Nullability change.
        if old.nullable != new.nullable:
            if not old.nullable and new.nullable:
                # required -> optional: SAFE
                delta.changes.append(
                    SchemaChange(
                        action=Action.SET_NULLABLE,
                        column=name,
                        safe=True,
                        detail={"type": new.type},
                    )
                )
            else:
                # optional -> required: UNSAFE
                delta.changes.append(
                    SchemaChange(
                        action=Action.DROP_NULLABLE,
                        column=name,
                        safe=False,
                        detail={"type": new.type},
                    )
                )

        # 2c-iii: Default change.
        if old.default != new.default:
            if new.default is None:
                delta.changes.append(
                    SchemaChange(
                        action=Action.DROP_DEFAULT,
                        column=name,
                        safe=True,
                        detail={
                            "type": new.type,
                            "old_default": old.default,
                        },
                    )
                )
            else:
                delta.changes.append(
                    SchemaChange(
                        action=Action.SET_DEFAULT,
                        column=name,
                        safe=True,
                        detail={
                            "type": new.type,
                            "nullable": new.nullable,
                            "new_default": new.default,
                        },
                    )
                )

    # 2d: Positional changes.
    common_in_s1_order = [n for n in s1_names if n in s2_name_set]
    common_in_s2_order = [n for n in s2_names if n in s1_name_set]

    if common_in_s1_order != common_in_s2_order:
        for new_pos, name in enumerate(common_in_s2_order):
            old_pos = common_in_s1_order.index(name)
            if old_pos != new_pos:
                after_col = common_in_s2_order[new_pos - 1] if new_pos > 0 else None
                col = s2_map[name]
                delta.changes.append(
                    SchemaChange(
                        action=Action.REORDER_COLUMN,
                        column=name,
                        safe=True,
                        detail={
                            "type": col.type,
                            "nullable": col.nullable,
                            "default": col.default,
                            "old_position": old_pos,
                            "new_position": new_pos,
                            "after_column": after_col,
                        },
                    )
                )

    return delta


def validate_delta(delta: SchemaDelta, strict: bool = False) -> bool:
    """Validate a schema delta.

    When ``strict`` is True, raise :class:`UnsafeSchemaChangeError` on
    any unsafe change. Returns True if all changes are safe.
    """
    unsafe = [c for c in delta.changes if not c.safe]

    if strict and unsafe:
        raise UnsafeSchemaChangeError(unsafe)

    return len(unsafe) == 0


# --- Renderer base ---------------------------------------------------


class DialectRenderer(ABC):
    """Abstract base for dialect-specific SQL renderers.

    Concrete subclasses define which :class:`Action` values they support
    (``SUPPORTED_ACTIONS``), whether they support physical column
    reordering (``SUPPORTS_REORDER``), and how to render an individual
    ALTER statement plus column definitions.
    """

    SUPPORTED_ACTIONS: set[Action] = set()
    SUPPORTS_REORDER: bool = False

    @abstractmethod
    def column_def(
        self,
        name: str,
        type_: str,
        nullable: bool,
        default: str | None,
    ) -> str:
        """Render a column definition for CREATE TABLE."""

    @abstractmethod
    def render_alter(
        self,
        table_name: str,
        change: SchemaChange,
    ) -> str | None:
        """Render a single ALTER statement. Return None to skip
        unsupported actions."""

    def render_sql(
        self,
        delta: SchemaDelta,
        options: RenderOptions | None = None,
    ) -> list[str]:
        if options is None:
            options = RenderOptions()

        if delta.is_create:
            return [self._render_create_table(delta)]

        stmts = []
        for change in delta.changes:
            if change.action == Action.REORDER_COLUMN:
                if options.reorder_policy == ReorderPolicy.IGNORE:
                    continue
                if options.reorder_policy == ReorderPolicy.FAIL:
                    raise ValueError(
                        f"Column reordering detected for "
                        f"'{change.column}' but reorder policy is FAIL"
                    )
                if not self.SUPPORTS_REORDER:
                    raise ValueError(
                        f"Dialect does not support column reordering"
                        f" (column '{change.column}')"
                    )

            if change.action not in self.SUPPORTED_ACTIONS:
                continue  # silently skip unsupported actions

            stmt = self.render_alter(delta.table_name, change)
            if stmt is not None:
                stmts.append(stmt)

        return stmts

    def _render_create_table(self, delta: SchemaDelta) -> str:
        add_columns = sorted(
            [c for c in delta.changes if c.action == Action.ADD_COLUMN],
            key=lambda c: c.detail["position"],
        )
        col_defs = ",\n    ".join(
            self.column_def(
                c.column,
                c.detail["type"],
                c.detail["nullable"],
                c.detail.get("default"),
            )
            for c in add_columns
        )
        return self._create_table_sql(delta.table_name, col_defs)

    def _create_table_sql(self, table_name: str, col_defs: str) -> str:
        return f"CREATE TABLE {table_name} (\n    {col_defs}\n)"
