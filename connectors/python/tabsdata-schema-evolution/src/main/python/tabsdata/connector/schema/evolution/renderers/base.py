#
# Copyright 2026 Tabs Data Inc.
#

from __future__ import annotations

from abc import ABC, abstractmethod

from tabsdata.connector.schema.evolution.models import (
    Action,
    RenderOptions,
    ReorderPolicy,
    SchemaChange,
    SchemaDelta,
)


class DialectRenderer(ABC):
    """Abstract base for dialect-specific SQL renderers."""

    # Set of actions this dialect supports for ALTER statements.
    SUPPORTED_ACTIONS: set[Action] = set()

    # Whether this dialect supports physical column reordering.
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
        ...

    @abstractmethod
    def render_alter(
        self,
        table_name: str,
        change: SchemaChange,
    ) -> str | None:
        """Render a single ALTER statement.

        Return None to skip unsupported actions.
        """
        ...

    def render_sql(
        self,
        delta: SchemaDelta,
        options: RenderOptions | None = None,
    ) -> list[str]:
        """Render a SchemaDelta as a list of SQL statements.

        Args:
            delta: The SchemaDelta to render.
            options: Render options (reorder policy, etc.).

        Returns:
            List of SQL statements to apply the delta.
        """
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
                        f"Column reordering detected for '{change.column}' "
                        f"but reorder policy is FAIL"
                    )
                # APPLY -- check dialect support
                if not self.SUPPORTS_REORDER:
                    raise ValueError(
                        f"Dialect does not support column reordering "
                        f"(column '{change.column}')"
                    )

            if change.action not in self.SUPPORTED_ACTIONS:
                continue  # silently skip unsupported actions

            stmt = self.render_alter(delta.table_name, change)
            if stmt is not None:
                stmts.append(stmt)

        return stmts

    def _render_create_table(self, delta: SchemaDelta) -> str:
        """Render a CREATE TABLE statement from a create delta."""
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
        """Render the CREATE TABLE statement.

        Override for dialect-specific suffixes.
        """
        return f"CREATE TABLE {table_name} (\n    {col_defs}\n)"
