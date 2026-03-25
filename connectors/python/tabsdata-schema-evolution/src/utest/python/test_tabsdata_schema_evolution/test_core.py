#
# Copyright 2026 Tabs Data Inc.
#

from __future__ import annotations

import pytest

from tabsdata.connector.schema.evolution.iceberg.core import compute_delta, validate_delta
from tabsdata.connector.schema.evolution.iceberg.models import (
    Action,
    ColumnDef,
    SchemaDelta,
    SchemaChange,
    TableSchema,
    TypeWideningMap,
    UnsafeSchemaChangeError,
)


WIDEN_MAP: TypeWideningMap = {
    "BIGINT": {"INT", "SMALLINT"},
    "DOUBLE": {"FLOAT"},
}


# ── compute_delta: CASE 1 (create) ──────────────────────────────────────────────────────


class TestComputeDeltaCreate:
    def test_null_s1_produces_create(self):
        s2 = TableSchema(
            table_name="t",
            columns=[ColumnDef(name="a", type="INT", nullable=False)],
        )
        delta = compute_delta(None, s2, WIDEN_MAP)
        assert delta.is_create is True
        assert len(delta.changes) == 1
        assert delta.changes[0].action == Action.ADD_COLUMN
        assert delta.changes[0].safe is True

    def test_empty_s1_produces_create(self):
        s1 = TableSchema(table_name="t", columns=[])
        s2 = TableSchema(
            table_name="t",
            columns=[ColumnDef(name="a", type="INT")],
        )
        delta = compute_delta(s1, s2, WIDEN_MAP)
        assert delta.is_create is True

    def test_create_preserves_position(self):
        s2 = TableSchema(
            table_name="t",
            columns=[
                ColumnDef(name="a", type="INT"),
                ColumnDef(name="b", type="TEXT"),
                ColumnDef(name="c", type="FLOAT"),
            ],
        )
        delta = compute_delta(None, s2, WIDEN_MAP)
        positions = [c.detail["position"] for c in delta.changes]
        assert positions == [0, 1, 2]


# ── compute_delta: CASE 2 (evolution) ────────────────────────────────────────────────────


class TestComputeDeltaEvolution:
    def test_no_changes(self):
        s = TableSchema(
            table_name="t",
            columns=[ColumnDef(name="a", type="INT", nullable=False)],
        )
        delta = compute_delta(s, s, WIDEN_MAP)
        assert delta.is_create is False
        assert delta.changes == []

    def test_drop_column(self):
        s1 = TableSchema(
            table_name="t",
            columns=[
                ColumnDef(name="a", type="INT"),
                ColumnDef(name="b", type="TEXT"),
            ],
        )
        s2 = TableSchema(
            table_name="t",
            columns=[ColumnDef(name="a", type="INT")],
        )
        delta = compute_delta(s1, s2, WIDEN_MAP)
        assert len(delta.changes) == 1
        assert delta.changes[0].action == Action.DROP_COLUMN
        assert delta.changes[0].column == "b"
        assert delta.changes[0].safe is True

    def test_add_nullable_column_is_safe(self):
        s1 = TableSchema(
            table_name="t",
            columns=[ColumnDef(name="a", type="INT")],
        )
        s2 = TableSchema(
            table_name="t",
            columns=[
                ColumnDef(name="a", type="INT"),
                ColumnDef(name="b", type="TEXT", nullable=True),
            ],
        )
        delta = compute_delta(s1, s2, WIDEN_MAP)
        add = [c for c in delta.changes if c.action == Action.ADD_COLUMN]
        assert len(add) == 1
        assert add[0].safe is True

    def test_add_non_nullable_without_default_is_unsafe(self):
        s1 = TableSchema(
            table_name="t",
            columns=[ColumnDef(name="a", type="INT")],
        )
        s2 = TableSchema(
            table_name="t",
            columns=[
                ColumnDef(name="a", type="INT"),
                ColumnDef(name="b", type="TEXT", nullable=False),
            ],
        )
        delta = compute_delta(s1, s2, WIDEN_MAP)
        add = [c for c in delta.changes if c.action == Action.ADD_COLUMN]
        assert add[0].safe is False

    def test_add_non_nullable_with_default_is_safe(self):
        s1 = TableSchema(
            table_name="t",
            columns=[ColumnDef(name="a", type="INT")],
        )
        s2 = TableSchema(
            table_name="t",
            columns=[
                ColumnDef(name="a", type="INT"),
                ColumnDef(
                    name="b", type="TEXT", nullable=False, default="'x'"
                ),
            ],
        )
        delta = compute_delta(s1, s2, WIDEN_MAP)
        add = [c for c in delta.changes if c.action == Action.ADD_COLUMN]
        assert add[0].safe is True

    def test_safe_type_widening(self):
        s1 = TableSchema(
            table_name="t",
            columns=[ColumnDef(name="a", type="INT")],
        )
        s2 = TableSchema(
            table_name="t",
            columns=[ColumnDef(name="a", type="BIGINT")],
        )
        delta = compute_delta(s1, s2, WIDEN_MAP)
        alter = [c for c in delta.changes if c.action == Action.ALTER_TYPE]
        assert len(alter) == 1
        assert alter[0].safe is True

    def test_unsafe_type_change(self):
        s1 = TableSchema(
            table_name="t",
            columns=[ColumnDef(name="a", type="BIGINT")],
        )
        s2 = TableSchema(
            table_name="t",
            columns=[ColumnDef(name="a", type="INT")],
        )
        delta = compute_delta(s1, s2, WIDEN_MAP)
        alter = [c for c in delta.changes if c.action == Action.ALTER_TYPE]
        assert alter[0].safe is False

    def test_set_nullable(self):
        s1 = TableSchema(
            table_name="t",
            columns=[ColumnDef(name="a", type="INT", nullable=False)],
        )
        s2 = TableSchema(
            table_name="t",
            columns=[ColumnDef(name="a", type="INT", nullable=True)],
        )
        delta = compute_delta(s1, s2, WIDEN_MAP)
        change = delta.changes[0]
        assert change.action == Action.SET_NULLABLE
        assert change.safe is True

    def test_drop_nullable(self):
        s1 = TableSchema(
            table_name="t",
            columns=[ColumnDef(name="a", type="INT", nullable=True)],
        )
        s2 = TableSchema(
            table_name="t",
            columns=[ColumnDef(name="a", type="INT", nullable=False)],
        )
        delta = compute_delta(s1, s2, WIDEN_MAP)
        change = delta.changes[0]
        assert change.action == Action.DROP_NULLABLE
        assert change.safe is False

    def test_set_default(self):
        s1 = TableSchema(
            table_name="t",
            columns=[ColumnDef(name="a", type="INT", default=None)],
        )
        s2 = TableSchema(
            table_name="t",
            columns=[ColumnDef(name="a", type="INT", default="42")],
        )
        delta = compute_delta(s1, s2, WIDEN_MAP)
        change = delta.changes[0]
        assert change.action == Action.SET_DEFAULT
        assert change.detail["new_default"] == "42"
        assert "nullable" in change.detail

    def test_drop_default(self):
        s1 = TableSchema(
            table_name="t",
            columns=[ColumnDef(name="a", type="INT", default="42")],
        )
        s2 = TableSchema(
            table_name="t",
            columns=[ColumnDef(name="a", type="INT", default=None)],
        )
        delta = compute_delta(s1, s2, WIDEN_MAP)
        change = delta.changes[0]
        assert change.action == Action.DROP_DEFAULT
        assert change.detail["old_default"] == "42"
        assert "nullable" not in change.detail

    def test_reorder_detection(self):
        s1 = TableSchema(
            table_name="t",
            columns=[
                ColumnDef(name="a", type="INT"),
                ColumnDef(name="b", type="INT"),
                ColumnDef(name="c", type="INT"),
            ],
        )
        s2 = TableSchema(
            table_name="t",
            columns=[
                ColumnDef(name="c", type="INT"),
                ColumnDef(name="a", type="INT"),
                ColumnDef(name="b", type="INT"),
            ],
        )
        delta = compute_delta(s1, s2, WIDEN_MAP)
        reorders = [
            c for c in delta.changes if c.action == Action.REORDER_COLUMN
        ]
        assert len(reorders) > 0
        assert all(r.safe is True for r in reorders)

    def test_reorder_not_triggered_by_add_drop(self):
        """Adding/dropping columns should not cause spurious reorder changes."""
        s1 = TableSchema(
            table_name="t",
            columns=[
                ColumnDef(name="a", type="INT"),
                ColumnDef(name="b", type="INT"),
            ],
        )
        s2 = TableSchema(
            table_name="t",
            columns=[
                ColumnDef(name="a", type="INT"),
                ColumnDef(name="c", type="INT", nullable=True),
                ColumnDef(name="b", type="INT"),
            ],
        )
        delta = compute_delta(s1, s2, WIDEN_MAP)
        reorders = [
            c for c in delta.changes if c.action == Action.REORDER_COLUMN
        ]
        assert reorders == []


# ── validate_delta ───────────────────────────────────────────────────────────────────────


class TestValidateDelta:
    def test_all_safe_returns_true(self):
        delta = SchemaDelta(
            table_name="t",
            changes=[
                SchemaChange(
                    action=Action.ADD_COLUMN,
                    column="x",
                    safe=True,
                    detail={},
                )
            ],
        )
        assert validate_delta(delta) is True

    def test_unsafe_returns_false(self):
        delta = SchemaDelta(
            table_name="t",
            changes=[
                SchemaChange(
                    action=Action.ALTER_TYPE,
                    column="x",
                    safe=False,
                    detail={},
                )
            ],
        )
        assert validate_delta(delta) is False

    def test_strict_raises_on_unsafe(self):
        delta = SchemaDelta(
            table_name="t",
            changes=[
                SchemaChange(
                    action=Action.DROP_NULLABLE,
                    column="x",
                    safe=False,
                    detail={},
                )
            ],
        )
        with pytest.raises(UnsafeSchemaChangeError):
            validate_delta(delta, strict=True)

    def test_strict_passes_when_all_safe(self):
        delta = SchemaDelta(
            table_name="t",
            changes=[
                SchemaChange(
                    action=Action.ADD_COLUMN,
                    column="x",
                    safe=True,
                    detail={},
                )
            ],
        )
        assert validate_delta(delta, strict=True) is True

    def test_empty_delta_is_valid(self):
        delta = SchemaDelta(table_name="t")
        assert validate_delta(delta, strict=True) is True


# ── Multiple changes on same column ──────────────────────────────────────────────────


class TestMultipleChangesOnSameColumn:
    """Column with simultaneous type + nullability + default changes."""

    def test_widen_and_set_nullable_and_set_default(self):
        """INT NOT NULL no-default → BIGINT NULL DEFAULT 0."""
        s1 = TableSchema(
            table_name="t",
            columns=[
                ColumnDef(name="a", type="INT", nullable=False),
            ],
        )
        s2 = TableSchema(
            table_name="t",
            columns=[
                ColumnDef(
                    name="a", type="BIGINT", nullable=True, default="0"
                ),
            ],
        )
        delta = compute_delta(s1, s2, WIDEN_MAP)
        actions = [c.action for c in delta.changes]
        assert Action.ALTER_TYPE in actions
        assert Action.SET_NULLABLE in actions
        assert Action.SET_DEFAULT in actions
        assert all(c.safe for c in delta.changes)

    def test_alter_type_carries_nullable_flag(self):
        """ALTER_TYPE detail includes the desired nullable state."""
        s1 = TableSchema(
            table_name="t",
            columns=[
                ColumnDef(name="a", type="INT", nullable=True),
            ],
        )
        s2 = TableSchema(
            table_name="t",
            columns=[
                ColumnDef(name="a", type="BIGINT", nullable=True),
            ],
        )
        delta = compute_delta(s1, s2, WIDEN_MAP)
        alter = [c for c in delta.changes if c.action == Action.ALTER_TYPE]
        assert len(alter) == 1
        assert alter[0].detail["nullable"] is True

    def test_empty_desired_schema_drops_all(self):
        """All columns dropped when desired schema has no columns."""
        s1 = TableSchema(
            table_name="t",
            columns=[
                ColumnDef(name="a", type="INT"),
                ColumnDef(name="b", type="TEXT"),
            ],
        )
        s2 = TableSchema(table_name="t", columns=[])
        delta = compute_delta(s1, s2, WIDEN_MAP)
        assert all(c.action == Action.DROP_COLUMN for c in delta.changes)
        assert len(delta.changes) == 2
