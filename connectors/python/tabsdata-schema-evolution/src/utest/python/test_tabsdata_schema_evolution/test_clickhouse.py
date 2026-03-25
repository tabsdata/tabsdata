#
# Copyright 2026 Tabs Data Inc.
#

from __future__ import annotations

from tabsdata.connector.schema.evolution.renderers.clickhouse import (
    CLICKHOUSE_TYPE_WIDENING_MAP,
    parse_clickhouse_type,
    render_clickhouse_sql,
    table_schema_from_system_columns,
)
from tabsdata.connector.schema.evolution.core import compute_delta, validate_delta
from tabsdata.connector.schema.evolution.models import (
    ColumnDef,
    RenderOptions,
    ReorderPolicy,
    TableSchema,
)


# ── parse_clickhouse_type ────────────────────────────────────────────────────────────────


class TestParseClickHouseType:
    def test_simple_type(self):
        assert parse_clickhouse_type("Int32") == ("Int32", False)

    def test_nullable_type(self):
        assert parse_clickhouse_type("Nullable(Int32)") == ("Int32", True)

    def test_low_cardinality(self):
        assert parse_clickhouse_type("LowCardinality(String)") == (
            "String",
            False,
        )

    def test_nullable_low_cardinality(self):
        assert parse_clickhouse_type(
            "Nullable(LowCardinality(String))"
        ) == ("String", True)

    def test_float64(self):
        assert parse_clickhouse_type("Float64") == ("Float64", False)

    def test_nullable_float64(self):
        assert parse_clickhouse_type("Nullable(Float64)") == (
            "Float64",
            True,
        )

    def test_datetime64(self):
        assert parse_clickhouse_type("DateTime64(6, 'UTC')") == (
            "DateTime64(6, 'UTC')",
            False,
        )

    def test_nullable_datetime64(self):
        assert parse_clickhouse_type("Nullable(DateTime64(6))") == (
            "DateTime64(6)",
            True,
        )

    def test_decimal(self):
        assert parse_clickhouse_type("Decimal(10, 2)") == (
            "Decimal(10, 2)",
            False,
        )

    def test_array_not_unwrapped(self):
        assert parse_clickhouse_type("Array(Int32)") == (
            "Array(Int32)",
            False,
        )

    def test_map_not_unwrapped(self):
        assert parse_clickhouse_type("Map(String, Int32)") == (
            "Map(String, Int32)",
            False,
        )

    def test_whitespace_stripped(self):
        assert parse_clickhouse_type("  Nullable(Int64)  ") == (
            "Int64",
            True,
        )


# ── table_schema_from_system_columns ─────────────────────────────────────────────────────


class TestTableSchemaFromSystemColumns:
    def test_basic_columns(self):
        rows = [
            ("id", "Int64", "", ""),
            ("name", "Nullable(String)", "", ""),
            ("value", "Float64", "DEFAULT", "0.0"),
        ]
        schema = table_schema_from_system_columns("`db`.`t`", rows)
        assert schema.table_name == "`db`.`t`"
        assert len(schema.columns) == 3
        assert schema.columns[0] == ColumnDef(
            name="id", type="Int64", nullable=False, default=None
        )
        assert schema.columns[1] == ColumnDef(
            name="name", type="String", nullable=True, default=None
        )
        assert schema.columns[2] == ColumnDef(
            name="value", type="Float64", nullable=False, default="0.0"
        )

    def test_materialized_default_ignored(self):
        rows = [("ts", "DateTime64(6)", "MATERIALIZED", "now64(6)")]
        schema = table_schema_from_system_columns("`db`.`t`", rows)
        assert schema.columns[0].default is None

    def test_alias_default_ignored(self):
        rows = [("full_name", "String", "ALIAS", "concat(first, last)")]
        schema = table_schema_from_system_columns("`db`.`t`", rows)
        assert schema.columns[0].default is None

    def test_empty_table(self):
        schema = table_schema_from_system_columns("`db`.`t`", [])
        assert schema.columns == []


# ── End-to-end: compute_delta + render for ClickHouse ────────────────────────────────────


class TestClickHouseEndToEnd:
    def test_create_table(self):
        desired = TableSchema(
            table_name="`db`.`events`",
            columns=[
                ColumnDef(name="id", type="Int64", nullable=False),
                ColumnDef(name="name", type="String", nullable=True),
            ],
        )
        delta = compute_delta(None, desired, CLICKHOUSE_TYPE_WIDENING_MAP)
        assert delta.is_create is True
        stmts = render_clickhouse_sql(delta)
        assert len(stmts) == 1
        assert "CREATE TABLE" in stmts[0]
        assert "`id` Int64" in stmts[0]
        assert "`name` Nullable(String)" in stmts[0]
        assert "ENGINE = MergeTree()" in stmts[0]

    def test_add_nullable_column(self):
        current = TableSchema(
            table_name="`db`.`t`",
            columns=[
                ColumnDef(name="id", type="Int64", nullable=False),
            ],
        )
        desired = TableSchema(
            table_name="`db`.`t`",
            columns=[
                ColumnDef(name="id", type="Int64", nullable=False),
                ColumnDef(name="email", type="String", nullable=True),
            ],
        )
        delta = compute_delta(
            current, desired, CLICKHOUSE_TYPE_WIDENING_MAP
        )
        assert validate_delta(delta, strict=True) is True
        stmts = render_clickhouse_sql(delta)
        assert len(stmts) == 1
        assert "ADD COLUMN `email` Nullable(String)" in stmts[0]

    def test_add_non_nullable_column_without_default_is_unsafe(self):
        current = TableSchema(
            table_name="`db`.`t`",
            columns=[
                ColumnDef(name="id", type="Int64", nullable=False),
            ],
        )
        desired = TableSchema(
            table_name="`db`.`t`",
            columns=[
                ColumnDef(name="id", type="Int64", nullable=False),
                ColumnDef(name="status", type="String", nullable=False),
            ],
        )
        delta = compute_delta(
            current, desired, CLICKHOUSE_TYPE_WIDENING_MAP
        )
        assert validate_delta(delta) is False

    def test_add_non_nullable_column_with_default_is_safe(self):
        current = TableSchema(
            table_name="`db`.`t`",
            columns=[
                ColumnDef(name="id", type="Int64", nullable=False),
            ],
        )
        desired = TableSchema(
            table_name="`db`.`t`",
            columns=[
                ColumnDef(name="id", type="Int64", nullable=False),
                ColumnDef(
                    name="status",
                    type="String",
                    nullable=False,
                    default="'active'",
                ),
            ],
        )
        delta = compute_delta(
            current, desired, CLICKHOUSE_TYPE_WIDENING_MAP
        )
        assert validate_delta(delta, strict=True) is True

    def test_drop_column(self):
        current = TableSchema(
            table_name="`db`.`t`",
            columns=[
                ColumnDef(name="id", type="Int64", nullable=False),
                ColumnDef(name="old_col", type="String", nullable=True),
            ],
        )
        desired = TableSchema(
            table_name="`db`.`t`",
            columns=[
                ColumnDef(name="id", type="Int64", nullable=False),
            ],
        )
        delta = compute_delta(
            current, desired, CLICKHOUSE_TYPE_WIDENING_MAP
        )
        stmts = render_clickhouse_sql(delta)
        assert len(stmts) == 1
        assert "DROP COLUMN `old_col`" in stmts[0]

    def test_safe_type_widening(self):
        current = TableSchema(
            table_name="`db`.`t`",
            columns=[
                ColumnDef(name="id", type="Int32", nullable=False),
                ColumnDef(name="value", type="Float32", nullable=True),
            ],
        )
        desired = TableSchema(
            table_name="`db`.`t`",
            columns=[
                ColumnDef(name="id", type="Int64", nullable=False),
                ColumnDef(name="value", type="Float64", nullable=True),
            ],
        )
        delta = compute_delta(
            current, desired, CLICKHOUSE_TYPE_WIDENING_MAP
        )
        assert validate_delta(delta, strict=True) is True
        stmts = render_clickhouse_sql(delta)
        assert len(stmts) == 2
        assert "MODIFY COLUMN `id` Int64" in stmts[0]
        assert "MODIFY COLUMN `value` Nullable(Float64)" in stmts[1]

    def test_unsafe_type_narrowing(self):
        current = TableSchema(
            table_name="`db`.`t`",
            columns=[
                ColumnDef(name="id", type="Int64", nullable=False),
            ],
        )
        desired = TableSchema(
            table_name="`db`.`t`",
            columns=[
                ColumnDef(name="id", type="Int32", nullable=False),
            ],
        )
        delta = compute_delta(
            current, desired, CLICKHOUSE_TYPE_WIDENING_MAP
        )
        assert validate_delta(delta) is False

    def test_set_nullable(self):
        current = TableSchema(
            table_name="`db`.`t`",
            columns=[
                ColumnDef(name="name", type="String", nullable=False),
            ],
        )
        desired = TableSchema(
            table_name="`db`.`t`",
            columns=[
                ColumnDef(name="name", type="String", nullable=True),
            ],
        )
        delta = compute_delta(
            current, desired, CLICKHOUSE_TYPE_WIDENING_MAP
        )
        assert validate_delta(delta, strict=True) is True
        stmts = render_clickhouse_sql(delta)
        assert len(stmts) == 1
        assert "MODIFY COLUMN `name` Nullable(String)" in stmts[0]

    def test_drop_nullable_is_unsafe(self):
        current = TableSchema(
            table_name="`db`.`t`",
            columns=[
                ColumnDef(name="name", type="String", nullable=True),
            ],
        )
        desired = TableSchema(
            table_name="`db`.`t`",
            columns=[
                ColumnDef(name="name", type="String", nullable=False),
            ],
        )
        delta = compute_delta(
            current, desired, CLICKHOUSE_TYPE_WIDENING_MAP
        )
        # DROP_NULLABLE is unsafe AND unsupported by ClickHouse renderer
        assert validate_delta(delta) is False
        stmts = render_clickhouse_sql(delta)
        # ClickHouse renderer skips DROP_NULLABLE (unsupported action)
        assert len(stmts) == 0

    def test_set_default(self):
        current = TableSchema(
            table_name="`db`.`t`",
            columns=[
                ColumnDef(name="status", type="String", nullable=False),
            ],
        )
        desired = TableSchema(
            table_name="`db`.`t`",
            columns=[
                ColumnDef(
                    name="status",
                    type="String",
                    nullable=False,
                    default="'active'",
                ),
            ],
        )
        delta = compute_delta(
            current, desired, CLICKHOUSE_TYPE_WIDENING_MAP
        )
        stmts = render_clickhouse_sql(delta)
        assert len(stmts) == 1
        assert "MODIFY COLUMN `status` String DEFAULT 'active'" in stmts[0]

    def test_drop_default(self):
        current = TableSchema(
            table_name="`db`.`t`",
            columns=[
                ColumnDef(
                    name="status",
                    type="String",
                    nullable=False,
                    default="'active'",
                ),
            ],
        )
        desired = TableSchema(
            table_name="`db`.`t`",
            columns=[
                ColumnDef(name="status", type="String", nullable=False),
            ],
        )
        delta = compute_delta(
            current, desired, CLICKHOUSE_TYPE_WIDENING_MAP
        )
        stmts = render_clickhouse_sql(delta)
        assert len(stmts) == 1
        assert "MODIFY COLUMN `status` REMOVE DEFAULT" in stmts[0]

    def test_reorder_ignored_by_default(self):
        current = TableSchema(
            table_name="`db`.`t`",
            columns=[
                ColumnDef(name="a", type="Int32", nullable=False),
                ColumnDef(name="b", type="Int32", nullable=False),
            ],
        )
        desired = TableSchema(
            table_name="`db`.`t`",
            columns=[
                ColumnDef(name="b", type="Int32", nullable=False),
                ColumnDef(name="a", type="Int32", nullable=False),
            ],
        )
        delta = compute_delta(
            current, desired, CLICKHOUSE_TYPE_WIDENING_MAP
        )
        # Default policy: IGNORE
        stmts = render_clickhouse_sql(delta)
        assert len(stmts) == 0

    def test_reorder_applied_when_policy_is_apply(self):
        current = TableSchema(
            table_name="`db`.`t`",
            columns=[
                ColumnDef(name="a", type="Int32", nullable=False),
                ColumnDef(name="b", type="Int32", nullable=False),
                ColumnDef(name="c", type="Int32", nullable=False),
            ],
        )
        desired = TableSchema(
            table_name="`db`.`t`",
            columns=[
                ColumnDef(name="c", type="Int32", nullable=False),
                ColumnDef(name="a", type="Int32", nullable=False),
                ColumnDef(name="b", type="Int32", nullable=False),
            ],
        )
        delta = compute_delta(
            current, desired, CLICKHOUSE_TYPE_WIDENING_MAP
        )
        options = RenderOptions(reorder_policy=ReorderPolicy.APPLY)
        stmts = render_clickhouse_sql(delta, options)
        assert any("FIRST" in s for s in stmts)

    def test_combined_evolution(self):
        """Full scenario: add, drop, widen type, set nullable."""
        current = TableSchema(
            table_name="`db`.`events`",
            columns=[
                ColumnDef(name="id", type="Int32", nullable=False),
                ColumnDef(name="name", type="String", nullable=False),
                ColumnDef(name="value", type="Float32", nullable=True),
                ColumnDef(name="old_col", type="String", nullable=True),
            ],
        )
        desired = TableSchema(
            table_name="`db`.`events`",
            columns=[
                ColumnDef(name="id", type="Int64", nullable=False),
                ColumnDef(name="name", type="String", nullable=True),
                ColumnDef(name="value", type="Float64", nullable=True),
                ColumnDef(
                    name="category",
                    type="String",
                    nullable=True,
                    default="'general'",
                ),
            ],
        )
        delta = compute_delta(
            current, desired, CLICKHOUSE_TYPE_WIDENING_MAP
        )
        assert validate_delta(delta, strict=True) is True

        stmts = render_clickhouse_sql(delta)
        assert len(stmts) == 5

        # DROP old_col
        assert any("DROP COLUMN `old_col`" in s for s in stmts)
        # ADD category
        assert any(
            "ADD COLUMN `category` Nullable(String) DEFAULT 'general'" in s
            for s in stmts
        )
        # ALTER_TYPE id Int32 → Int64
        assert any("MODIFY COLUMN `id` Int64" in s for s in stmts)
        # SET_NULLABLE name
        assert any(
            "MODIFY COLUMN `name` Nullable(String)" in s for s in stmts
        )
        # ALTER_TYPE value Float32 → Float64 (nullable preserved)
        assert any("MODIFY COLUMN `value` Nullable(Float64)" in s for s in stmts)

    def test_no_changes_produces_empty_delta(self):
        schema = TableSchema(
            table_name="`db`.`t`",
            columns=[
                ColumnDef(name="id", type="Int64", nullable=False),
                ColumnDef(name="name", type="String", nullable=True),
            ],
        )
        delta = compute_delta(
            schema, schema, CLICKHOUSE_TYPE_WIDENING_MAP
        )
        assert delta.changes == []
        stmts = render_clickhouse_sql(delta)
        assert stmts == []


# ── table_schema_from_arrow with column_nulls ────────────────────────────────────────


class TestTableSchemaFromArrowColumnNulls:
    """Test that parquet-stats-based nullability overrides Arrow's always-nullable flag."""

    @staticmethod
    def _make_arrow_schema(fields):
        import pyarrow as pa

        return pa.schema(
            [pa.field(name, typ, nullable=nul) for name, typ, nul in fields]
        )

    def test_no_column_nulls_uses_arrow_flag(self):
        """Without column_nulls, Arrow's nullable flag is used (Polars: always True)."""
        from tabsdata.connector.schema.evolution.renderers.clickhouse import (
            table_schema_from_arrow,
        )
        import pyarrow as pa

        schema = self._make_arrow_schema([
            ("id", pa.int64(), True),
            ("name", pa.string(), True),
        ])
        ts = table_schema_from_arrow("t", schema)
        assert ts.columns[0].nullable is True
        assert ts.columns[1].nullable is True

    def test_column_nulls_false_overrides_arrow_nullable(self):
        """Parquet stats say no nulls → column is NOT nullable despite Arrow flag."""
        from tabsdata.connector.schema.evolution.renderers.clickhouse import (
            table_schema_from_arrow,
        )
        import pyarrow as pa

        schema = self._make_arrow_schema([
            ("id", pa.int64(), True),
            ("name", pa.string(), True),
        ])
        column_nulls = {"id": False, "name": False}
        ts = table_schema_from_arrow("t", schema, column_nulls)
        assert ts.columns[0].nullable is False
        assert ts.columns[1].nullable is False

    def test_column_nulls_true_keeps_nullable(self):
        """Parquet stats say has nulls → column IS nullable."""
        from tabsdata.connector.schema.evolution.renderers.clickhouse import (
            table_schema_from_arrow,
        )
        import pyarrow as pa

        schema = self._make_arrow_schema([
            ("id", pa.int64(), True),
            ("name", pa.string(), True),
        ])
        column_nulls = {"id": False, "name": True}
        ts = table_schema_from_arrow("t", schema, column_nulls)
        assert ts.columns[0].nullable is False
        assert ts.columns[1].nullable is True

    def test_column_nulls_none_treated_as_nullable(self):
        """Missing stats (None) → conservatively treated as nullable."""
        from tabsdata.connector.schema.evolution.renderers.clickhouse import (
            table_schema_from_arrow,
        )
        import pyarrow as pa

        schema = self._make_arrow_schema([
            ("id", pa.int64(), True),
        ])
        column_nulls = {"id": None}
        ts = table_schema_from_arrow("t", schema, column_nulls)
        assert ts.columns[0].nullable is True

    def test_column_nulls_mixed(self):
        """Mix of True, False, None across columns."""
        from tabsdata.connector.schema.evolution.renderers.clickhouse import (
            table_schema_from_arrow,
        )
        import pyarrow as pa

        schema = self._make_arrow_schema([
            ("a", pa.int64(), True),
            ("b", pa.string(), True),
            ("c", pa.float64(), True),
        ])
        column_nulls = {"a": False, "b": True, "c": None}
        ts = table_schema_from_arrow("t", schema, column_nulls)
        assert ts.columns[0].nullable is False  # a: no nulls
        assert ts.columns[1].nullable is True   # b: has nulls
        assert ts.columns[2].nullable is True   # c: unknown → nullable

    def test_container_types_never_nullable(self):
        """Array/Map/Struct columns are never nullable in ClickHouse."""
        from tabsdata.connector.schema.evolution.renderers.clickhouse import (
            table_schema_from_arrow,
        )
        import pyarrow as pa

        schema = self._make_arrow_schema([
            ("arr", pa.list_(pa.int32()), True),
            ("m", pa.map_(pa.string(), pa.int32()), True),
        ])
        column_nulls = {"arr": True, "m": True}
        ts = table_schema_from_arrow("t", schema, column_nulls)
        assert ts.columns[0].nullable is False  # container → never nullable
        assert ts.columns[1].nullable is False

    def test_column_not_in_nulls_uses_arrow_flag(self):
        """Column missing from column_nulls falls back to Arrow flag."""
        from tabsdata.connector.schema.evolution.renderers.clickhouse import (
            table_schema_from_arrow,
        )
        import pyarrow as pa

        schema = self._make_arrow_schema([
            ("id", pa.int64(), True),
            ("name", pa.string(), False),
        ])
        column_nulls = {"id": False}  # 'name' not in column_nulls
        ts = table_schema_from_arrow("t", schema, column_nulls)
        assert ts.columns[0].nullable is False  # from column_nulls
        assert ts.columns[1].nullable is False   # from Arrow flag


# ── ALTER_TYPE preserves Nullable ────────────────────────────────────────────────────


class TestAlterTypePreservesNullable:
    """Verify ALTER_TYPE includes Nullable() wrapper when column is nullable."""

    def test_alter_type_nullable_column(self):
        """Widening a Nullable(Int32) → Nullable(Int64) preserves Nullable."""
        current = TableSchema(
            table_name="`db`.`t`",
            columns=[
                ColumnDef(name="v", type="Int32", nullable=True),
            ],
        )
        desired = TableSchema(
            table_name="`db`.`t`",
            columns=[
                ColumnDef(name="v", type="Int64", nullable=True),
            ],
        )
        delta = compute_delta(current, desired, CLICKHOUSE_TYPE_WIDENING_MAP)
        stmts = render_clickhouse_sql(delta)
        assert len(stmts) == 1
        assert "MODIFY COLUMN `v` Nullable(Int64)" in stmts[0]

    def test_alter_type_not_nullable_column(self):
        """Widening a NOT NULL Int32 → Int64 stays NOT NULL."""
        current = TableSchema(
            table_name="`db`.`t`",
            columns=[
                ColumnDef(name="v", type="Int32", nullable=False),
            ],
        )
        desired = TableSchema(
            table_name="`db`.`t`",
            columns=[
                ColumnDef(name="v", type="Int64", nullable=False),
            ],
        )
        delta = compute_delta(current, desired, CLICKHOUSE_TYPE_WIDENING_MAP)
        stmts = render_clickhouse_sql(delta)
        assert len(stmts) == 1
        assert "MODIFY COLUMN `v` Int64" in stmts[0]
        assert "Nullable" not in stmts[0]

    def test_alter_type_and_set_nullable_together(self):
        """Column changes type AND becomes nullable — both changes emitted."""
        current = TableSchema(
            table_name="`db`.`t`",
            columns=[
                ColumnDef(name="v", type="Int32", nullable=False),
            ],
        )
        desired = TableSchema(
            table_name="`db`.`t`",
            columns=[
                ColumnDef(name="v", type="Int64", nullable=True),
            ],
        )
        delta = compute_delta(current, desired, CLICKHOUSE_TYPE_WIDENING_MAP)
        stmts = render_clickhouse_sql(delta)
        # ALTER_TYPE carries nullable=True from desired
        assert any("MODIFY COLUMN `v` Nullable(Int64)" in s for s in stmts)
        # SET_NULLABLE also emitted
        assert any("Nullable(Int64)" in s for s in stmts)

    def test_set_default_on_nullable_column(self):
        """SET_DEFAULT preserves Nullable wrapper."""
        current = TableSchema(
            table_name="`db`.`t`",
            columns=[
                ColumnDef(name="v", type="String", nullable=True),
            ],
        )
        desired = TableSchema(
            table_name="`db`.`t`",
            columns=[
                ColumnDef(
                    name="v", type="String", nullable=True, default="'x'"
                ),
            ],
        )
        delta = compute_delta(current, desired, CLICKHOUSE_TYPE_WIDENING_MAP)
        stmts = render_clickhouse_sql(delta)
        assert len(stmts) == 1
        assert "MODIFY COLUMN `v` Nullable(String) DEFAULT 'x'" in stmts[0]

    def test_set_default_on_not_nullable_column(self):
        """SET_DEFAULT without Nullable wrapper when column is NOT NULL."""
        current = TableSchema(
            table_name="`db`.`t`",
            columns=[
                ColumnDef(name="v", type="String", nullable=False),
            ],
        )
        desired = TableSchema(
            table_name="`db`.`t`",
            columns=[
                ColumnDef(
                    name="v", type="String", nullable=False, default="'x'"
                ),
            ],
        )
        delta = compute_delta(current, desired, CLICKHOUSE_TYPE_WIDENING_MAP)
        stmts = render_clickhouse_sql(delta)
        assert len(stmts) == 1
        assert "MODIFY COLUMN `v` String DEFAULT 'x'" in stmts[0]
        assert "Nullable" not in stmts[0]
