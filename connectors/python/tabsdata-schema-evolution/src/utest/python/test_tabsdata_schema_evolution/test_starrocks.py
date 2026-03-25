#
# Copyright 2026 Tabs Data Inc.
#

from __future__ import annotations

from tabsdata.connector.schema.evolution.renderers.starrocks import (
    STARROCKS_TYPE_WIDENING_MAP,
    parse_starrocks_type,
    render_starrocks_sql,
    table_schema_from_arrow,
    table_schema_from_information_schema,
)
from tabsdata.connector.schema.evolution.core import compute_delta, validate_delta
from tabsdata.connector.schema.evolution.models import (
    ColumnDef,
    RenderOptions,
    ReorderPolicy,
    TableSchema,
)


# ── parse_starrocks_type ─────────────────────────────────────────────────────────────


class TestParseStarRocksType:
    def test_simple_int(self):
        assert parse_starrocks_type("INT") == ("INT", False)

    def test_simple_bigint(self):
        assert parse_starrocks_type("BIGINT") == ("BIGINT", False)

    def test_simple_string(self):
        assert parse_starrocks_type("STRING") == ("STRING", False)

    def test_simple_boolean(self):
        assert parse_starrocks_type("BOOLEAN") == ("BOOLEAN", False)

    def test_simple_double(self):
        assert parse_starrocks_type("DOUBLE") == ("DOUBLE", False)

    def test_simple_float(self):
        assert parse_starrocks_type("FLOAT") == ("FLOAT", False)

    def test_simple_date(self):
        assert parse_starrocks_type("DATE") == ("DATE", False)

    def test_simple_datetime(self):
        assert parse_starrocks_type("DATETIME") == ("DATETIME", False)

    def test_int_with_display_width(self):
        assert parse_starrocks_type("int(11)") == ("INT", False)

    def test_bigint_with_display_width(self):
        assert parse_starrocks_type("bigint(20)") == ("BIGINT", False)

    def test_tinyint_1_is_boolean(self):
        assert parse_starrocks_type("tinyint(1)") == ("BOOLEAN", False)

    def test_varchar_65533_is_string(self):
        assert parse_starrocks_type("varchar(65533)") == ("STRING", False)

    def test_decimal(self):
        assert parse_starrocks_type("decimal(10, 2)") == ("DECIMAL(10, 2)", False)

    def test_array(self):
        assert parse_starrocks_type("ARRAY<INT>") == ("ARRAY<INT>", False)

    def test_map(self):
        assert parse_starrocks_type("MAP<STRING, INT>") == (
            "MAP<STRING, INT>",
            False,
        )

    def test_whitespace_stripped(self):
        assert parse_starrocks_type("  BIGINT  ") == ("BIGINT", False)

    def test_nullable_always_false(self):
        """parse_starrocks_type always returns False for nullable — nullability
        comes from IS_NULLABLE in information_schema."""
        _, nullable = parse_starrocks_type("INT")
        assert nullable is False


# ── table_schema_from_information_schema ─────────────────────────────────────────────


class TestTableSchemaFromInformationSchema:
    def test_basic_columns_with_mixed_nullability(self):
        rows = [
            ("id", "bigint(20)", "NO", None),
            ("name", "varchar(65533)", "YES", None),
            ("value", "double", "NO", "0.0"),
        ]
        schema = table_schema_from_information_schema("`db`.`t`", rows)
        assert schema.table_name == "`db`.`t`"
        assert len(schema.columns) == 3
        assert schema.columns[0] == ColumnDef(
            name="id", type="BIGINT", nullable=False, default=None
        )
        assert schema.columns[1] == ColumnDef(
            name="name", type="STRING", nullable=True, default=None
        )
        assert schema.columns[2] == ColumnDef(
            name="value", type="DOUBLE", nullable=False, default="0.0"
        )

    def test_columns_with_defaults(self):
        rows = [
            ("status", "varchar(65533)", "NO", "'active'"),
            ("count", "int(11)", "YES", "0"),
        ]
        schema = table_schema_from_information_schema("`db`.`t`", rows)
        assert schema.columns[0].default == "'active'"
        assert schema.columns[1].default == "0"

    def test_empty_table(self):
        schema = table_schema_from_information_schema("`db`.`t`", [])
        assert schema.columns == []


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
        import pyarrow as pa

        schema = self._make_arrow_schema([
            ("id", pa.int64(), True),
            ("name", pa.string(), True),
        ])
        ts = table_schema_from_arrow("t", schema)
        assert ts.columns[0].nullable is True
        assert ts.columns[1].nullable is True

    def test_column_nulls_false_overrides_arrow_nullable(self):
        """Parquet stats say no nulls -> column is NOT nullable despite Arrow flag."""
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
        """Parquet stats say has nulls -> column IS nullable."""
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
        """Missing stats (None) -> conservatively treated as nullable."""
        import pyarrow as pa

        schema = self._make_arrow_schema([
            ("id", pa.int64(), True),
        ])
        column_nulls = {"id": None}
        ts = table_schema_from_arrow("t", schema, column_nulls)
        assert ts.columns[0].nullable is True

    def test_column_nulls_mixed(self):
        """Mix of True, False, None across columns."""
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
        assert ts.columns[2].nullable is True   # c: unknown -> nullable

    def test_container_types_never_nullable(self):
        """Array/Map columns are never nullable in StarRocks."""
        import pyarrow as pa

        schema = self._make_arrow_schema([
            ("arr", pa.list_(pa.int32()), True),
            ("m", pa.map_(pa.string(), pa.int32()), True),
        ])
        column_nulls = {"arr": True, "m": True}
        ts = table_schema_from_arrow("t", schema, column_nulls)
        assert ts.columns[0].nullable is False  # container -> never nullable
        assert ts.columns[1].nullable is False

    def test_column_not_in_nulls_uses_arrow_flag(self):
        """Column missing from column_nulls falls back to Arrow flag."""
        import pyarrow as pa

        schema = self._make_arrow_schema([
            ("id", pa.int64(), True),
            ("name", pa.string(), False),
        ])
        column_nulls = {"id": False}  # 'name' not in column_nulls
        ts = table_schema_from_arrow("t", schema, column_nulls)
        assert ts.columns[0].nullable is False  # from column_nulls
        assert ts.columns[1].nullable is False   # from Arrow flag


# ── End-to-end: compute_delta + render for StarRocks ─────────────────────────────────


class TestStarRocksEndToEnd:
    def test_create_table(self):
        desired = TableSchema(
            table_name="`db`.`events`",
            columns=[
                ColumnDef(name="id", type="BIGINT", nullable=False),
                ColumnDef(name="name", type="STRING", nullable=True),
            ],
        )
        delta = compute_delta(None, desired, STARROCKS_TYPE_WIDENING_MAP)
        assert delta.is_create is True
        stmts = render_starrocks_sql(delta)
        assert len(stmts) == 1
        assert "CREATE TABLE" in stmts[0]
        assert "`id` BIGINT NOT NULL" in stmts[0]
        assert "`name` STRING NULL" in stmts[0]
        # StarRocks _create_table_sql does not include ENGINE
        assert "ENGINE" not in stmts[0]

    def test_add_nullable_column(self):
        current = TableSchema(
            table_name="`db`.`t`",
            columns=[
                ColumnDef(name="id", type="BIGINT", nullable=False),
            ],
        )
        desired = TableSchema(
            table_name="`db`.`t`",
            columns=[
                ColumnDef(name="id", type="BIGINT", nullable=False),
                ColumnDef(name="email", type="STRING", nullable=True),
            ],
        )
        delta = compute_delta(current, desired, STARROCKS_TYPE_WIDENING_MAP)
        assert validate_delta(delta, strict=True) is True
        stmts = render_starrocks_sql(delta)
        assert len(stmts) == 1
        assert "ADD COLUMN `email` STRING NULL" in stmts[0]

    def test_add_non_nullable_column_without_default_is_unsafe(self):
        current = TableSchema(
            table_name="`db`.`t`",
            columns=[
                ColumnDef(name="id", type="BIGINT", nullable=False),
            ],
        )
        desired = TableSchema(
            table_name="`db`.`t`",
            columns=[
                ColumnDef(name="id", type="BIGINT", nullable=False),
                ColumnDef(name="status", type="STRING", nullable=False),
            ],
        )
        delta = compute_delta(current, desired, STARROCKS_TYPE_WIDENING_MAP)
        assert validate_delta(delta) is False

    def test_add_non_nullable_column_with_default_is_safe(self):
        current = TableSchema(
            table_name="`db`.`t`",
            columns=[
                ColumnDef(name="id", type="BIGINT", nullable=False),
            ],
        )
        desired = TableSchema(
            table_name="`db`.`t`",
            columns=[
                ColumnDef(name="id", type="BIGINT", nullable=False),
                ColumnDef(
                    name="status",
                    type="STRING",
                    nullable=False,
                    default="'active'",
                ),
            ],
        )
        delta = compute_delta(current, desired, STARROCKS_TYPE_WIDENING_MAP)
        assert validate_delta(delta, strict=True) is True

    def test_drop_column(self):
        current = TableSchema(
            table_name="`db`.`t`",
            columns=[
                ColumnDef(name="id", type="BIGINT", nullable=False),
                ColumnDef(name="old_col", type="STRING", nullable=True),
            ],
        )
        desired = TableSchema(
            table_name="`db`.`t`",
            columns=[
                ColumnDef(name="id", type="BIGINT", nullable=False),
            ],
        )
        delta = compute_delta(current, desired, STARROCKS_TYPE_WIDENING_MAP)
        stmts = render_starrocks_sql(delta)
        assert len(stmts) == 1
        assert "DROP COLUMN `old_col`" in stmts[0]

    def test_safe_type_widening_tinyint_to_int(self):
        current = TableSchema(
            table_name="`db`.`t`",
            columns=[
                ColumnDef(name="v", type="TINYINT", nullable=False),
            ],
        )
        desired = TableSchema(
            table_name="`db`.`t`",
            columns=[
                ColumnDef(name="v", type="INT", nullable=False),
            ],
        )
        delta = compute_delta(current, desired, STARROCKS_TYPE_WIDENING_MAP)
        assert validate_delta(delta, strict=True) is True
        stmts = render_starrocks_sql(delta)
        assert len(stmts) == 1
        assert "MODIFY COLUMN `v` INT NOT NULL" in stmts[0]

    def test_safe_type_widening_int_to_bigint(self):
        current = TableSchema(
            table_name="`db`.`t`",
            columns=[
                ColumnDef(name="v", type="INT", nullable=False),
            ],
        )
        desired = TableSchema(
            table_name="`db`.`t`",
            columns=[
                ColumnDef(name="v", type="BIGINT", nullable=False),
            ],
        )
        delta = compute_delta(current, desired, STARROCKS_TYPE_WIDENING_MAP)
        assert validate_delta(delta, strict=True) is True
        stmts = render_starrocks_sql(delta)
        assert len(stmts) == 1
        assert "MODIFY COLUMN `v` BIGINT NOT NULL" in stmts[0]

    def test_safe_type_widening_float_to_double(self):
        current = TableSchema(
            table_name="`db`.`t`",
            columns=[
                ColumnDef(name="v", type="FLOAT", nullable=True),
            ],
        )
        desired = TableSchema(
            table_name="`db`.`t`",
            columns=[
                ColumnDef(name="v", type="DOUBLE", nullable=True),
            ],
        )
        delta = compute_delta(current, desired, STARROCKS_TYPE_WIDENING_MAP)
        assert validate_delta(delta, strict=True) is True
        stmts = render_starrocks_sql(delta)
        assert len(stmts) == 1
        assert "MODIFY COLUMN `v` DOUBLE NULL" in stmts[0]

    def test_unsafe_type_narrowing(self):
        current = TableSchema(
            table_name="`db`.`t`",
            columns=[
                ColumnDef(name="v", type="BIGINT", nullable=False),
            ],
        )
        desired = TableSchema(
            table_name="`db`.`t`",
            columns=[
                ColumnDef(name="v", type="INT", nullable=False),
            ],
        )
        delta = compute_delta(current, desired, STARROCKS_TYPE_WIDENING_MAP)
        assert validate_delta(delta) is False

    def test_set_nullable(self):
        current = TableSchema(
            table_name="`db`.`t`",
            columns=[
                ColumnDef(name="name", type="STRING", nullable=False),
            ],
        )
        desired = TableSchema(
            table_name="`db`.`t`",
            columns=[
                ColumnDef(name="name", type="STRING", nullable=True),
            ],
        )
        delta = compute_delta(current, desired, STARROCKS_TYPE_WIDENING_MAP)
        assert validate_delta(delta, strict=True) is True
        stmts = render_starrocks_sql(delta)
        assert len(stmts) == 1
        assert "MODIFY COLUMN `name` STRING NULL" in stmts[0]

    def test_drop_nullable_is_unsafe_and_unsupported(self):
        current = TableSchema(
            table_name="`db`.`t`",
            columns=[
                ColumnDef(name="name", type="STRING", nullable=True),
            ],
        )
        desired = TableSchema(
            table_name="`db`.`t`",
            columns=[
                ColumnDef(name="name", type="STRING", nullable=False),
            ],
        )
        delta = compute_delta(current, desired, STARROCKS_TYPE_WIDENING_MAP)
        # DROP_NULLABLE is unsafe AND unsupported by StarRocks renderer
        assert validate_delta(delta) is False
        stmts = render_starrocks_sql(delta)
        # StarRocks renderer skips DROP_NULLABLE (unsupported action)
        assert len(stmts) == 0

    def test_set_default(self):
        current = TableSchema(
            table_name="`db`.`t`",
            columns=[
                ColumnDef(name="status", type="STRING", nullable=False),
            ],
        )
        desired = TableSchema(
            table_name="`db`.`t`",
            columns=[
                ColumnDef(
                    name="status",
                    type="STRING",
                    nullable=False,
                    default="'active'",
                ),
            ],
        )
        delta = compute_delta(current, desired, STARROCKS_TYPE_WIDENING_MAP)
        stmts = render_starrocks_sql(delta)
        assert len(stmts) == 1
        assert (
            "MODIFY COLUMN `status` STRING NOT NULL DEFAULT 'active'" in stmts[0]
        )

    def test_drop_default(self):
        current = TableSchema(
            table_name="`db`.`t`",
            columns=[
                ColumnDef(
                    name="status",
                    type="STRING",
                    nullable=False,
                    default="'active'",
                ),
            ],
        )
        desired = TableSchema(
            table_name="`db`.`t`",
            columns=[
                ColumnDef(name="status", type="STRING", nullable=False),
            ],
        )
        delta = compute_delta(current, desired, STARROCKS_TYPE_WIDENING_MAP)
        stmts = render_starrocks_sql(delta)
        assert len(stmts) == 1
        # StarRocks DROP_DEFAULT restates type without DEFAULT (no REMOVE DEFAULT)
        assert "MODIFY COLUMN `status` STRING NOT NULL" in stmts[0]
        assert "DEFAULT" not in stmts[0]

    def test_combined_evolution(self):
        """Full scenario: add, drop, widen type, set nullable."""
        current = TableSchema(
            table_name="`db`.`events`",
            columns=[
                ColumnDef(name="id", type="INT", nullable=False),
                ColumnDef(name="name", type="STRING", nullable=False),
                ColumnDef(name="value", type="FLOAT", nullable=True),
                ColumnDef(name="old_col", type="STRING", nullable=True),
            ],
        )
        desired = TableSchema(
            table_name="`db`.`events`",
            columns=[
                ColumnDef(name="id", type="BIGINT", nullable=False),
                ColumnDef(name="name", type="STRING", nullable=True),
                ColumnDef(name="value", type="DOUBLE", nullable=True),
                ColumnDef(
                    name="category",
                    type="STRING",
                    nullable=True,
                    default="'general'",
                ),
            ],
        )
        delta = compute_delta(
            current, desired, STARROCKS_TYPE_WIDENING_MAP
        )
        assert validate_delta(delta, strict=True) is True

        stmts = render_starrocks_sql(delta)
        assert len(stmts) == 5

        # DROP old_col
        assert any("DROP COLUMN `old_col`" in s for s in stmts)
        # ADD category
        assert any(
            "ADD COLUMN `category` STRING NULL DEFAULT 'general'" in s
            for s in stmts
        )
        # ALTER_TYPE id INT -> BIGINT
        assert any("MODIFY COLUMN `id` BIGINT NOT NULL" in s for s in stmts)
        # SET_NULLABLE name
        assert any("MODIFY COLUMN `name` STRING NULL" in s for s in stmts)
        # ALTER_TYPE value FLOAT -> DOUBLE (nullable preserved)
        assert any("MODIFY COLUMN `value` DOUBLE NULL" in s for s in stmts)

    def test_no_changes_produces_empty_delta(self):
        schema = TableSchema(
            table_name="`db`.`t`",
            columns=[
                ColumnDef(name="id", type="BIGINT", nullable=False),
                ColumnDef(name="name", type="STRING", nullable=True),
            ],
        )
        delta = compute_delta(schema, schema, STARROCKS_TYPE_WIDENING_MAP)
        assert delta.changes == []
        stmts = render_starrocks_sql(delta)
        assert stmts == []


# ── ALTER_TYPE preserves NULL/NOT NULL ───────────────────────────────────────────────


class TestAlterTypePreservesNullable:
    """Verify ALTER_TYPE includes NULL/NOT NULL suffix when column nullability varies."""

    def test_alter_type_nullable_column(self):
        """Widening a nullable TINYINT -> INT preserves NULL."""
        current = TableSchema(
            table_name="`db`.`t`",
            columns=[
                ColumnDef(name="v", type="TINYINT", nullable=True),
            ],
        )
        desired = TableSchema(
            table_name="`db`.`t`",
            columns=[
                ColumnDef(name="v", type="INT", nullable=True),
            ],
        )
        delta = compute_delta(current, desired, STARROCKS_TYPE_WIDENING_MAP)
        stmts = render_starrocks_sql(delta)
        assert len(stmts) == 1
        assert "MODIFY COLUMN `v` INT NULL" in stmts[0]

    def test_alter_type_not_nullable_column(self):
        """Widening a NOT NULL INT -> BIGINT stays NOT NULL."""
        current = TableSchema(
            table_name="`db`.`t`",
            columns=[
                ColumnDef(name="v", type="INT", nullable=False),
            ],
        )
        desired = TableSchema(
            table_name="`db`.`t`",
            columns=[
                ColumnDef(name="v", type="BIGINT", nullable=False),
            ],
        )
        delta = compute_delta(current, desired, STARROCKS_TYPE_WIDENING_MAP)
        stmts = render_starrocks_sql(delta)
        assert len(stmts) == 1
        assert "MODIFY COLUMN `v` BIGINT NOT NULL" in stmts[0]
        assert "NULL" in stmts[0]  # NOT NULL contains NULL substring

    def test_alter_type_and_set_nullable_together(self):
        """Column changes type AND becomes nullable -- both changes emitted."""
        current = TableSchema(
            table_name="`db`.`t`",
            columns=[
                ColumnDef(name="v", type="INT", nullable=False),
            ],
        )
        desired = TableSchema(
            table_name="`db`.`t`",
            columns=[
                ColumnDef(name="v", type="BIGINT", nullable=True),
            ],
        )
        delta = compute_delta(current, desired, STARROCKS_TYPE_WIDENING_MAP)
        stmts = render_starrocks_sql(delta)
        # ALTER_TYPE carries nullable from desired, SET_NULLABLE also emitted
        assert any("MODIFY COLUMN `v` BIGINT NULL" in s for s in stmts)

    def test_set_default_on_nullable_column(self):
        """SET_DEFAULT preserves NULL suffix."""
        current = TableSchema(
            table_name="`db`.`t`",
            columns=[
                ColumnDef(name="v", type="STRING", nullable=True),
            ],
        )
        desired = TableSchema(
            table_name="`db`.`t`",
            columns=[
                ColumnDef(
                    name="v", type="STRING", nullable=True, default="'x'"
                ),
            ],
        )
        delta = compute_delta(current, desired, STARROCKS_TYPE_WIDENING_MAP)
        stmts = render_starrocks_sql(delta)
        assert len(stmts) == 1
        assert "MODIFY COLUMN `v` STRING NULL DEFAULT 'x'" in stmts[0]

    def test_set_default_on_not_nullable_column(self):
        """SET_DEFAULT preserves NOT NULL suffix."""
        current = TableSchema(
            table_name="`db`.`t`",
            columns=[
                ColumnDef(name="v", type="STRING", nullable=False),
            ],
        )
        desired = TableSchema(
            table_name="`db`.`t`",
            columns=[
                ColumnDef(
                    name="v", type="STRING", nullable=False, default="'x'"
                ),
            ],
        )
        delta = compute_delta(current, desired, STARROCKS_TYPE_WIDENING_MAP)
        stmts = render_starrocks_sql(delta)
        assert len(stmts) == 1
        assert "MODIFY COLUMN `v` STRING NOT NULL DEFAULT 'x'" in stmts[0]
        assert "NULL DEFAULT" in stmts[0]
