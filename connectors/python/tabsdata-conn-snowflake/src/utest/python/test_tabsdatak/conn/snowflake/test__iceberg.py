#
# Copyright 2026 Tabsdata Inc.
#

from __future__ import annotations

import pytest

# noinspection PyProtectedMember
from tabsdatak.conn.snowflake import _iceberg

pytestmark = pytest.mark.snowflake


def _change(action, column, **detail):
    return _iceberg.SchemaChange(
        action=action, column=column, safe=True, detail=detail
    )


def _delta(*changes, table='"DB"."S"."T"', is_create=False):
    return _iceberg.SchemaDelta(
        table_name=table, is_create=is_create, changes=list(changes)
    )


# ---------------------------------------------------------------------------
# quote_column
#
# Every name reaching the renderer comes back from Snowflake already in the
# case it stores, so quoting is exact rather than case-shifting -- and it is
# the only way to spell a name that is not a legal bare identifier.
# ---------------------------------------------------------------------------


@pytest.mark.parametrize(
    "name,quoted",
    [
        ("ORDER_ID", '"ORDER_ID"'),
        ("order_id", '"order_id"'),
        ("weird name", '"weird name"'),
        ("1_leading_digit", '"1_leading_digit"'),
        ('od"d', '"od""d"'),
    ],
)
def test_quote_column_quotes_and_doubles_embedded_quotes(name, quoted):
    assert _iceberg.quote_column(name) == quoted


# ---------------------------------------------------------------------------
# SnowflakeRenderer -- ADD_COLUMN only
# ---------------------------------------------------------------------------


def test_renderer_supports_add_column_alone():
    assert _iceberg.SnowflakeRenderer.SUPPORTED_ACTIONS == {
        _iceberg.Action.ADD_COLUMN
    }
    assert _iceberg.SnowflakeRenderer.SUPPORTS_REORDER is False


def test_render_alter_adds_a_column_if_not_exists():
    stmt = _iceberg.SnowflakeRenderer().render_alter(
        '"DB"."S"."T"',
        _change(_iceberg.Action.ADD_COLUMN, "AMOUNT", type="FLOAT"),
    )
    assert stmt == (
        'ALTER TABLE "DB"."S"."T" ADD COLUMN IF NOT EXISTS "AMOUNT" FLOAT'
    )


def test_render_alter_keeps_a_parameterized_type_intact():
    stmt = _iceberg.SnowflakeRenderer().render_alter(
        '"T"', _change(_iceberg.Action.ADD_COLUMN, "PRICE", type="NUMBER(10,2)")
    )
    assert stmt.endswith('"PRICE" NUMBER(10,2)')


def test_render_alter_never_emits_not_null_or_default():
    # A column added to a populated table has no value for the existing rows.
    stmt = _iceberg.SnowflakeRenderer().render_alter(
        '"T"',
        _change(
            _iceberg.Action.ADD_COLUMN,
            "AMOUNT",
            type="FLOAT",
            nullable=False,
            default="0",
        ),
    )
    assert "NOT NULL" not in stmt
    assert "DEFAULT" not in stmt


@pytest.mark.parametrize(
    "action",
    [
        _iceberg.Action.DROP_COLUMN,
        _iceberg.Action.ALTER_TYPE,
        _iceberg.Action.SET_NULLABLE,
        _iceberg.Action.DROP_NULLABLE,
        _iceberg.Action.SET_DEFAULT,
        _iceberg.Action.DROP_DEFAULT,
    ],
)
def test_render_alter_returns_none_for_every_other_action(action):
    assert (
        _iceberg.SnowflakeRenderer().render_alter(
            '"T"', _change(action, "AMOUNT", type="FLOAT", new_type="TEXT")
        )
        is None
    )


def test_render_sql_skips_the_actions_the_dialect_does_not_support():
    delta = _delta(
        _change(_iceberg.Action.DROP_COLUMN, "GONE", old_type="TEXT"),
        _change(
            _iceberg.Action.ALTER_TYPE, "WIDENED", old_type="INT", new_type="TEXT"
        ),
        _change(_iceberg.Action.ADD_COLUMN, "FRESH", type="TEXT"),
    )
    assert _iceberg.render_snowflake_sql(delta) == [
        'ALTER TABLE "DB"."S"."T" ADD COLUMN IF NOT EXISTS "FRESH" TEXT'
    ]


def test_render_sql_ignores_a_reorder_by_default():
    delta = _delta(
        _change(
            _iceberg.Action.REORDER_COLUMN,
            "MOVED",
            type="TEXT",
            after_column="FIRST",
        )
    )
    assert _iceberg.render_snowflake_sql(delta) == []


def test_render_sql_fails_a_reorder_when_the_policy_demands_it():
    delta = _delta(
        _change(_iceberg.Action.REORDER_COLUMN, "MOVED", type="TEXT")
    )
    options = _iceberg.RenderOptions(
        reorder_policy=_iceberg.ReorderPolicy.FAIL
    )
    with pytest.raises(ValueError, match="reorder policy is FAIL"):
        _iceberg.render_snowflake_sql(delta, options)


def test_render_sql_refuses_to_apply_a_reorder_the_dialect_cannot_do():
    delta = _delta(
        _change(_iceberg.Action.REORDER_COLUMN, "MOVED", type="TEXT")
    )
    options = _iceberg.RenderOptions(
        reorder_policy=_iceberg.ReorderPolicy.APPLY
    )
    with pytest.raises(ValueError, match="does not support column reordering"):
        _iceberg.render_snowflake_sql(delta, options)


def test_render_sql_preserves_the_order_columns_were_inferred_in():
    delta = _delta(
        _change(_iceberg.Action.ADD_COLUMN, "FIRST", type="TEXT"),
        _change(_iceberg.Action.ADD_COLUMN, "SECOND", type="INT"),
    )
    assert [s.split("EXISTS ")[1] for s in _iceberg.render_snowflake_sql(delta)] == [
        '"FIRST" TEXT',
        '"SECOND" INT',
    ]


# ---------------------------------------------------------------------------
# The create path is closed
#
# A table is created by ``_impl._create_table_from_inferred_schema`` with
# ``USING TEMPLATE (infer_schema(...))``, so Snowflake picks each type from the
# parquet. Rendering column definitions here would fork that decision.
# ---------------------------------------------------------------------------


def test_column_def_is_not_a_path_this_renderer_offers():
    with pytest.raises(NotImplementedError, match="ALTER statements only"):
        _iceberg.SnowflakeRenderer().column_def("C", "TEXT", True, None)


def test_rendering_a_create_delta_raises_rather_than_forking_the_create():
    # The detail carries what ``compute_delta`` puts on a create's ADD_COLUMN,
    # so the create path is reached rather than tripping over a missing key.
    delta = _delta(
        _change(
            _iceberg.Action.ADD_COLUMN,
            "C",
            type="TEXT",
            nullable=True,
            default=None,
            position=0,
        ),
        is_create=True,
    )
    with pytest.raises(NotImplementedError, match="ALTER statements only"):
        _iceberg.render_snowflake_sql(delta)


# ---------------------------------------------------------------------------
# Schema conversion
# ---------------------------------------------------------------------------


def test_table_schema_from_probe_upper_folds_the_current_names():
    # The load matches columns case-insensitively, so the diff has to as well --
    # a target holding a quoted lower-case column must not gain an upper-case
    # duplicate beside it.
    schema = _iceberg.table_schema_from_probe('"T"', ["order_id", "STATUS"])
    assert [c.name for c in schema.columns] == ["ORDER_ID", "STATUS"]
    assert schema.table_name == '"T"'


def test_table_schema_from_probe_leaves_types_empty():
    # Nothing reads them: ALTER_TYPE is not a supported action. A real type read
    # is the prerequisite for ever supporting it.
    schema = _iceberg.table_schema_from_probe('"T"', ["ID"])
    assert [c.type for c in schema.columns] == [""]


def test_table_schema_from_inferred_keeps_names_and_types_verbatim():
    schema = _iceberg.table_schema_from_inferred(
        '"T"', [("ORDER_ID", "NUMBER(38,0)"), ("weird name", "TEXT")]
    )
    assert [(c.name, c.type) for c in schema.columns] == [
        ("ORDER_ID", "NUMBER(38,0)"),
        ("weird name", "TEXT"),
    ]


def test_table_schema_from_inferred_makes_every_column_nullable():
    schema = _iceberg.table_schema_from_inferred('"T"', [("ID", "INT")])
    assert schema.columns[0].nullable is True
    assert schema.columns[0].default is None


def test_widening_map_is_empty_because_no_type_action_is_supported():
    assert _iceberg.SNOWFLAKE_TYPE_WIDENING_MAP == {}
    assert _iceberg.Action.ALTER_TYPE not in (
        _iceberg.SnowflakeRenderer.SUPPORTED_ACTIONS
    )


# ---------------------------------------------------------------------------
# compute_delta through the Snowflake schema builders
# ---------------------------------------------------------------------------


def _added(existing, staged):
    delta = _iceberg.compute_delta(
        _iceberg.table_schema_from_probe('"T"', existing),
        _iceberg.table_schema_from_inferred('"T"', staged),
        _iceberg.SNOWFLAKE_TYPE_WIDENING_MAP,
    )
    return [
        c.column
        for c in delta.changes
        if c.action is _iceberg.Action.ADD_COLUMN
    ]


def test_delta_adds_only_the_columns_the_table_lacks():
    assert _added(["ID"], [("ID", "INT"), ("EXTRA", "TEXT")]) == ["EXTRA"]


def test_delta_adds_nothing_when_the_table_is_already_wider():
    assert _added(["ID", "EXTRA"], [("ID", "INT")]) == []


def test_delta_sees_a_case_difference_as_the_same_column():
    assert _added(["order_id"], [("ORDER_ID", "INT")]) == []


def test_delta_marks_an_added_column_safe():
    delta = _iceberg.compute_delta(
        _iceberg.table_schema_from_probe('"T"', ["ID"]),
        _iceberg.table_schema_from_inferred('"T"', [("ID", "INT"), ("NEW", "TEXT")]),
        _iceberg.SNOWFLAKE_TYPE_WIDENING_MAP,
    )
    added = [c for c in delta.changes if c.action is _iceberg.Action.ADD_COLUMN]
    assert [c.safe for c in added] == [True]
