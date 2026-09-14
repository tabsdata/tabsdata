#
# Copyright 2026 Tabsdata Inc.
#

from __future__ import annotations

import pytest

# noinspection PyProtectedMember
from tabsdatak.conn.bigquery import _iceberg

pytestmark = pytest.mark.bigquery


def _change(action, column, **detail):
    return _iceberg.SchemaChange(
        action=action, column=column, safe=True, detail=detail
    )


def _delta(*changes, table="`p.ds.t`", is_create=False):
    return _iceberg.SchemaDelta(
        table_name=table, is_create=is_create, changes=list(changes)
    )


# ---------------------------------------------------------------------------
# quote_column
#
# Backticks are BigQuery's escape, not a case-sensitivity marker, so quoting
# is safe unconditionally -- and it is the only way to spell a flexible column
# name or one that collides with a reserved word.
# ---------------------------------------------------------------------------


@pytest.mark.parametrize(
    "name,quoted",
    [
        ("order_id", "`order_id`"),
        ("ORDER_ID", "`ORDER_ID`"),
        ("weird name", "`weird name`"),
        ("order", "`order`"),
        ("select", "`select`"),
        ("ünïcode", "`ünïcode`"),
    ],
)
def test_quote_column_backticks_a_name(name, quoted):
    assert _iceberg.quote_column(name) == quoted


def test_quote_column_escapes_an_embedded_backtick():
    assert _iceberg.quote_column("od`d") == "`od\\`d`"


def test_quote_column_leaves_an_already_quoted_name_alone():
    assert _iceberg.quote_column("`order_id`") == "`order_id`"


# ---------------------------------------------------------------------------
# BigQueryRenderer -- ADD_COLUMN only
# ---------------------------------------------------------------------------


def test_renderer_supports_adding_and_widening_alone():
    assert _iceberg.BigQueryRenderer.SUPPORTED_ACTIONS == {
        _iceberg.Action.ADD_COLUMN,
        _iceberg.Action.ALTER_TYPE,
    }
    assert _iceberg.BigQueryRenderer.SUPPORTS_REORDER is False


def test_render_alter_adds_a_column_if_not_exists():
    stmt = _iceberg.BigQueryRenderer().render_alter(
        "`p.ds.t`",
        _change(_iceberg.Action.ADD_COLUMN, "note", type="STRING"),
    )
    assert stmt == "ALTER TABLE `p.ds.t` ADD COLUMN IF NOT EXISTS `note` STRING"


def test_render_alter_keeps_a_compound_type_intact():
    stmt = _iceberg.BigQueryRenderer().render_alter(
        "`p.ds.t`",
        _change(_iceberg.Action.ADD_COLUMN, "tags", type="ARRAY<STRING>"),
    )
    assert stmt.endswith("`tags` ARRAY<STRING>")


def test_render_alter_never_emits_not_null_or_default():
    """BigQuery refuses a REQUIRED column on a populated table, and the rows
    already there have no value for a column being added."""
    stmt = _iceberg.BigQueryRenderer().render_alter(
        "`p.ds.t`",
        _change(
            _iceberg.Action.ADD_COLUMN,
            "note",
            type="STRING",
            nullable=False,
            default="'x'",
        ),
    )
    assert "NOT NULL" not in stmt
    assert "DEFAULT" not in stmt


@pytest.mark.parametrize(
    "action",
    [
        _iceberg.Action.DROP_COLUMN,
        _iceberg.Action.SET_NULLABLE,
        _iceberg.Action.DROP_NULLABLE,
        _iceberg.Action.SET_DEFAULT,
        _iceberg.Action.DROP_DEFAULT,
        _iceberg.Action.REORDER_COLUMN,
    ],
)
def test_render_alter_returns_none_for_every_other_action(action):
    assert (
        _iceberg.BigQueryRenderer().render_alter("`p.ds.t`", _change(action, "note"))
        is None
    )


def test_render_sql_skips_the_actions_the_dialect_does_not_support():
    delta = _delta(
        _change(_iceberg.Action.DROP_COLUMN, "gone"),
        _change(_iceberg.Action.ADD_COLUMN, "note", type="STRING"),
        _change(_iceberg.Action.SET_DEFAULT, "total", default="0"),
    )
    assert _iceberg.render_bigquery_sql(delta) == [
        "ALTER TABLE `p.ds.t` ADD COLUMN IF NOT EXISTS `note` STRING"
    ]


def test_render_sql_ignores_a_reorder_by_default():
    delta = _delta(_change(_iceberg.Action.REORDER_COLUMN, "note"))
    assert _iceberg.render_bigquery_sql(delta) == []


def test_render_sql_fails_a_reorder_when_the_policy_demands_it():
    delta = _delta(_change(_iceberg.Action.REORDER_COLUMN, "note"))
    options = _iceberg.RenderOptions(reorder_policy=_iceberg.ReorderPolicy.FAIL)
    with pytest.raises(ValueError, match="reorder policy is FAIL"):
        _iceberg.render_bigquery_sql(delta, options)


def test_render_sql_refuses_to_apply_a_reorder_the_dialect_cannot_do():
    delta = _delta(_change(_iceberg.Action.REORDER_COLUMN, "note"))
    options = _iceberg.RenderOptions(reorder_policy=_iceberg.ReorderPolicy.APPLY)
    with pytest.raises(ValueError, match="does not support column reordering"):
        _iceberg.render_bigquery_sql(delta, options)


def test_render_sql_preserves_the_order_the_columns_arrived_in():
    delta = _delta(
        _change(_iceberg.Action.ADD_COLUMN, "b", type="STRING"),
        _change(_iceberg.Action.ADD_COLUMN, "a", type="INT64"),
    )
    assert [stmt.split("EXISTS ")[1] for stmt in _iceberg.render_bigquery_sql(delta)] == [
        "`b` STRING",
        "`a` INT64",
    ]


# ---------------------------------------------------------------------------
# CREATE path -- the target a merge has to have before it can run
# ---------------------------------------------------------------------------


def test_column_def_names_and_types_a_column():
    assert (
        _iceberg.BigQueryRenderer().column_def("note", "STRING", True, None)
        == "`note` STRING"
    )


def test_column_def_spells_default_before_not_null():
    """BigQuery's column_schema takes DEFAULT ahead of NOT NULL, the reverse of
    most dialects."""
    assert (
        _iceberg.BigQueryRenderer().column_def("n", "INT64", False, "0")
        == "`n` INT64 DEFAULT 0 NOT NULL"
    )


def test_column_def_omits_what_it_was_not_given():
    renderer = _iceberg.BigQueryRenderer()
    assert renderer.column_def("n", "INT64", False, None) == "`n` INT64 NOT NULL"
    assert renderer.column_def("n", "INT64", True, "0") == "`n` INT64 DEFAULT 0"


def test_rendering_a_create_delta_emits_one_create_table():
    delta = _delta(
        _change(
            _iceberg.Action.ADD_COLUMN,
            "order_id",
            type="INT64",
            nullable=True,
            position=0,
        ),
        _change(
            _iceberg.Action.ADD_COLUMN,
            "tags",
            type="ARRAY<STRING>",
            nullable=True,
            position=1,
        ),
        is_create=True,
    )
    assert _iceberg.render_bigquery_sql(delta) == [
        "CREATE TABLE IF NOT EXISTS `p.ds.t` (\n"
        "    `order_id` INT64,\n"
        "    `tags` ARRAY<STRING>\n"
        ")"
    ]


def test_create_is_idempotent_so_the_caller_need_not_probe_first():
    delta = _delta(
        _change(
            _iceberg.Action.ADD_COLUMN, "a", type="INT64", nullable=True, position=0
        ),
        is_create=True,
    )
    (statement,) = _iceberg.render_bigquery_sql(delta)
    assert statement.startswith("CREATE TABLE IF NOT EXISTS")


def test_create_orders_columns_by_position_not_by_delta_order():
    delta = _delta(
        _change(
            _iceberg.Action.ADD_COLUMN, "b", type="STRING", nullable=True, position=1
        ),
        _change(
            _iceberg.Action.ADD_COLUMN, "a", type="INT64", nullable=True, position=0
        ),
        is_create=True,
    )
    (statement,) = _iceberg.render_bigquery_sql(delta)
    assert statement.index("`a` INT64") < statement.index("`b` STRING")


# ---------------------------------------------------------------------------
# Schema conversion
# ---------------------------------------------------------------------------


def test_table_schema_from_columns_keeps_names_and_types_verbatim():
    schema = _iceberg.table_schema_from_columns(
        "`p.ds.t`", [("order_id", "INT64"), ("tags", "ARRAY<STRING>")]
    )
    assert [(col.name, col.type) for col in schema.columns] == [
        ("order_id", "INT64"),
        ("tags", "ARRAY<STRING>"),
    ]


def test_table_schema_from_columns_makes_every_column_nullable():
    schema = _iceberg.table_schema_from_columns("`p.ds.t`", [("note", "STRING")])
    assert all(col.nullable for col in schema.columns)
    assert all(col.default is None for col in schema.columns)


def test_widening_map_is_keyed_by_the_type_being_widened_to():
    """An entry names the types that can safely become the key, so nothing
    widens *to* the narrowest one."""
    widenings = _iceberg.BIGQUERY_TYPE_WIDENING_MAP
    assert "INT64" not in widenings
    assert widenings["NUMERIC"] == {"INT64"}
    assert "NUMERIC" in widenings["BIGNUMERIC"]


def test_widening_map_feeds_the_safety_flag_the_caller_gates_on():
    """The map marks a retype safe; acting on that is the caller's call, since
    the renderer renders whatever it is handed."""
    assert _iceberg.Action.ALTER_TYPE in _iceberg.BigQueryRenderer.SUPPORTED_ACTIONS
    current = _iceberg.table_schema_from_columns("`p.ds.t`", [("n", "INT64")])
    desired = _iceberg.table_schema_from_columns("`p.ds.t`", [("n", "NUMERIC")])
    delta = _iceberg.compute_delta(
        current, desired, _iceberg.BIGQUERY_TYPE_WIDENING_MAP
    )
    (alter,) = [c for c in delta.changes if c.action is _iceberg.Action.ALTER_TYPE]
    assert alter.safe is True


def test_widening_map_leaves_parameterized_types_to_can_widen():
    """``_impl.column_type`` spells a type with its parameters, which these keys
    do not carry -- so the map alone would call a widening unsafe."""
    assert "NUMERIC(20, 4)" not in _iceberg.BIGQUERY_TYPE_WIDENING_MAP
    assert _iceberg.can_widen("NUMERIC(9, 2)", "NUMERIC(20, 4)") is True


# ---------------------------------------------------------------------------
# compute_delta through the BigQuery schemas
# ---------------------------------------------------------------------------


def _added(existing, staged):
    delta = _iceberg.compute_delta(
        _iceberg.table_schema_from_columns("`p.ds.t`", existing),
        _iceberg.table_schema_from_columns("`p.ds.t`", staged),
        _iceberg.BIGQUERY_TYPE_WIDENING_MAP,
    )
    return [
        change.column
        for change in delta.changes
        if change.action is _iceberg.Action.ADD_COLUMN
    ]


def test_delta_adds_only_the_columns_the_table_lacks():
    added = _added(
        [("order_id", "INT64")], [("order_id", "INT64"), ("note", "STRING")]
    )
    assert added == ["note"]


def test_delta_adds_nothing_when_the_table_is_already_wider():
    assert (
        _added(
            [("order_id", "INT64"), ("note", "STRING")], [("order_id", "INT64")]
        )
        == []
    )


def test_delta_marks_an_added_column_safe():
    delta = _iceberg.compute_delta(
        _iceberg.table_schema_from_columns("`p.ds.t`", [("order_id", "INT64")]),
        _iceberg.table_schema_from_columns("`p.ds.t`", [("note", "STRING")]),
        _iceberg.BIGQUERY_TYPE_WIDENING_MAP,
    )
    added = [c for c in delta.changes if c.action is _iceberg.Action.ADD_COLUMN]
    assert [c.safe for c in added] == [True]
