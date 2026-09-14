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
# SnowflakeRenderer -- ADD_COLUMN and ALTER_TYPE
# ---------------------------------------------------------------------------


def test_renderer_supports_the_two_widening_actions():
    assert _iceberg.SnowflakeRenderer.SUPPORTED_ACTIONS == {
        _iceberg.Action.ADD_COLUMN,
        _iceberg.Action.ALTER_TYPE,
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


def test_render_alter_sets_the_new_data_type():
    stmt = _iceberg.SnowflakeRenderer().render_alter(
        '"DB"."S"."T"',
        _change(
            _iceberg.Action.ALTER_TYPE,
            "AMOUNT",
            old_type="NUMBER(10,2)",
            new_type="NUMBER(20,2)",
        ),
    )
    assert stmt == (
        'ALTER TABLE "DB"."S"."T" ALTER COLUMN "AMOUNT"'
        " SET DATA TYPE NUMBER(20,2)"
    )


def test_render_alter_type_quotes_a_special_char_column():
    stmt = _iceberg.SnowflakeRenderer().render_alter(
        '"T"',
        _change(
            _iceberg.Action.ALTER_TYPE,
            'od"d name',
            old_type="VARCHAR(10)",
            new_type="VARCHAR(20)",
        ),
    )
    assert stmt == (
        'ALTER TABLE "T" ALTER COLUMN "od""d name" SET DATA TYPE VARCHAR(20)'
    )


def test_render_alter_type_carries_no_if_exists():
    # Snowflake's ALTER COLUMN grammar has no per-column IF EXISTS, so the
    # statement must not borrow the ADD's.
    stmt = _iceberg.SnowflakeRenderer().render_alter(
        '"T"', _change(_iceberg.Action.ALTER_TYPE, "AMOUNT", new_type="FLOAT")
    )
    assert "IF EXISTS" not in stmt


def test_render_alter_type_renders_a_change_marked_unsafe():
    # A renderer renders; whether Snowflake will make the conversion is
    # ``safe``, which the caller gates on before it gets here.
    change = _iceberg.SchemaChange(
        action=_iceberg.Action.ALTER_TYPE,
        column="AMOUNT",
        safe=False,
        detail={"old_type": "NUMBER(38,0)", "new_type": "VARCHAR(16777216)"},
    )
    stmt = _iceberg.SnowflakeRenderer().render_alter('"T"', change)
    assert stmt.endswith("SET DATA TYPE VARCHAR(16777216)")


@pytest.mark.parametrize(
    "action",
    [
        _iceberg.Action.DROP_COLUMN,
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
            _iceberg.Action.ALTER_TYPE,
            "WIDENED",
            old_type="VARCHAR(10)",
            new_type="VARCHAR(20)",
        ),
        _change(_iceberg.Action.ADD_COLUMN, "FRESH", type="TEXT"),
    )
    assert _iceberg.render_snowflake_sql(delta) == [
        'ALTER TABLE "DB"."S"."T" ALTER COLUMN "WIDENED" SET DATA TYPE VARCHAR(20)',
        'ALTER TABLE "DB"."S"."T" ADD COLUMN IF NOT EXISTS "FRESH" TEXT',
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
    schema = _iceberg.table_schema_from_probe(
        '"T"', [("order_id", "NUMBER(38,0)"), ("STATUS", "VARCHAR(16777216)")]
    )
    assert [c.name for c in schema.columns] == ["ORDER_ID", "STATUS"]
    assert schema.table_name == '"T"'


def test_table_schema_from_probe_carries_the_reported_types_verbatim():
    # The type is the spelling a statement would use for that column, so it is
    # taken as reported -- parameters and all -- and never folded.
    schema = _iceberg.table_schema_from_probe(
        '"T"', [("ID", "NUMBER(38,0)"), ("NAME", "VARCHAR(16777216)")]
    )
    assert [c.type for c in schema.columns] == ["NUMBER(38,0)", "VARCHAR(16777216)"]


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


def test_widening_map_is_empty_because_no_base_type_reaches_another():
    # Snowflake has no cross-type widening at all: what it allows is a wider
    # parameterization of the same base, which ``can_widen`` answers for and a
    # map keyed by base cannot express.
    assert _iceberg.SNOWFLAKE_TYPE_WIDENING_MAP == {}
    assert _iceberg.can_widen("VARCHAR(10)", "VARCHAR(20)") is True
    assert _iceberg.can_widen("NUMBER(38,0)", "VARCHAR(16777216)") is False


# ---------------------------------------------------------------------------
# can_widen
#
# The safety oracle ``_impl._evolve_target_columns`` hands to ``compute_delta``,
# and the only thing standing between a reported retype and an ALTER COLUMN.
# Snowflake widens a column two ways -- a longer text, a higher NUMBER precision
# at the same scale -- and everything else is refused, including the no-op that
# the two type vocabularies produce for a column nobody touched.
# ---------------------------------------------------------------------------


@pytest.mark.parametrize(
    "old,new",
    [
        ("VARCHAR(10)", "VARCHAR(20)"),
        # An alias resolves to what it stands for, so a target column reaches
        # full width by being retyped to a name that carries no parameters.
        ("VARCHAR(10)", "TEXT"),
        ("VARCHAR(10)", "STRING"),
        # A bare CHAR is one character wide, not the maximum.
        ("CHAR", "VARCHAR(10)"),
        ("CHAR(5)", "CHAR(10)"),
        ("NUMBER(10,2)", "NUMBER(20,2)"),
        ("NUMBER(10,0)", "NUMBER(38,0)"),
        ("NUMBER(10,0)", "INT"),
        ("DECIMAL(10,2)", "NUMBER(20,2)"),
        # Case and whitespace are the spelling, not the type.
        ("varchar(10)", "VARCHAR(20)"),
        ("NUMBER(10, 2)", "NUMBER( 20 , 2 )"),
    ],
)
def test_can_widen_allows_a_wider_parameterization_of_one_base(old, new):
    assert _iceberg.can_widen(old, new) is True


@pytest.mark.parametrize(
    "old,new",
    [
        # Identical, and the same type under two names -- the case the write path
        # meets on every merge, since DESC TABLE and infer_schema disagree on
        # how to spell an unparameterized column.
        ("VARCHAR(10)", "VARCHAR(10)"),
        ("VARCHAR(16777216)", "TEXT"),
        ("TEXT", "VARCHAR(16777216)"),
        ("NUMBER(38,0)", "INT"),
        ("INTEGER", "NUMBER(38,0)"),
        ("DECIMAL(10,2)", "NUMBER(10,2)"),
        # Narrowing.
        ("VARCHAR(20)", "VARCHAR(10)"),
        ("NUMBER(20,2)", "NUMBER(10,2)"),
        ("TEXT", "VARCHAR(10)"),
        # Precision may grow; scale may not move, in either direction.
        ("NUMBER(10,2)", "NUMBER(20,4)"),
        ("NUMBER(10,2)", "NUMBER(20,0)"),
        # No base type is reachable from another.
        ("NUMBER(38,0)", "VARCHAR(16777216)"),
        ("VARCHAR(10)", "NUMBER(38,0)"),
        ("NUMBER(38,0)", "FLOAT"),
        ("FLOAT", "NUMBER(38,0)"),
        ("DATE", "TIMESTAMP_NTZ(9)"),
        ("BOOLEAN", "VARCHAR(10)"),
    ],
)
def test_can_widen_refuses_everything_else(old, new):
    assert _iceberg.can_widen(old, new) is False


@pytest.mark.parametrize(
    "old,new",
    [
        ("TIMESTAMP_NTZ(6)", "TIMESTAMP_NTZ(9)"),
        ("BINARY(10)", "BINARY(20)"),
    ],
)
def test_can_widen_refuses_a_base_it_does_not_list_as_alterable(old, new):
    # Only NUMBER and VARCHAR are listed, so a parameter that grows on any other
    # base is refused. Conservative by construction: refusing a change Snowflake
    # would have made costs a widening, admitting one it will not costs the write.
    assert _iceberg.can_widen(old, new) is False


def test_can_widen_reads_a_bare_varchar_as_already_at_full_width():
    # An unparameterized VARCHAR is VARCHAR(16777216), so nothing widens it and
    # every length is a narrowing of it.
    assert _iceberg.can_widen("VARCHAR", "VARCHAR(10)") is False
    assert _iceberg.can_widen("VARCHAR(10)", "VARCHAR") is True


@pytest.mark.parametrize(
    "old,new",
    [
        ("VARCHAR(10)", "VARCHAR(20)"),
        ("NUMBER(10,2)", "NUMBER(20,2)"),
        ("CHAR(5)", "VARCHAR(4000)"),
    ],
)
def test_can_widen_is_one_way(old, new):
    # Widening is directional: a pair that passes one way must fail the other, or
    # the write path could talk itself into narrowing a column.
    assert _iceberg.can_widen(old, new) is True
    assert _iceberg.can_widen(new, old) is False


@pytest.mark.parametrize(
    "type_",
    [
        # A structured type: the parameter list is not a number, or a pair of
        # them, so the parse gives up rather than guessing.
        "ARRAY(NUMBER(38,0))",
        "OBJECT(a VARCHAR)",
        "VECTOR(FLOAT, 3)",
        # Ill-formed for the base named.
        "VARCHAR(10,2)",
        "NUMBER(2,5)",
        "",
    ],
)
def test_can_widen_raises_on_a_type_it_cannot_parse(type_):
    # Documents what reaches the caller today: ``compute_delta`` calls this for
    # every column whose reported type changed, so an unparseable type on either
    # side is raised rather than answered.
    with pytest.raises(_iceberg.ParseError):
        _iceberg.can_widen(type_, "VARCHAR(20)")
    with pytest.raises(_iceberg.ParseError):
        _iceberg.can_widen("VARCHAR(10)", type_)


# ---------------------------------------------------------------------------
# same_type
#
# What keeps the two vocabularies from reading as schema drift: the target's
# ``DESC TABLE`` spelling against the staged file's ``infer_schema`` spelling.
# ---------------------------------------------------------------------------


@pytest.mark.parametrize(
    "old,new",
    [
        ("VARCHAR(16777216)", "TEXT"),
        ("TEXT", "VARCHAR(16777216)"),
        ("VARCHAR(16777216)", "STRING"),
        ("NUMBER(38,0)", "INT"),
        ("NUMBER(38,0)", "INTEGER"),
        ("NUMBER(10,2)", "DECIMAL(10,2)"),
        ("CHAR", "VARCHAR(1)"),
        ("varchar(10)", "VARCHAR(10)"),
        ("NUMBER(10, 2)", "NUMBER(10,2)"),
        ("FLOAT", "FLOAT"),
    ],
)
def test_same_type_reads_two_spellings_of_one_type_as_one(old, new):
    assert _iceberg.same_type(old, new) is True


@pytest.mark.parametrize(
    "old,new",
    [
        ("VARCHAR(10)", "VARCHAR(20)"),
        ("VARCHAR(10)", "TEXT"),
        ("NUMBER(10,2)", "NUMBER(10,0)"),
        ("NUMBER(10,2)", "NUMBER(20,2)"),
        ("DATE", "TIMESTAMP_NTZ(9)"),
        ("TIMESTAMP_NTZ(6)", "TIMESTAMP_NTZ(9)"),
        ("NUMBER(38,0)", "FLOAT"),
    ],
)
def test_same_type_keeps_two_real_types_apart(old, new):
    assert _iceberg.same_type(old, new) is False


@pytest.mark.parametrize(
    "type_",
    ["ARRAY(NUMBER(38,0))", "OBJECT(a VARCHAR)", "VECTOR(FLOAT, 3)", ""],
)
def test_same_type_answers_false_for_a_type_it_cannot_parse(type_):
    # Unlike ``can_widen`` this one answers rather than raising: it is asked
    # whether two spellings are the same type, and "cannot tell" has to read as
    # "not the same" -- the change is then reported and judged on its own.
    assert _iceberg.same_type(type_, "VARCHAR(20)") is False
    assert _iceberg.same_type("VARCHAR(10)", type_) is False


def test_same_type_holds_for_an_identical_unparseable_spelling():
    # Two identical strings never reach here from ``_aligned_type`` -- the delta
    # reports no change for them at all -- so answering False costs nothing.
    assert _iceberg.same_type("ARRAY(NUMBER(38,0))", "ARRAY(NUMBER(38,0))") is False


# ---------------------------------------------------------------------------
# same_unalterable_base
#
# A difference that is real and that nothing can act on: ALTER COLUMN reaches
# the parameters of NUMBER and VARCHAR alone.
# ---------------------------------------------------------------------------


@pytest.mark.parametrize(
    "old,new",
    [
        ("TIMESTAMP_NTZ(9)", "TIMESTAMP_NTZ(6)"),
        ("TIMESTAMP_NTZ(6)", "TIMESTAMP_NTZ(9)"),
        ("TIMESTAMP_LTZ(9)", "TIMESTAMP_LTZ(3)"),
        ("BINARY(10)", "BINARY(20)"),
        ("TIME(3)", "TIME(9)"),
    ],
)
def test_same_unalterable_base_holds_for_one_base_that_cannot_be_altered(old, new):
    assert _iceberg.same_unalterable_base(old, new) is True


@pytest.mark.parametrize(
    "old,new",
    [
        # Alterable: the difference is actionable, so it stays a change.
        ("VARCHAR(10)", "VARCHAR(20)"),
        ("VARCHAR(20)", "VARCHAR(10)"),
        ("NUMBER(10,2)", "NUMBER(20,2)"),
        ("TEXT", "VARCHAR(10)"),
        # Two bases: real drift, and worth stopping the write for.
        ("DATE", "TIMESTAMP_NTZ(9)"),
        ("NUMBER(38,0)", "VARCHAR(16777216)"),
        ("BINARY(10)", "VARCHAR(10)"),
        # Unparseable: never claimed to match.
        ("ARRAY(NUMBER(38,0))", "ARRAY(NUMBER(38,0))"),
    ],
)
def test_same_unalterable_base_refuses_everything_else(old, new):
    assert _iceberg.same_unalterable_base(old, new) is False


# ---------------------------------------------------------------------------
# compute_delta through the Snowflake schema builders
# ---------------------------------------------------------------------------


def _current(existing):
    """``(name, type)`` pairs for the target. A bare name takes a placeholder
    type: only the tests that turn on the current type spell one."""
    return [(c, "TEXT") if isinstance(c, str) else c for c in existing]


def _added(existing, staged):
    delta = _iceberg.compute_delta(
        _iceberg.table_schema_from_probe('"T"', _current(existing)),
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
        _iceberg.table_schema_from_probe('"T"', [("ID", "INT")]),
        _iceberg.table_schema_from_inferred('"T"', [("ID", "INT"), ("NEW", "TEXT")]),
        _iceberg.SNOWFLAKE_TYPE_WIDENING_MAP,
    )
    added = [c for c in delta.changes if c.action is _iceberg.Action.ADD_COLUMN]
    assert [c.safe for c in added] == [True]


def test_delta_reads_the_two_type_vocabularies_as_a_retype():
    # DESC TABLE and infer_schema spell the same column differently, so a column
    # neither side touched is reported as an ALTER_TYPE -- and the renderer now
    # renders one. Holding that statement back is why ``_evolve_target_columns``
    # lifts the ADD_COLUMN changes into a delta of their own instead of handing
    # this one to the renderer whole.
    delta = _iceberg.compute_delta(
        _iceberg.table_schema_from_probe('"T"', [("NAME", "VARCHAR(16777216)")]),
        _iceberg.table_schema_from_inferred('"T"', [("NAME", "TEXT")]),
        _iceberg.SNOWFLAKE_TYPE_WIDENING_MAP,
    )
    assert [c.action for c in delta.changes] == [_iceberg.Action.ALTER_TYPE]
    assert _iceberg.render_snowflake_sql(delta) == [
        'ALTER TABLE "T" ALTER COLUMN "NAME" SET DATA TYPE TEXT'
    ]
