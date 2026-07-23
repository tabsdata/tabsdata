#
# Copyright 2026 Tabsdata Inc.
#

"""Tests for tabsdatax.itest.tdksystems.changes — the model-agnostic change
planner, SQL rendering, file-insert conversion, and recipe loading."""

from __future__ import annotations

import pytest

from tabsdatax.itest.tdksystems import changes as ch
from tabsdatax.itest.tdksystems.emit import Column
from tabsdatax.itest.tdksystems.errors import DatagenError

_COUNTS = {
    "employees": {"emp_employees": 50, "emp_departments": 9},
    "finance": {"customers": 40, "customer_subscription_events": 20},
    "airportdb": {"ap_airport": 30, "ap_passenger": 30, "ap_booking": 25},
}

# Every change entity a model's recipe defines (recipe / parents-first order).
_ALL = {
    "employees": ["emp_employees", "emp_departments"],
    "finance": ["customers", "customer_subscription_events"],
    "airportdb": ["ap_airport", "ap_passenger", "ap_booking"],
}


# ---- recipe discovery / loading -------------------------------------------


@pytest.mark.parametrize("model", ["employees", "finance", "airportdb"])
def test_bundled_models_ship_a_recipe(model):
    assert ch.model_supports_changes(model)
    recipe = ch.load_recipe(model)
    assert isinstance(recipe, ch.ChangeModel)
    assert recipe.entities


def test_model_without_recipe_is_unsupported():
    # sakila is a bundled model but ships no changes.py.
    assert not ch.model_supports_changes("sakila")
    with pytest.raises(DatagenError, match="no changes.py"):
        ch.load_recipe("sakila")


# ---- planning: entities ---------------------------------------------------


def _plan(model, entities, ops, count=6, rows=1, seed=7, insert_base=0):
    return ch.plan(
        ch.load_recipe(model),
        ch.ChangeSpec(
            entities=entities,
            ops=ops,
            count=count,
            rows=rows,
            seed=seed,
            insert_base=insert_base,
        ),
        _COUNTS[model],
    )


def test_one_entity_one_row_is_one_change_per_transaction():
    txns = _plan("employees", ["emp_employees"], ch.OpSelector.INSERT, rows=1)
    assert len(txns) == 6
    assert all(len(t) == 1 for t in txns)


def test_rows_group_in_the_named_entity():
    txns = _plan("employees", ["emp_employees"], ch.OpSelector.INSERT, rows=3)
    assert all(len(t) == 3 for t in txns)
    for t in txns:
        assert {c.table for c in t} == {"emp_employees"}


def test_multiple_entities_spread_across_tables():
    txns = _plan(
        "airportdb", ["ap_airport", "ap_passenger"], ch.OpSelector.INSERT, rows=1
    )
    assert all(len(t) == 2 for t in txns)
    for t in txns:
        assert {c.table for c in t} == {"ap_airport", "ap_passenger"}


def test_rows_apply_per_listed_entity():
    txns = _plan(
        "airportdb", ["ap_airport", "ap_passenger"], ch.OpSelector.INSERT, rows=2
    )
    for t in txns:
        assert len(t) == 4
        assert sum(1 for c in t if c.table == "ap_airport") == 2
        assert sum(1 for c in t if c.table == "ap_passenger") == 2


def test_entities_ordered_parents_first_regardless_of_arg_order():
    # Listed passenger-before-airport, but the recipe order (airport, passenger)
    # is what a transaction follows, so inserts satisfy foreign keys.
    txns = _plan(
        "airportdb", ["ap_passenger", "ap_airport"], ch.OpSelector.INSERT, rows=1
    )
    for t in txns:
        assert [c.table for c in t] == ["ap_airport", "ap_passenger"]


def test_entity_not_supporting_the_verb_is_skipped():
    # emp_departments supports only UPDATE; asking INSERT on both leaves each
    # transaction with just the emp_employees insert.
    txns = _plan(
        "employees",
        ["emp_employees", "emp_departments"],
        ch.OpSelector.INSERT,
        rows=1,
    )
    for t in txns:
        assert {c.table for c in t} == {"emp_employees"}


def test_unknown_entity_rejected():
    recipe = ch.load_recipe("employees")
    spec = ch.ChangeSpec(entities=["nope"], ops=ch.OpSelector.INSERT, count=1)
    with pytest.raises(DatagenError, match="unknown entit"):
        ch.plan(recipe, spec, _COUNTS["employees"])


def test_empty_entities_rejected():
    recipe = ch.load_recipe("employees")
    spec = ch.ChangeSpec(entities=[], ops=ch.OpSelector.INSERT, count=1)
    with pytest.raises(DatagenError, match="at least one entity"):
        ch.plan(recipe, spec, _COUNTS["employees"])


def test_mix_cycles_verbs():
    txns = _plan("employees", ["emp_employees"], ch.OpSelector.MIX, count=6)
    kinds = [t[0].kind for t in txns]
    assert ch.OpKind.INSERT in kinds
    assert ch.OpKind.UPDATE in kinds
    assert ch.OpKind.DELETE in kinds


# ---- planning: targeting / determinism ------------------------------------


def test_inserts_use_non_colliding_keys():
    txns = _plan("employees", ["emp_employees"], ch.OpSelector.INSERT)
    emp_inserts = [c for t in txns for c in t if c.table == "emp_employees"]
    assert emp_inserts
    assert all(c.row["emp_no"] >= 9_000_000 for c in emp_inserts)


def test_deletes_walk_the_tail():
    # At seed 0 the fold is a no-op: the first delete targets the tail (emp 10050).
    txns = _plan("employees", ["emp_employees"], ch.OpSelector.DELETE, seed=0)
    assert txns[0][0].where["emp_no"] == 10001 + 50 - 1


def test_updates_target_the_front():
    # At seed 0 the first update targets the front (customer_id 1).
    txns = _plan("finance", ["customers"], ch.OpSelector.UPDATE, seed=0)
    assert txns[0][0].where["customer_id"] == 1


def test_seed_shifts_update_delete_targets():
    # Different seeds (what --seed auto increments through) touch different rows.
    u0 = _plan("finance", ["customers"], ch.OpSelector.UPDATE, count=1, seed=0)
    u1 = _plan("finance", ["customers"], ch.OpSelector.UPDATE, count=1, seed=1)
    assert u0[0][0].where["customer_id"] != u1[0][0].where["customer_id"]


def test_insert_base_makes_inserts_disjoint_across_runs():
    # --seed auto advances a compact insert cursor; distinct bases never collide.
    a = _plan("employees", ["emp_employees"], ch.OpSelector.INSERT, insert_base=0)
    b = _plan("employees", ["emp_employees"], ch.OpSelector.INSERT, insert_base=100)
    ka = {c.row["emp_no"] for t in a for c in t}
    kb = {c.row["emp_no"] for t in b for c in t}
    assert ka and kb and ka.isdisjoint(kb)


def test_all_ops_deterministic_and_nonempty_with_seeded_data():
    # The --seed auto guarantee: every mode produces a valid, deterministic plan
    # (given seeded rows) — no collisions in the plan, reproducible.
    for model in ("employees", "finance", "airportdb"):
        for ops in ch.OpSelector:
            base = 5 if ops in (ch.OpSelector.INSERT, ch.OpSelector.MIX) else 0
            a = _plan(model, _ALL[model], ops, count=8, seed=3, insert_base=base)
            b = _plan(model, _ALL[model], ops, count=8, seed=3, insert_base=base)
            render = [ch.render_sql(c) for t in a for c in t]
            assert render == [ch.render_sql(c) for t in b for c in t]
            inserts = [c.row for t in a for c in t if c.kind is ch.OpKind.INSERT]
            keys = [tuple(sorted(r.items())) for r in inserts]
            assert len(keys) == len(set(keys))  # no dup insert rows within a run


def test_plan_is_deterministic_for_a_seed():
    a = _plan("airportdb", _ALL["airportdb"], ch.OpSelector.MIX, count=12, seed=3)
    b = _plan("airportdb", _ALL["airportdb"], ch.OpSelector.MIX, count=12, seed=3)

    def render(txns):
        return [ch.render_sql(c) for t in txns for c in t]

    assert render(a) == render(b)


def test_empty_counts_leaves_update_delete_with_nothing():
    recipe = ch.load_recipe("finance")
    spec = ch.ChangeSpec(
        entities=["customer_subscription_events"],
        ops=ch.OpSelector.DELETE,
        count=4,
        seed=1,
    )
    with pytest.raises(DatagenError, match="nothing to do"):
        ch.plan(recipe, spec, {})


def test_insert_needs_no_seeded_rows():
    recipe = ch.load_recipe("airportdb")
    spec = ch.ChangeSpec(entities=["ap_airport"], ops=ch.OpSelector.INSERT, count=3)
    txns = ch.plan(recipe, spec, {})  # no counts -> inserts still fine
    assert len(txns) == 3


@pytest.mark.parametrize("count", [0, -1])
def test_bad_count_rejected(count):
    recipe = ch.load_recipe("employees")
    spec = ch.ChangeSpec(
        entities=["emp_employees"], ops=ch.OpSelector.INSERT, count=count
    )
    with pytest.raises(DatagenError, match="count"):
        ch.plan(recipe, spec, _COUNTS["employees"])


# ---- SQL rendering --------------------------------------------------------

_COLS = [
    Column("id", "int", auto=True),
    Column("name", "varchar(20)"),
    Column("amount", "decimal(10,2)"),
]


def test_render_insert_skips_auto_and_quotes_values():
    c = ch.Change(ch.OpKind.INSERT, "t", _COLS, row={"name": "a'b", "amount": 5})
    sql = ch.render_sql(c)
    assert sql == "INSERT INTO t (name,amount) VALUES ('a''b',5)"


def test_render_update():
    c = ch.Change(
        ch.OpKind.UPDATE, "t", _COLS, set_values={"name": "x"}, where={"id": 3}
    )
    assert ch.render_sql(c) == "UPDATE t SET name = 'x' WHERE id = 3"


def test_render_delete():
    c = ch.Change(ch.OpKind.DELETE, "t", _COLS, where={"id": 7})
    assert ch.render_sql(c) == "DELETE FROM t WHERE id = 7"


# ---- file (insert-only) conversion ----------------------------------------


def test_insert_tables_collects_inserts_by_table():
    txns = _plan(
        "airportdb",
        ["ap_airport", "ap_passenger"],
        ch.OpSelector.INSERT,
        count=3,
        rows=4,
    )
    tables = ch.insert_tables(txns)
    by_name = {t.name: t for t in tables}
    assert set(by_name) == {"ap_airport", "ap_passenger"}
    assert sum(len(t.rows) for t in tables) == 24  # 3 txns * (4 + 4)


def test_has_insert():
    ins = _plan("employees", ["emp_employees"], ch.OpSelector.INSERT)
    dele = _plan("employees", ["emp_employees"], ch.OpSelector.DELETE)
    assert ch.has_insert(ins)
    assert not ch.has_insert(dele)
