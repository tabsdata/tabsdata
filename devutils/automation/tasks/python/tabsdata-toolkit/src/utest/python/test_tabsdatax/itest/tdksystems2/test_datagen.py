#
# Copyright 2026 Tabsdata Inc.
#

"""Deterministic data generation: initial data, delta planning, SQL rendering."""

from __future__ import annotations

import random

import pytest

from tabsdatax.itest.tdksystems2 import datagen
from tabsdatax.itest.tdksystems2.datagen import ChangeSpec, OpKind, OpSelector
from tabsdatax.itest.tdksystems2.errors import TdxError


def _initial(n):
    mdl = datagen.load_model("employees")
    return mdl, mdl.build_initial(random.Random(datagen.INITIAL_SEED), n)


def test_list_models_and_entities():
    assert "employees" in datagen.list_models()
    assert datagen.entities_of("employees")[0] == "departments"
    assert len(datagen.entities_of("employees")) == 6


def test_unknown_model_raises():
    with pytest.raises(TdxError):
        datagen.entities_of("nope")


def test_base_size_employee_count():
    _, base = _initial(datagen.BASE_SIZE)
    emp = next(t for t in base if t.name == "employees")
    assert len(emp.rows) == 100
    assert emp.rows[0]["emp_no"] == 10001  # base for modify-data targeting


def test_entities_have_sizeable_markers():
    # world: cities grow with --size; countries/languages are fixed reference.
    assert datagen.entities_of("world") == ["country", "city", "country_language"]
    assert datagen.sizeable_entities("world") == ["city"]
    # a fully synthetic model: every entity is sizeable.
    assert set(datagen.sizeable_entities("courierlogistics")) == set(
        datagen.entities_of("courierlogistics")
    )


def _finance(size):
    mdl = datagen.load_model("finance")
    tables = mdl.build_initial(
        random.Random(datagen.INITIAL_SEED), datagen.BASE_SIZE * size
    )
    return {t.name: t.rows for t in tables}


def test_finance_size_scales_the_fact_tables_only():
    # finance is pinned to the accounts.csv snapshot: the account-keyed entities
    # and the plan catalogue keep their row counts, the facts grow with --size.
    base, scaled = _finance(1), _finance(3)
    for fixed in ("subscription_plans", "subscription_plan_prices", "customers",
                  "payment_methods"):
        assert len(scaled[fixed]) == len(base[fixed])
    assert len(scaled["customer_subscriptions"]) == 3 * len(
        base["customer_subscriptions"]
    )
    assert len(scaled["customer_subscription_events"]) > len(
        base["customer_subscription_events"]
    )
    assert datagen.sizeable_entities("finance") == [
        "customer_subscriptions",
        "customer_subscription_events",
    ]


def test_finance_keeps_one_live_subscription_per_customer_and_plan():
    # Every SQL flavour of the model carries a partial unique index over the live
    # statuses, so the subscriptions a multiplier adds must be terminal ones.
    live = {"TRIALING", "ACTIVE", "PAST_DUE", "PAUSED"}
    subs = _finance(5)["customer_subscriptions"]
    keys = [(r["customer_id"], r["plan_id"]) for r in subs if r["status"] in live]
    assert len(keys) == len(set(keys))


def test_finance_events_reference_generated_subscription_ids():
    # subscription_id is an identity column filled in insert order, so the ids
    # the generator hands the events must be that same 1..N run.
    tables = _finance(2)
    ids = [r["subscription_id"] for r in tables["customer_subscriptions"]]
    assert ids == list(range(1, len(ids) + 1))
    events = {r["subscription_id"] for r in tables["customer_subscription_events"]}
    assert events <= set(ids)


def test_initial_data_is_deterministic():
    _, a = _initial(50)
    _, b = _initial(50)
    assert [t.rows for t in a] == [t.rows for t in b]


def _plan_sql(spec, counts):
    mdl = datagen.load_model("employees")
    return [
        [datagen.render_sql(c, quote="`") for c in txn]
        for txn in datagen.plan(mdl, spec, counts)
    ]


def test_plan_is_deterministic_and_reproducible():
    counts = {"employees": 50, "departments": 9}
    spec = ChangeSpec(
        entities=["employees"], ops=OpSelector.MIX, sets=6,
        rows_per_entity={"employees": 1}, seed=2, insert_base=2000,
    )
    assert _plan_sql(spec, counts) == _plan_sql(spec, counts)


def test_insert_uses_seed_block_so_distinct_seeds_never_collide():
    counts = {"employees": 50}
    mdl = datagen.load_model("employees")

    def inserts_for(seed):
        spec = ChangeSpec(
            entities=["employees"], ops=OpSelector.INSERT, sets=3,
            rows_per_entity={"employees": 1}, seed=seed,
            insert_base=seed * datagen.FIXED_SEED_INSERT_STRIDE,
        )
        return [
            c.row["emp_no"]
            for txn in datagen.plan(mdl, spec, counts) for c in txn
        ]

    s1, s2 = inserts_for(1), inserts_for(2)
    assert not (set(s1) & set(s2)), "distinct fixed seeds must mint distinct inserts"
    assert inserts_for(1) == s1  # a given seed replays the same inserts


def test_single_entity_sets_is_row_count():
    counts = {"employees": 50}
    mdl = datagen.load_model("employees")
    spec = ChangeSpec(
        entities=["employees"], ops=OpSelector.UPDATE, sets=4,
        rows_per_entity={"employees": 1}, seed=1, insert_base=0,
    )
    txns = datagen.plan(mdl, spec, counts)
    assert sum(len(t) for t in txns) == 4  # 4 rows == --sets


def test_all_entities_uses_per_entity_rows_per_set():
    counts = {"employees": 50, "departments": 9}
    mdl = datagen.load_model("employees")
    spec = ChangeSpec(
        entities=["employees", "departments"], ops=OpSelector.UPDATE, sets=2,
        rows_per_entity={"employees": 1, "departments": 1}, seed=1, insert_base=0,
    )
    txns = datagen.plan(mdl, spec, counts)
    # 2 sets x (1 employee update + 1 department update) = 4 changes
    assert sum(len(t) for t in txns) == 4


def test_departments_reject_insert_and_delete():
    mdl = datagen.load_model("employees")
    dept = next(e for e in mdl.entities if e.table == "departments")
    assert dept.supports == (OpKind.UPDATE,)


def test_plan_nothing_to_do_raises():
    mdl = datagen.load_model("employees")
    spec = ChangeSpec(
        entities=["departments"], ops=OpSelector.DELETE, sets=2,
        rows_per_entity={"departments": 1}, seed=1, insert_base=0,
    )
    with pytest.raises(TdxError):
        datagen.plan(mdl, spec, {"departments": 9})


def test_op_counts_and_net_deltas():
    counts = {"employees": 50, "departments": 9}
    mdl = datagen.load_model("employees")
    spec = ChangeSpec(
        entities=["employees", "departments"], ops=OpSelector.MIX, sets=3,
        rows_per_entity={"employees": 1, "departments": 1}, seed=1,
        insert_base=1000,
    )
    txns = datagen.plan(mdl, spec, counts)
    per = datagen.op_counts(txns)
    # Mix over 3 sets = 1 insert + 1 update + 1 delete on employees; departments
    # takes only the update (insert/delete unsupported).
    assert per["employees"] == {OpKind.INSERT: 1, OpKind.UPDATE: 1, OpKind.DELETE: 1}
    assert per["departments"] == {OpKind.INSERT: 0, OpKind.UPDATE: 1, OpKind.DELETE: 0}
    # net row change: employees +1 -1 = 0, departments unchanged.
    assert datagen.net_row_deltas(txns) == {"employees": 0}


def test_ops_tag_and_op_applies():
    assert datagen.ops_tag((OpKind.INSERT, OpKind.UPDATE, OpKind.DELETE)) == "I/U/D"
    assert datagen.ops_tag((OpKind.UPDATE,)) == "U"
    assert datagen.ops_tag(()) == ""
    # a single op must be supported; MIX applies if any op is supported.
    assert datagen.op_applies((OpKind.UPDATE,), OpSelector.MIX)
    assert datagen.op_applies((OpKind.UPDATE,), OpSelector.UPDATE)
    assert not datagen.op_applies((OpKind.UPDATE,), OpSelector.INSERT)
    assert not datagen.op_applies((), OpSelector.MIX)
