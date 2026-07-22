#
# Copyright 2026 Tabsdata Inc.
#

from __future__ import annotations

from pathlib import Path

import pytest

from tabsdatak.server._tdrunner.function._lib.initial_values import (
    InvalidInitialValuesError,
)
from tabsdatak.server._tdrunner.function._lib.request_ctx import (
    FromRequestCtx,
)

pytestmark = [pytest.mark.unit, pytest.mark.tabsdatak]


def _ctx() -> FromRequestCtx:
    return FromRequestCtx(
        work_dir=Path("/tmp/work"),
        exec_plan_id="ep-1",
        trx_id="trx-1",
        exec_plan_ts="2026-06-04T07:14:28.376000+00:00",
        collection_id="c-1",
        collection_name="col",
        function_id="fn-1",
        function_name="publish_people",
    )


# --- identifier properties (unchanged from prior baseline) --------------


def test_identifier_properties_round_trip():
    c = _ctx()
    assert c.work_dir == Path("/tmp/work")
    assert c.exec_plan_id == "ep-1"
    assert c.trx_id == "trx-1"
    assert c.exec_plan_ts == "2026-06-04T07:14:28.376000+00:00"
    assert c.collection_id == "c-1"
    assert c.collection_name == "col"
    assert c.function_id == "fn-1"
    assert c.function_name == "publish_people"


# --- get_attr / set_attr (real impl) ----------------------------------


def test_get_attr_unset_returns_none():
    assert _ctx().get_attr("missing") is None


def test_set_attr_writethrough_get_attr_sees_value():
    c = _ctx()
    c.set_attr("k", "v")
    assert c.get_attr("k") == "v"


def test_set_attr_overwrites_existing_value():
    c = _ctx()
    c.set_attr("k", "v1")
    c.set_attr("k", "v2")
    assert c.get_attr("k") == "v2"


def test_set_attr_accepts_all_basic_type_shapes():
    """``BasicTypeSpec`` is ``str | int | float | bool`` -- every scalar
    shape round-trips through the scratch store."""
    c = _ctx()
    c.set_attr("s", "x")
    c.set_attr("i", 42)
    c.set_attr("f", 3.14)
    c.set_attr("b", True)
    assert c.get_attr("s") == "x"
    assert c.get_attr("i") == 42
    assert c.get_attr("f") == 3.14
    assert c.get_attr("b") is True


def test_set_attr_with_none_keeps_key_present():
    """``None`` is a valid value per the SPI type. Storing None is
    NOT the same as never having stored -- a subsequent ``get_attr``
    returns None for both, but ``drain`` shows the difference."""
    c = _ctx()
    c.set_attr("k", None)
    assert c.get_attr("k") is None
    assert "k" in c.drain()


def test_add_metadata_is_still_a_noop():
    """``add_metadata`` is out of scope here -- still a documented
    no-op until the observability sink lands."""
    assert _ctx().add_metadata("k", "v") is None


# --- hydrate / drain (runtime-only) -------------------------------------


def test_hydrate_populates_scratch():
    c = _ctx()
    c.hydrate({"a": "1", "b": 2})
    assert c.get_attr("a") == "1"
    assert c.get_attr("b") == 2


def test_drain_returns_full_scratch():
    c = _ctx()
    c.set_attr("x", "v")
    c.set_attr("y", 42)
    assert c.drain() == {"x": "v", "y": 42}


def test_drain_on_fresh_ctx_returns_empty_dict():
    assert _ctx().drain() == {}


def test_drain_returns_a_copy_not_a_view():
    """Mutating the drained dict must not affect the ctx, and
    subsequent set_attr must not leak into a previously-drained
    dict."""
    c = _ctx()
    c.set_attr("x", "v")
    snapshot = c.drain()
    snapshot["x"] = "tampered"
    snapshot["y"] = "added"
    assert c.get_attr("x") == "v"  # ctx unaffected
    assert c.get_attr("y") is None
    # Subsequent store doesn't propagate back to the snapshot.
    c.set_attr("z", "new")
    assert "z" not in snapshot


def test_hydrate_merges_into_existing_state():
    """Repeated hydrate calls merge -- existing keys overwritten by
    the new dict, untouched keys preserved."""
    c = _ctx()
    c.hydrate({"a": "1", "b": "2"})
    c.hydrate({"b": "B", "c": "3"})
    assert c.drain() == {"a": "1", "b": "B", "c": "3"}


def test_hydrate_then_user_store_then_drain():
    """End-to-end: runtime hydrates from previous run, user stores
    new value, runtime drains for persistence."""
    c = _ctx()
    c.hydrate({"last-modified": "2026-05-27T00:00:00Z"})
    # User function reads via get_attr.
    assert c.get_attr("last-modified") == "2026-05-27T00:00:00Z"
    # User function writes via set_attr.
    c.set_attr("last-modified", "2026-05-28T00:00:00Z")
    # Runtime drains for persistence.
    assert c.drain() == {"last-modified": "2026-05-28T00:00:00Z"}


def test_hydrate_with_empty_dict_is_noop():
    c = _ctx()
    c.set_attr("x", "v")
    c.hydrate({})
    assert c.drain() == {"x": "v"}


# --- isolation between instances ---------------------------------------


def test_separate_instances_have_separate_scratch_stores():
    """Each ctx has its own dict -- no class-level leak."""
    a = _ctx()
    b = _ctx()
    a.set_attr("k", "from_a")
    assert b.get_attr("k") is None


# --- eager validation in set_attr -------------------------------------


def test_set_attr_rejects_non_str_key_at_call_site():
    """``ContextKeySpec`` is plain ``str``, but a non-string key must
    still raise at the user's ``set_attr`` call site rather than
    silently landing in the scratch store and exploding later at
    drain time."""
    c = _ctx()
    with pytest.raises(InvalidInitialValuesError, match="ContextKeySpec"):
        c.set_attr(123, "v")  # type: ignore[arg-type]
    # Failure means the scratch store stayed clean.
    assert 123 not in c.drain()


def test_set_attr_accepts_any_string_key_shape():
    """``ContextKeySpec`` is plain ``str`` -- underscores, dashes,
    dots, spaces, mixed: anything goes. The framework packs all the
    user's flat keys into one ``user_namespace`` Struct cell, so
    keys never appear as Parquet column names."""
    c = _ctx()
    for ok in (
        "last_modified",
        "min-id",
        "snake_case_key",
        "kebab-case-key",
        "dotted.namespace",
        "with space",
        "weird!@#",
    ):
        c.set_attr(ok, "v")
        assert c.get_attr(ok) == "v"
