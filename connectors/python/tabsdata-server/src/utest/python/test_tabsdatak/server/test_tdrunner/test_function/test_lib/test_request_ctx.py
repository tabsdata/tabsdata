#
# Copyright 2026 Tabs Data Inc.
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
        collection_id="c-1",
        collection_name="col",
        function_id="fn-1",
    )


# --- identifier properties (unchanged from prior baseline) --------------


def test_identifier_properties_round_trip():
    c = _ctx()
    assert c.work_dir == Path("/tmp/work")
    assert c.exec_plan_id == "ep-1"
    assert c.trx_id == "trx-1"
    assert c.collection_id == "c-1"
    assert c.collection_name == "col"
    assert c.function_id == "fn-1"


def test_function_name_raises_notimplemented():
    with pytest.raises(NotImplementedError, match="function_name"):
        _ = _ctx().function_name


def test_exec_plan_ts_raises_notimplemented():
    with pytest.raises(NotImplementedError, match="exec_plan_ts"):
        _ = _ctx().exec_plan_ts


# --- get_attr / store_attr (real impl) ----------------------------------


def test_get_attr_unset_returns_none():
    assert _ctx().get_attr("missing") is None


def test_store_attr_writethrough_get_attr_sees_value():
    c = _ctx()
    c.store_attr("k", "v")
    assert c.get_attr("k") == "v"


def test_store_attr_overwrites_existing_value():
    c = _ctx()
    c.store_attr("k", "v1")
    c.store_attr("k", "v2")
    assert c.get_attr("k") == "v2"


def test_store_attr_accepts_all_context_value_shapes():
    """ContextValueSpec is `str | int | float | bool | list[str] |
    dict[str, str]` -- every shape round-trips."""
    c = _ctx()
    c.store_attr("s", "x")
    c.store_attr("i", 42)
    c.store_attr("f", 3.14)
    c.store_attr("b", True)
    c.store_attr("lst", ["a", "b"])
    c.store_attr("d", {"k": "v"})
    assert c.get_attr("s") == "x"
    assert c.get_attr("i") == 42
    assert c.get_attr("f") == 3.14
    assert c.get_attr("b") is True
    assert c.get_attr("lst") == ["a", "b"]
    assert c.get_attr("d") == {"k": "v"}


def test_store_attr_with_none_keeps_key_present():
    """``None`` is a valid value per the SPI type. Storing None is
    NOT the same as never having stored -- a subsequent ``get_attr``
    returns None for both, but ``drain`` shows the difference."""
    c = _ctx()
    c.store_attr("k", None)
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
    c.store_attr("x", "v")
    c.store_attr("y", 42)
    assert c.drain() == {"x": "v", "y": 42}


def test_drain_on_fresh_ctx_returns_empty_dict():
    assert _ctx().drain() == {}


def test_drain_returns_a_copy_not_a_view():
    """Mutating the drained dict must not affect the ctx, and
    subsequent store_attr must not leak into a previously-drained
    dict."""
    c = _ctx()
    c.store_attr("x", "v")
    snapshot = c.drain()
    snapshot["x"] = "tampered"
    snapshot["y"] = "added"
    assert c.get_attr("x") == "v"  # ctx unaffected
    assert c.get_attr("y") is None
    # Subsequent store doesn't propagate back to the snapshot.
    c.store_attr("z", "new")
    assert "z" not in snapshot


def test_drain_deep_copies_nested_values():
    """``drain`` returns a deep copy: mutating a nested mutable in
    the snapshot must not bleed back into the ctx, and a subsequent
    ``store_attr`` of a nested value must not echo through any
    previously-drained snapshot."""
    c = _ctx()
    c.store_attr("labels", {"env": "prod"})
    snapshot = c.drain()
    snapshot["labels"]["env"] = "tampered"
    assert c.get_attr("labels") == {"env": "prod"}

    c.store_attr("tags", ["a", "b"])
    snapshot2 = c.drain()
    snapshot2["tags"].append("tampered")
    assert c.get_attr("tags") == ["a", "b"]


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
    # User function writes via store_attr.
    c.store_attr("last-modified", "2026-05-28T00:00:00Z")
    # Runtime drains for persistence.
    assert c.drain() == {"last-modified": "2026-05-28T00:00:00Z"}


def test_hydrate_with_empty_dict_is_noop():
    c = _ctx()
    c.store_attr("x", "v")
    c.hydrate({})
    assert c.drain() == {"x": "v"}


# --- isolation between instances ---------------------------------------


def test_separate_instances_have_separate_scratch_stores():
    """Each ctx has its own dict -- no class-level leak."""
    a = _ctx()
    b = _ctx()
    a.store_attr("k", "from_a")
    assert b.get_attr("k") is None


# --- eager validation in store_attr -------------------------------------


def test_store_attr_rejects_bad_key_at_call_site():
    """An invalid key (e.g. with an underscore -- ContextKeySpec is
    dash-only) must raise at the user's ``store_attr`` call site
    rather than silently landing in the scratch store and exploding
    later at drain time."""
    c = _ctx()
    with pytest.raises(InvalidInitialValuesError, match="ContextKeySpec"):
        c.store_attr("bad_key", "v")
    # Failure means the scratch store stayed clean.
    assert "bad_key" not in c.drain()


def test_store_attr_rejects_empty_key_at_call_site():
    c = _ctx()
    with pytest.raises(InvalidInitialValuesError, match="ContextKeySpec"):
        c.store_attr("", "v")


def test_store_attr_rejects_punctuation_in_key():
    """Path-traversal-style or special-character keys are rejected at
    the call site, not at drain."""
    c = _ctx()
    for bad in ("a/b", "a..b", "a b", "a!b"):
        with pytest.raises(InvalidInitialValuesError, match="ContextKeySpec"):
            c.store_attr(bad, "v")


def test_store_attr_accepts_dotted_keys():
    """Valid dotted-identifier keys are accepted: each segment is
    a DNS-1123 label-style identifier separated by dots."""
    c = _ctx()
    c.store_attr("databricks.last-modified", "v")
    assert c.get_attr("databricks.last-modified") == "v"
