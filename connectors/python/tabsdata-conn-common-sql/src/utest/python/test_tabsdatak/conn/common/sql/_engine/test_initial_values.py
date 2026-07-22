#
# Copyright 2026 Tabsdata Inc.
#

from __future__ import annotations

from pathlib import Path

import pytest

# noinspection PyProtectedMember
from tabsdatak.conn.common.sql._engine.initial_values import resolve_initial_values

# noinspection PyProtectedMember
from test_tabsdatak.conn.common.sql._testing import CtxStub


@pytest.fixture
def work_dir(tmp_path: Path) -> Path:
    out = tmp_path / "work"
    out.mkdir()
    return out


def test_resolve_initial_values_returns_none_when_seed_is_none(work_dir):
    """No seed → ``None``. Connector skips substitution entirely
    (no placeholders, no work to do)."""
    ctx = CtxStub(work_dir)
    assert resolve_initial_values(ctx, None) is None


def test_resolve_initial_values_returns_seed_when_ctx_empty(work_dir):
    """First run for a flow: ctx scratch has no entry for any bind
    name, fall back to the user-supplied seed for each."""
    ctx = CtxStub(work_dir)
    seed = {"last_modified": "2026-01-01T00:00:00Z", "min_id": 0}
    assert resolve_initial_values(ctx, seed) == seed


def test_resolve_initial_values_prefers_ctx_per_bind_name(work_dir):
    """Subsequent run: each bind name independently picks up its
    most recent value via ``ctx.get_attr(name)``. Seed is only
    used for the names ctx doesn't have."""
    ctx = CtxStub(
        work_dir,
        scratch={
            "last_modified": "2026-05-28T00:00:00Z",
            "min_id": 1234,
        },
    )
    seed = {"last_modified": "from-seed", "min_id": 0}
    assert resolve_initial_values(ctx, seed) == {
        "last_modified": "2026-05-28T00:00:00Z",
        "min_id": 1234,
    }


def test_resolve_initial_values_partial_ctx_falls_back_per_name(work_dir):
    """If ctx has one bind name set but not another, the missing
    name falls back to the seed -- per-name granularity."""
    ctx = CtxStub(work_dir, scratch={"last_modified": "2026-05-28T00:00:00Z"})
    seed = {"last_modified": "seed-ts", "min_id": 42}
    assert resolve_initial_values(ctx, seed) == {
        "last_modified": "2026-05-28T00:00:00Z",
        "min_id": 42,
    }


def test_resolve_initial_values_preserves_falsy_ctx_values(work_dir):
    """Uses ``is not None`` (not truthiness): a legitimately falsy
    value persisted last run (``0``, ``False``, ``""``) isn't
    silently shadowed by the seed."""
    ctx = CtxStub(work_dir, scratch={"min_id": 0, "flag": False, "label": ""})
    seed = {"min_id": 999, "flag": True, "label": "default"}
    assert resolve_initial_values(ctx, seed) == {
        "min_id": 0,
        "flag": False,
        "label": "",
    }


def test_resolve_initial_values_heterogeneous_primitive_values(work_dir):
    """The realistic SQL use case: bind values of mixed primitive
    types (str + int). The flat-key model round-trips them cleanly."""
    ctx = CtxStub(
        work_dir,
        scratch={
            "last_modified": "2026-01-15T00:00:00Z",
            "min_id": 1234,
        },
    )
    seed = {"last_modified": "seed", "min_id": 0}
    out = resolve_initial_values(ctx, seed)
    assert isinstance(out["last_modified"], str)
    assert isinstance(out["min_id"], int)
    assert out == {"last_modified": "2026-01-15T00:00:00Z", "min_id": 1234}


def test_resolve_initial_values_does_not_mutate_seed(work_dir):
    """Returned dict is a fresh object -- mutating it does not
    leak back into the user-supplied ``Src.initial_values``."""
    ctx = CtxStub(work_dir, scratch={"x": 1})
    seed = {"x": 0, "y": 2}
    out = resolve_initial_values(ctx, seed)
    assert out is not seed
    out["x"] = 99
    assert seed == {"x": 0, "y": 2}
