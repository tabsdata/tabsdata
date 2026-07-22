#
#  Copyright 2026 Tabsdata Inc.
#

"""Unit tests for the offset-ownership policy on
:class:`~tabsdatak.conn.common.sql.cdc._ingester.CdcIngester` -- the
``commit_state`` / ``resume_offset`` / ``offset_from_state`` /
``seed_offset`` surface that drives stream-source resume. These run
without a database; per-connector subclasses are exercised separately."""

from __future__ import annotations

import json
from typing import Any

import pytest

from tabsdatak.conn.common.sql.cdc._ingester import (
    CDC_OFFSET_STATE_KEY,
    CdcIngester,
)
from tabsdatak.conn.common.sql.cdc._model import CdcOffset


class FakeOffset(CdcOffset):
    """A trivial concrete :class:`CdcOffset` that round-trips through a
    single value, so ``write`` / ``read`` are observable."""

    def __init__(self, value: int):
        self.value = value

    def update(self, other: CdcOffset) -> None:  # pragma: no cover - unused here
        self.value = other.value

    def write(self) -> str:
        return json.dumps({"v": self.value})

    @classmethod
    def read(cls, data: str) -> "FakeOffset":
        return cls(json.loads(data)["v"])

    def __eq__(self, other: object) -> bool:
        return isinstance(other, FakeOffset) and other.value == self.value


class _BaseIngester(CdcIngester):
    """Minimal concrete ingester so the ABC can be instantiated. Only the
    offset-policy methods matter for these tests; the rest are stubs."""

    @property
    def ingester_type(self):
        return None

    def close_session(self, session: Any) -> None:
        pass

    @property
    def dialect(self):
        return None

    def base_offset(self) -> CdcOffset:
        return FakeOffset(0)

    def commit_offset(self, offset: CdcOffset) -> None:
        pass

    def poll_changes(self, session: Any):
        return []


class StateIngester(_BaseIngester):
    """Tabsdata-owned-offset ingester: implements ``offset_from_state``."""

    def offset_from_state(self, offset_state: str) -> CdcOffset:
        return FakeOffset.read(offset_state)


def _make(cls=StateIngester) -> CdcIngester:
    return cls("schema.table")


# --- commit_state (default, Tabsdata-owned) -----------------------------


def test_commit_state_returns_written_offset_under_state_key():
    ingester = _make()
    spec = ingester.commit_state(FakeOffset(7))
    # Tabsdata-owned offset: commit_state returns the resume-state dict
    # whose value is the offset's serialized string (``offset.write()``).
    assert isinstance(spec, dict)
    assert spec == {CDC_OFFSET_STATE_KEY: FakeOffset(7).write()}


# --- resume_offset (default, Tabsdata-owned) ----------------------------


def test_resume_offset_first_stage_returns_none():
    """Cold start: seed from configured ``start_from``, not persisted
    state, even when state is present."""
    ingester = _make()
    assert (
        ingester.resume_offset(
            first_stage=True,
            stream_state={CDC_OFFSET_STATE_KEY: FakeOffset(1).write()},
        )
        is None
    )


@pytest.mark.parametrize("state", [None, {}])
def test_resume_offset_without_state_returns_none(state):
    ingester = _make()
    assert ingester.resume_offset(first_stage=False, stream_state=state) is None


def test_resume_offset_reconstructs_from_persisted_state():
    ingester = _make()
    resumed = ingester.resume_offset(
        first_stage=False,
        stream_state={CDC_OFFSET_STATE_KEY: FakeOffset(9).write()},
    )
    assert resumed == FakeOffset(9)


def test_resume_offset_round_trips_commit_state():
    """The dict ``commit_state`` persisted is exactly what
    ``resume_offset`` reconstructs from."""
    ingester = _make()
    persisted = ingester.commit_state(FakeOffset(13))
    resumed = ingester.resume_offset(first_stage=False, stream_state=persisted)
    assert resumed == FakeOffset(13)


# --- offset_from_state (base raises) ------------------------------------


def test_base_offset_from_state_raises_not_implemented():
    """An ingester that never overrides ``offset_from_state`` (e.g. an
    external-owned one) must fail loudly if the base resume path reaches
    it, rather than silently cold-starting."""
    ingester = _make(_BaseIngester)
    with pytest.raises(NotImplementedError):
        ingester.resume_offset(
            first_stage=False,
            stream_state={CDC_OFFSET_STATE_KEY: FakeOffset(1).write()},
        )


# --- seed_offset --------------------------------------------------------


def test_seed_offset_with_value_sets_mark():
    ingester = _make()
    offset = FakeOffset(5)
    ingester.seed_offset(offset)
    assert ingester._mark is offset


def test_seed_offset_none_leaves_mark_untouched():
    """``None`` (cold start / source-managed) must not clobber a mark."""
    ingester = _make()
    offset = FakeOffset(5)
    ingester.seed_offset(offset)
    ingester.seed_offset(None)
    assert ingester._mark is offset
