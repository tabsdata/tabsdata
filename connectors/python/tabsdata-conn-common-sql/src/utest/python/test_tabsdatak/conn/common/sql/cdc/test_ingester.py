#
#  Copyright 2026 Tabsdata Inc.
#

"""Unit tests for the offset-ownership surface of
:class:`~tabsdatak.conn.common.sql.cdc._ingester.CdcIngester` -- the
``stage_state`` / ``resume_offset`` / ``seed_offset`` methods, delegated
to the ``offsets`` keeper, that drive stream-source resume. These run
without a database; per-connector subclasses are exercised separately."""

from __future__ import annotations

import json
from typing import Any

import pytest

# noinspection PyProtectedMember
from tabsdatak.conn.common._stream._offsets import TabsdataKeptOffsets
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

    def poll_changes(self, session: Any):
        return []


class StateIngester(_BaseIngester):
    """Tabsdata-kept-offset ingester: declares a ``TabsdataKeptOffsets``
    keeper reading ``FakeOffset``."""

    def __init__(self, tables):
        super().__init__(tables)
        self.offsets = TabsdataKeptOffsets(
            key=CDC_OFFSET_STATE_KEY,
            read=FakeOffset.read,
        )


def _make(cls=StateIngester) -> CdcIngester:
    return cls("schema.table")


# --- stage_state (Tabsdata-kept) -----------------------------------------


def test_stage_state_returns_written_offset_under_state_key():
    ingester = _make()
    spec = ingester.stage_state(FakeOffset(7))
    # Tabsdata-kept offset: stage_state returns the resume-state dict
    # whose value is the offset's serialized string (``offset.write()``).
    assert isinstance(spec, dict)
    assert spec == {CDC_OFFSET_STATE_KEY: FakeOffset(7).write()}


# --- resume_offset (Tabsdata-kept) ---------------------------------------


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


def test_resume_offset_round_trips_stage_state():
    """The dict ``stage_state`` persisted is exactly what
    ``resume_offset`` reconstructs from."""
    ingester = _make()
    persisted = ingester.stage_state(FakeOffset(13))
    resumed = ingester.resume_offset(first_stage=False, stream_state=persisted)
    assert resumed == FakeOffset(13)


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
