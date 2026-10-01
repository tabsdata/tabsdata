#
#  Copyright 2026 Tabsdata Inc.
#

"""Unit tests for the dialect-agnostic initial loader
(:mod:`tabsdatak.conn.common.sql.cdc._initial_load`), covering the
ordering the :class:`InitialLoadSession` contract requires: the resume
position is read once every table has been yielded, never before.

A session whose position is fixed only while its tables are yielded -- on
the first yield or after the last one, as the Db2 one does -- is the case
that catches a loader reading it too early."""

from __future__ import annotations

from unittest.mock import MagicMock

import pytest

from tabsdatak.conn.common.sql.cdc._initial_load import InitialLoader
from tabsdatak.conn.common.sql.cdc._model import CdcOffset, InitialLoadSession


class _Offset(CdcOffset):
    def __init__(self, value: str):
        self.value = value

    def update(self, other: CdcOffset) -> None:
        self.value = other.value

    def write(self) -> str:
        return self.value

    @classmethod
    def read(cls, data: str) -> CdcOffset:
        return cls(data)


class _PinOnYieldSession(InitialLoadSession):
    """Fixes its position when it pins, and pins on the first yield."""

    def __init__(self, tables: dict):
        self._offset: _Offset | None = None
        self._tables = tables
        self.pinned = False

    @property
    def engine(self):
        return MagicMock()

    @property
    def offset(self) -> CdcOffset:
        if self._offset is None:
            raise AssertionError("The position was read before the tables were pinned")
        return self._offset

    def tables(self, tracked_tables):
        self.pinned = True
        self._offset = _Offset("pinned")
        for tracked_table in tracked_tables.values():
            yield tracked_table, MagicMock()

    def close(self) -> None:
        pass


def _loader(ingester) -> InitialLoader:
    return InitialLoader(
        ingester=ingester,
        chunk_sentinel=MagicMock(),
        cdc_format=MagicMock(),
        large_column_policy=MagicMock(),
        fetch_size=10,
        chunk_size=10,
    )


def _ingester(tracked: dict) -> MagicMock:
    ingester = MagicMock()
    ingester.tracked_tables = tracked
    return ingester


def test_the_position_is_read_after_the_tables_are_pinned(monkeypatch):
    tracked = {"s.t": MagicMock()}
    session = _PinOnYieldSession(tracked)
    loader = _loader(_ingester(tracked))
    monkeypatch.setattr(loader, "_load_table", lambda *a, **k: None)

    offset = loader.load(session)

    assert session.pinned is True
    assert offset.write() == "pinned"


def test_the_position_is_read_after_a_session_that_pins_after_the_last_table(
    monkeypatch,
):
    class _PinsAfterTheLastTable(_PinOnYieldSession):
        def tables(self, tracked_tables):
            for tracked_table in tracked_tables.values():
                yield tracked_table, MagicMock()
            self.pinned = True
            self._offset = _Offset("pinned")

    tracked = {"s.t": MagicMock(), "s.u": MagicMock()}
    session = _PinsAfterTheLastTable(tracked)
    loader = _loader(_ingester(tracked))
    monkeypatch.setattr(loader, "_load_table", lambda *a, **k: None)

    offset = loader.load(session)

    assert session.pinned is True
    assert offset.write() == "pinned"


def test_the_position_is_read_even_when_no_table_is_tracked(monkeypatch):
    session = _PinOnYieldSession({})
    loader = _loader(_ingester({}))
    monkeypatch.setattr(loader, "_load_table", lambda *a, **k: None)

    offset = loader.load(session)

    assert session.pinned is True
    assert offset.write() == "pinned"


def test_a_session_that_never_pins_surfaces_its_own_error(monkeypatch):
    class _NeverPins(_PinOnYieldSession):
        def tables(self, tracked_tables):
            yield from ()

    session = _NeverPins({})
    loader = _loader(_ingester({}))
    monkeypatch.setattr(loader, "_load_table", lambda *a, **k: None)

    with pytest.raises(AssertionError, match="before the tables were pinned"):
        loader.load(session)
