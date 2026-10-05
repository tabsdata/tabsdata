#
# Copyright 2026 Tabsdata Inc.
#

from __future__ import annotations

from pathlib import Path
from typing import Any, Self

import pytest

# noinspection PyProtectedMember
from tabsdatak.conn.common._stream._ingester import StreamIngester

# noinspection PyProtectedMember
from tabsdatak.conn.common._stream._lifecycle import StreamLifecycle

# noinspection PyProtectedMember
from tabsdatak.conn.common._stream._offset import StreamOffset

# noinspection PyProtectedMember
from tabsdatak.conn.common._stream._offsets import TabsdataKeptOffsets


class Counter(StreamOffset):
    def __init__(self, value: int) -> None:
        self.value = value

    def update(self, other: Self) -> None:
        self.value = max(self.value, other.value)

    def write(self) -> str:
        return str(self.value)

    @classmethod
    def read(cls, data: str) -> Self:
        return cls(int(data))


class CounterIngester(StreamIngester[int, Counter]):
    def __init__(self) -> None:
        super().__init__()
        self.offsets = TabsdataKeptOffsets(key="counter", read=Counter.read)

    def close_session(self, session: Any) -> None:
        pass

    def base_offset(self) -> Counter:
        return Counter(0)

    def poll_changes(self, session: Any) -> list[int]:
        return []


def test_an_offset_round_trips_through_its_written_form():
    assert Counter.read(Counter(7).write()).value == 7


def test_an_offset_missing_a_contract_method_cannot_be_built():
    class Partial(StreamOffset):
        def update(self, other: Self) -> None:
            pass

    with pytest.raises(TypeError):
        Partial()


def test_an_ingester_missing_a_contract_method_cannot_be_built():
    class Partial(StreamIngester[int, Counter]):
        def close_session(self, session: Any) -> None:
            pass

    with pytest.raises(TypeError):
        Partial()


def test_an_ingester_starts_with_no_mark():
    ingester = CounterIngester()
    assert ingester._mark is None


def test_seeding_an_offset_sets_the_mark():
    ingester = CounterIngester()
    offset = Counter(3)
    ingester.seed_offset(offset)
    assert ingester._mark is offset


def test_seeding_none_leaves_the_mark_untouched():
    ingester = CounterIngester()
    offset = Counter(3)
    ingester.seed_offset(offset)
    ingester.seed_offset(None)
    assert ingester._mark is offset


class Recording(StreamLifecycle[Counter]):
    def __init__(self) -> None:
        self.checkpoints: list[Counter] = []

    def checkpoint(self, offset: Counter) -> None:
        self.checkpoints.append(offset)


def test_an_ingester_starts_with_a_lifecycle_that_takes_no_notice():
    ingester = CounterIngester()
    ingester.lifecycle.checkpoint(Counter(5))
    ingester.lifecycle.settle()


def test_attaching_a_lifecycle_routes_the_checkpoints_to_it():
    ingester = CounterIngester()
    lifecycle = Recording()
    ingester.attach_lifecycle(lifecycle)
    ingester.lifecycle.checkpoint(Counter(5))
    assert [offset.value for offset in lifecycle.checkpoints] == [5]


def test_settling_by_default_records_nothing():
    ingester = CounterIngester()
    lifecycle = Recording()
    ingester.attach_lifecycle(lifecycle)
    ingester.settle_offset()
    assert lifecycle.checkpoints == []


def test_opening_a_session_by_default_returns_none():
    assert CounterIngester().open_session(Path("/tmp")) is None
