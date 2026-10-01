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
from tabsdatak.conn.common._stream._offset import StreamOffset


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
    def close_session(self, session: Any) -> None:
        pass

    def base_offset(self) -> Counter:
        return Counter(0)

    def commit_offset(self, offset: Counter) -> None:
        pass

    def poll_changes(self, session: Any) -> list[int]:
        return []

    def commit_state(self, offset: Counter) -> dict:
        return {"counter": offset.write()}

    def resume_offset(
        self,
        *,
        first_stage: bool,
        stream_state: dict | None,
    ) -> Counter | None:
        if first_stage or not stream_state:
            return None
        return Counter.read(stream_state["counter"])


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


def test_an_ingester_starts_with_no_mark_and_no_checkpoint():
    ingester = CounterIngester()
    assert ingester._mark is None
    assert ingester._checkpoint is None


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


def test_watching_the_position_keeps_the_checkpoint():
    ingester = CounterIngester()
    recorded: list[Counter] = []
    ingester.watch_position(recorded.append)
    ingester._checkpoint(Counter(5))
    assert [offset.value for offset in recorded] == [5]


def test_settling_by_default_records_nothing():
    ingester = CounterIngester()
    recorded: list[Counter] = []
    ingester.watch_position(recorded.append)
    ingester.settle()
    assert recorded == []


def test_opening_a_session_by_default_returns_none():
    assert CounterIngester().open_session(Path("/tmp")) is None
