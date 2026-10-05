#
# Copyright 2026 Tabsdata Inc.
#

from __future__ import annotations

from typing import Self

import pytest

# noinspection PyProtectedMember
from tabsdatak.conn.common._stream._offset import StreamOffset

# noinspection PyProtectedMember
from tabsdatak.conn.common._stream._offsets import (
    OffsetKeeper,
    SourceKeptOffsets,
    TabsdataKeptOffsets,
)
from tabsdatak.conn.common.error import ConnErrorCode, ConnStreamException


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


class Flaky(Exception):
    pass


def _kept(acknowledger=None) -> TabsdataKeptOffsets[Counter]:
    return TabsdataKeptOffsets(
        key="counter",
        read=Counter.read,
        acknowledger=acknowledger,
    )


def _source(acknowledger, **kwargs) -> SourceKeptOffsets[Counter]:
    return SourceKeptOffsets(
        acknowledger=acknowledger,
        retriable=lambda error: isinstance(error, Flaky),
        sleep=kwargs.pop("sleep", lambda _secs: None),
        **kwargs,
    )


def test_a_keeper_missing_a_contract_method_cannot_be_built():
    class Partial(OffsetKeeper):
        def stage_state(self, offset):
            return {}

    with pytest.raises(TypeError):
        Partial()


def test_tabsdata_kept_offsets_stage_the_written_offset_under_the_key():
    assert _kept().stage_state(Counter(7)) == {"counter": "7"}


@pytest.mark.parametrize(
    ("first_stage", "state"),
    [
        (True, {"counter": "7"}),
        (False, None),
        (False, {}),
        (False, {"other": "7"}),
    ],
)
def test_tabsdata_kept_offsets_resume_nothing_without_a_kept_offset(
    first_stage, state
):
    assert _kept().resume_offset(first_stage=first_stage, stream_state=state) is None


def test_tabsdata_kept_offsets_resume_from_what_they_staged():
    keeper = _kept()
    state = keeper.stage_state(Counter(11))
    assert keeper.resume_offset(first_stage=False, stream_state=state).value == 11


def test_tabsdata_kept_offsets_acknowledge_nothing_by_default():
    _kept().acknowledge_offset(Counter(3))


def test_tabsdata_kept_offsets_pass_the_acknowledgement_to_the_source():
    confirmed = []
    _kept(confirmed.append).acknowledge_offset(Counter(3))
    assert [offset.value for offset in confirmed] == [3]


def test_a_failed_source_acknowledgement_is_only_a_warning(caplog):
    def refuse(_offset):
        raise RuntimeError("slot gone")

    with caplog.at_level("WARNING"):
        _kept(refuse).acknowledge_offset(Counter(3))

    assert "slot gone" in caplog.text


def test_source_kept_offsets_never_resume_from_the_state():
    keeper = _source(lambda _offset: None)
    assert keeper.resume_offset(first_stage=False, stream_state={"counter": "7"}) is None


def test_source_kept_offsets_stage_a_callable_over_a_snapshot():
    acknowledged = []
    keeper = _source(acknowledged.append)
    offset = Counter(2)

    acknowledge = keeper.stage_state(offset)
    offset.update(Counter(9))
    acknowledge()

    assert [kept.value for kept in acknowledged] == [2]


def test_source_kept_offsets_retry_a_retriable_failure():
    outcomes = [Flaky("broker away"), None]
    attempts = []

    def acknowledger(offset):
        attempts.append(offset.value)
        outcome = outcomes.pop(0)
        if outcome is not None:
            raise outcome

    paused = []
    _source(acknowledger, sleep=paused.append).acknowledge_staged(Counter(5))

    assert attempts == [5, 5]
    assert paused == [1.0]


def test_source_kept_offsets_give_up_after_the_attempts():
    def acknowledger(_offset):
        raise Flaky("broker away")

    with pytest.raises(ConnStreamException) as raised:
        _source(acknowledger, attempts=3).acknowledge_staged(Counter(5))

    assert raised.value.api_error is ConnErrorCode.CONN_35
    assert isinstance(raised.value.__cause__, Flaky)
    assert "after 3 attempts" in str(raised.value)


def test_source_kept_offsets_do_not_retry_a_final_failure():
    attempts = []

    def acknowledger(offset):
        attempts.append(offset.value)
        raise RuntimeError("not a member")

    with pytest.raises(RuntimeError):
        _source(acknowledger).acknowledge_staged(Counter(5))

    assert attempts == [5]


def test_source_kept_offsets_acknowledge_nothing_after_the_stage():
    acknowledged = []
    _source(acknowledged.append).acknowledge_offset(Counter(5))
    assert acknowledged == []
