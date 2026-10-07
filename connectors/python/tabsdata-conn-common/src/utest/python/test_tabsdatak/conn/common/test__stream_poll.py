#
# Copyright 2026 Tabsdata Inc.
#

from __future__ import annotations

# noinspection PyProtectedMember
from tabsdatak.conn.common._stream._poll import SourcePoll


class Clock:
    def __init__(self) -> None:
        self.now = 0.0

    def __call__(self) -> float:
        return self.now


def test_a_poll_counts_only_the_time_spent_waiting_on_the_source():
    clock = Clock()

    def source():
        clock.now += 1.0
        yield "a"
        clock.now += 2.0
        yield "b"

    poll = SourcePoll(source(), clock=clock)
    consumed = []
    for record in poll:
        clock.now += 10.0
        consumed.append(record)

    assert consumed == ["a", "b"]
    assert (poll.records, poll.seconds) == (2, 3.0)


def test_the_wait_after_the_last_record_until_the_poll_ends_is_not_counted():
    clock = Clock()

    def source():
        clock.now += 0.25
        yield "a"
        clock.now += 10.0

    poll = SourcePoll(source(), clock=clock)
    assert list(poll) == ["a"]
    assert (poll.records, poll.seconds) == (1, 0.25)


def test_a_poll_over_a_plain_list_counts_its_records():
    poll = SourcePoll(["a", "b", "c"], clock=Clock())
    assert list(poll) == ["a", "b", "c"]
    assert poll.records == 3


def test_a_poll_can_count_the_rows_inside_each_record():
    poll = SourcePoll([[1, 2, 3], [4]], count=len, clock=Clock())
    assert list(poll) == [[1, 2, 3], [4]]
    assert poll.records == 4


def test_an_empty_poll_logs_nothing(caplog):
    poll = SourcePoll([], clock=Clock())
    list(poll)
    with caplog.at_level("DEBUG", logger="tabsdatak.conn.common._stream._poll"):
        poll.log()
    assert caplog.records == []


def test_a_poll_with_records_logs_them_with_its_seconds(caplog):
    clock = Clock()

    def source():
        clock.now += 1.25
        yield "a"

    poll = SourcePoll(source(), clock=clock)
    list(poll)
    with caplog.at_level("DEBUG", logger="tabsdatak.conn.common._stream._poll"):
        poll.log()
    assert [(r.levelname, r.args) for r in caplog.records] == [("DEBUG", (1, 1.25))]
