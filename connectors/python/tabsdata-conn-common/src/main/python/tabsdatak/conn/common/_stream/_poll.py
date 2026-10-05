#
# Copyright 2026 Tabsdata Inc.
#

from __future__ import annotations

import logging
import time
from collections.abc import Callable, Iterable, Iterator

logger = logging.getLogger(__name__)
logger.setLevel(logging.DEBUG)


class SourcePoll[R]:
    """One poll of the source, timed on the source alone: the seconds spent
    waiting on its iterator from the request of the first record to the
    arrival of the last, not the buffering and writing of what it yields and
    not the wait after the last record for a poll to reach its deadline."""

    def __init__(
        self,
        source: Iterable[R],
        count: Callable[[R], int] | None = None,
        clock: Callable[[], float] = time.perf_counter,
    ):
        self.source = iter(source)
        self.count = count
        self.clock = clock
        self.records = 0
        self.seconds = 0.0

    def __iter__(self) -> Iterator[R]:
        source = self.source
        count = self.count
        clock = self.clock
        while True:
            started = clock()
            try:
                record = next(source)
            except StopIteration:
                return
            self.seconds += clock() - started
            self.records += 1 if count is None else count(record)
            yield record

    def log(self) -> None:
        if self.records == 0:
            return
        logger.debug(
            "Polled %d record(s) from the source in %.3f s",
            self.records,
            self.seconds,
        )
