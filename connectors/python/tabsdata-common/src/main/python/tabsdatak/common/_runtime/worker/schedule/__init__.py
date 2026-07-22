#
# Copyright 2026 Tabsdata Inc.
#

from __future__ import annotations

import logging
from abc import abstractmethod
from datetime import datetime, timezone

from tabsdatak.common._runtime.worker import Worker

logger = logging.getLogger(__name__)


class ScheduleWorker(Worker):
    start: datetime | None
    stop: datetime | None
    frequency: float

    @abstractmethod
    def task(self) -> None: ...

    @staticmethod
    def now() -> datetime:
        return datetime.now(tz=timezone.utc)

    def perform(self) -> None:
        if self.start is not None:
            delay = (self.start - self.now()).total_seconds()
            if delay > 0:
                logger.info(f"Waiting {delay} seconds until the scheduled start.")
                self.stop_event.wait(timeout=delay)
        while not self.stop_event.is_set():
            if self.stop is not None and self.now() >= self.stop:
                logger.info("The scheduled stop time was reached.")
                return
            self.task()
            self.stop_event.wait(timeout=self.frequency)
