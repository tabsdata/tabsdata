#
# Copyright 2026 Tabs Data Inc.
#

from __future__ import annotations

import logging
import time
from abc import abstractmethod

from tabsdatak.common._runtime.worker.action import ActionWorker
from tabsdatak.common._runtime.worker.poll.error import PollErrorCode

logger = logging.getLogger(__name__)


class PollWorker(ActionWorker):
    timeout: float
    interval: float

    @abstractmethod
    def ready(self) -> bool: ...

    def perform(self) -> None:
        deadline = time.monotonic() + self.timeout
        while True:
            if self.stop_event.is_set():
                raise PollErrorCode.POLL_2.exception(signal=self.signalled)
            if self.ready():
                logger.info("The condition became ready.")
                return
            if time.monotonic() >= deadline:
                raise PollErrorCode.POLL_1.exception(timeout=self.timeout)
            logger.info(
                f"The condition is not ready yet; retrying in {self.interval} seconds."
            )
            self.stop_event.wait(timeout=self.interval)
