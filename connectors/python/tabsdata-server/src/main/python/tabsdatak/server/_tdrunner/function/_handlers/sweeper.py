#
# Copyright 2026 Tabsdata Inc.
#

from __future__ import annotations

import logging
import time
from collections import deque
from typing import Callable

from tabsdatak.server._tdrunner.function._commons.manifests.stage import (
    InvalidManifestError,
    StageManifest,
)
from tabsdatak.server._tdrunner.function._commons.storage.stage import Stage
from tabsdatak.server._tdrunner.function._handlers.landing import LandedPair

logger = logging.getLogger(__name__)

# Short enough to pop the tail of the queue within the back-off window.
# Long enough not to spin while the queue is empty.
IDLE_SECONDS = 0.5


class Sweeper:
    stage: Stage
    queue: deque[str]

    def __init__(self, stage: Stage) -> None:
        self.stage = stage
        self.queue = deque()

    def __len__(self) -> int:
        return len(self.queue)

    def enqueue(self, path: str) -> None:
        try:
            manifest = StageManifest.read(self.stage, path)
        except InvalidManifestError as error:
            logger.warning(
                f"Cannot list the files of {path} ({error}); queuing it only"
            )
            self.queue.append(path)
            return
        if manifest is None:
            logger.warning(f"Manifest {path} is gone; queuing it only")
            self.queue.append(path)
            return
        for files in manifest.tables:
            for name in files:
                self.queue.extend(LandedPair.of(name).names)
        self.queue.append(path)

    def sweep_until(self, deadline: float, active: Callable[[], bool]) -> None:
        while active() and time.monotonic() < deadline:
            if not self.queue:
                time.sleep(min(IDLE_SECONDS, max(0.0, deadline - time.monotonic())))
                continue
            name = self.queue.popleft()
            try:
                self.stage.discard(name)
                logger.debug(f"Deleted {name}")
            except Exception as error:
                logger.warning(f"Cannot delete {name}: {error}")
