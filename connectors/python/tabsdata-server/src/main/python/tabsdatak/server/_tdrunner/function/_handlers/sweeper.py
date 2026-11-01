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

logger = logging.getLogger(__name__)

# Short enough to pop the tail of the queue within the back-off window.
# Long enough not to spin while the queue is empty.
IDLE_SECONDS = 0.5


class Sweeper:
    stage: Stage
    queue: deque[str]
    folders: set[str]

    def __init__(self, stage: Stage) -> None:
        self.stage = stage
        self.queue = deque()
        self.folders = set()

    def __len__(self) -> int:
        return len(self.queue)

    def enqueue(self, path: str) -> None:
        try:
            manifest = StageManifest.parse(path)
        except InvalidManifestError as error:
            logger.warning(
                f"Cannot tell the batch of {path} ({error}); queuing it only"
            )
            self.queue.append(path)
            return
        self.enqueue_folder(manifest.folder)
        self.queue.append(path)

    def enqueue_folder(self, folder: str) -> None:
        if folder in self.folders:
            return
        try:
            names = self.stage.names(folder)
        except Exception as error:
            logger.warning(f"Cannot list {folder} ({error}); leaving it for later")
            return
        self.folders.add(folder)
        self.queue.extend(names)

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
