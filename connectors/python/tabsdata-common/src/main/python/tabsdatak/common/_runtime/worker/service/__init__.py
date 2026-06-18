#
# Copyright 2026 Tabs Data Inc.
#

from __future__ import annotations

import logging
from abc import abstractmethod

from tabsdatak.common._runtime.worker import Worker

logger = logging.getLogger(__name__)


class ServiceWorker(Worker):
    @abstractmethod
    def serve(self) -> None: ...

    def perform(self) -> None:
        self.serve()
