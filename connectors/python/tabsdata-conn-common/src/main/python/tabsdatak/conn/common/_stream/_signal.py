#
# Copyright 2026 Tabsdata Inc.
#

from __future__ import annotations

import logging
import signal
import sys
import threading
from typing import Self

logger = logging.getLogger(__name__)


class StopSignal:
    def __init__(self) -> None:
        self._event = threading.Event()

    def is_set(self) -> bool:
        return self._event.is_set()

    def set(self) -> None:
        self._event.set()

    def wait(self, timeout: float) -> bool:
        return self._event.wait(timeout=timeout)

    @classmethod
    def install(cls) -> Self:
        stop_signal = cls()

        if threading.current_thread() is not threading.main_thread():
            return stop_signal

        previous = {
            signum: signal.getsignal(signum)
            for signum in (signal.SIGINT, signal.SIGTERM)
        }

        def handle_signal(signum, frame):
            logger.info(f"Received termination signal ({signum}). Shutting down...")
            stop_signal.set()
            chained = previous.get(signum)
            if callable(chained) and chained is not signal.default_int_handler:
                chained(signum, frame)

        signal.signal(signal.SIGINT, handle_signal)
        signal.signal(signal.SIGTERM, handle_signal)

        if sys.platform == "win32":
            signal.signal(signal.SIGBREAK, handle_signal)
            try:
                # noinspection PyPackageRequirements,PyUnresolvedReferences
                import win32api

                win32api.SetConsoleCtrlHandler(handle_signal, True)
            except ImportError:
                pass

        return stop_signal
