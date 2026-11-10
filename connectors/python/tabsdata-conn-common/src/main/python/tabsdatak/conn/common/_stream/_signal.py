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

        signums = [signal.SIGINT, signal.SIGTERM]
        if sys.platform == "win32":
            signums.append(signal.SIGBREAK)
        previous = {signum: signal.getsignal(signum) for signum in signums}

        def handle_signal(signum, frame):
            logger.info(f"Received termination signal ({signum}). Shutting down...")
            stop_signal.set()
            chained = previous.get(signum)
            if callable(chained) and chained is not signal.default_int_handler:
                chained(signum, frame)

        def handle_console_event(event: int) -> bool:
            logger.info(f"Received console event {event}. Shutting down...")
            stop_signal.set()
            return False

        for signum in signums:
            signal.signal(signum, handle_signal)

        if sys.platform == "win32":
            try:
                # noinspection PyPackageRequirements,PyUnresolvedReferences
                import win32api

                win32api.SetConsoleCtrlHandler(handle_console_event, True)
            except ImportError:
                pass

        return stop_signal
