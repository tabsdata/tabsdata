#
# Copyright 2026 Tabsdata Inc.
#

from __future__ import annotations

import logging
import signal
import sys
from collections.abc import Callable
from types import FrameType

logger = logging.getLogger(__name__)

# Set from signal handlers, which may interrupt each other: a lock here (as in
# threading.Event) deadlocks when a second signal lands while the first holds it.
requested = False


def request_shutdown() -> None:
    global requested
    requested = True


def is_shutdown_requested() -> bool:
    return requested


def listen_for_shutdown(stop: Callable[[], None]) -> None:
    def handle_stop(signal_number: int, frame: FrameType | None) -> None:
        logger.info(
            f"Received {signal.Signals(signal_number).name}; stopping at the next check"
        )
        request_shutdown()
        stop()

    try:
        signal.signal(signal.SIGTERM, handle_stop)
        signal.signal(signal.SIGINT, handle_stop)
        if sys.platform == "win32":
            signal.signal(signal.SIGBREAK, handle_stop)
        else:
            signal.signal(signal.SIGHUP, ignore_hangup)
    except ValueError:
        logger.debug("Not on the main thread; the shutdown signals are not handled")


def ignore_hangup(signal_number: int, frame: FrameType | None) -> None:
    logger.info("Ignoring SIGHUP, as the process outlives the terminal that started it")
