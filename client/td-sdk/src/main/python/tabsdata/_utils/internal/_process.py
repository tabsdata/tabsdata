#
# Copyright 2025 Tabs Data Inc.
#

import logging
import signal
import sys
import threading
from typing import Callable

logger = logging.getLogger(__name__)
logger.setLevel(logging.INFO)


class TaskRunner:
    def __init__(self, task: Callable[[], None], frequency: int):
        self.stop_event = threading.Event()
        self.task = task
        self.frequency = frequency

    def setup_signal_handlers(self):
        def handle_signal(signum, frame):
            logger.info(
                f"\nReceived termination signal ({signum} - {frame}). Shutting down..."
            )
            self.stop_event.set()

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

    def schedule(self):
        self.setup_signal_handlers()
        logger.info("Starting background task runner")
        while not self.stop_event.is_set():
            try:
                logger.info("Running background task...")
                self.task()
            except Exception as e:
                logger.exception(f"Background task raised an exception: {e}")
                raise
            logger.info(
                f"Waiting {self.frequency} seconds for next background task run"
            )
            self.stop_event.wait(timeout=self.frequency)
        logger.info("Stop signal received before start. Leaving.")
        logger.info("Exiting background task runner")

    def execute(self):
        self.setup_signal_handlers()
        logger.info("Starting foreground task")
        if not self.stop_event.is_set():
            logger.info("Stop signal received before start. Leaving.")
        else:
            try:
                logger.info("Running foreground task...")
                self.task()
            except Exception as e:
                logger.exception(f"Foreground task raised an exception: {e}")
                raise
        logger.info("Task completed. Waiting briefly for possible signals...")
        self.stop_event.wait(timeout=1)
        logger.info("Exiting foreground task runner")
