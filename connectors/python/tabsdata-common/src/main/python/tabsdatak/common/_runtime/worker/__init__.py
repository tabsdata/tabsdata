#
# Copyright 2026 Tabsdata Inc.
#

from __future__ import annotations

import logging
import signal
import subprocess
import sys
import threading
from abc import ABC, abstractmethod
from typing import Sequence

from tabsdatak.common._runtime.pathenv import extend_path
from tabsdatak.common._runtime.worker.error import (
    ExitCode,
    WorkerErrorCode,
    WorkerException,
)

logger = logging.getLogger(__name__)


class Worker(ABC):
    def __init__(self) -> None:
        self.stop_event = threading.Event()
        self.signalled: int | None = None

    def setup_signal_handlers(self) -> None:
        def handle_signal(signum, _frame):
            logger.info(f"Received termination signal ({signum}). Shutting down.")
            self.signalled = signum
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

    @abstractmethod
    def perform(self) -> None: ...

    def run(self) -> int:
        extend_path()
        self.setup_signal_handlers()
        # noinspection PyBroadException
        try:
            self.perform()
            return int(ExitCode.SUCCESS)
        except WorkerException as exception:
            logger.exception("Handled worker exception")
            code = exception.exit_code
            if code == ExitCode.SUCCESS:
                logger.error(
                    "A worker exception was raised with a success exit code; "
                    f"promoting it to {ExitCode.INCONSISTENT}."
                )
                return int(ExitCode.INCONSISTENT)
            return int(code)
        except SystemExit as exit_event:
            logger.error(
                f"An exit ({exit_event.code}) was intercepted by the worker; "
                f"promoting it to {ExitCode.PREMATURE}."
            )
            return int(ExitCode.PREMATURE)
        except BaseException:
            logger.exception("An unhandled exception was caught by the worker")
            return int(ExitCode.UNEXPECTED)

    # noinspection PyMethodMayBeStatic
    def submit(
        self,
        argv: Sequence[str],
        timeout: float | None = None,
        check: bool = True,
    ) -> subprocess.CompletedProcess:
        try:
            completed = subprocess.run(
                list(argv),
                capture_output=True,
                text=True,
                timeout=timeout,
            )
        except subprocess.TimeoutExpired as exception:
            raise WorkerErrorCode.WORKER_3.exception(
                cause=exception,
                argv=list(argv),
                timeout=timeout,
            )
        except OSError as exception:
            raise WorkerErrorCode.WORKER_1.exception(
                cause=exception,
                argv=list(argv),
                detail=str(exception),
            )
        if check and completed.returncode != 0:
            raise WorkerErrorCode.WORKER_2.exception(
                argv=list(argv),
                status=completed.returncode,
            )
        return completed
