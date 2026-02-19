#
#  Copyright 2026 Tabs Data Inc.
#

from __future__ import annotations

import logging
import sys
import threading
import time
import warnings
from collections.abc import Generator
from contextlib import contextmanager
from typing import Any, TextIO, cast

import colorama
import tqdm

logger = logging.getLogger(__name__)


def init_logging() -> logging.Logger:
    # noinspection PyBroadException
    # noinspection PyUnusedLocal
    def suppress_warnings() -> None:
        root_logger = logging.getLogger()
        root_logger.setLevel(logging.ERROR)

        handler = logging.StreamHandler()
        handler.setLevel(logging.ERROR)
        root_logger.addHandler(handler)

        logging.getLogger("distutils").setLevel(logging.ERROR)
        logging.getLogger("setuptools").setLevel(logging.ERROR)

        try:
            # noinspection PyUnresolvedReferences
            from setuptools.command.build_py import _IncludePackageDataAbuse

            _IncludePackageDataAbuse._Warning._DETAILS = ""
            warnings.filterwarnings(
                "ignore", category=_IncludePackageDataAbuse._Warning
            )
        except Exception:
            pass

        warnings.filterwarnings(
            "ignore", message=".*is absent from the `packages` configuration.*"
        )

    colorama.init()
    # suppress_warnings()
    return logging.getLogger()


class TqdmStream:
    def __init__(self, file: TextIO) -> None:
        self.file = file

    def write(self, text: str) -> None:
        if text and text.strip():
            tqdm.tqdm.write(text.rstrip("\n"), file=cast(Any, self.file))

    def flush(self) -> None:
        self.file.flush()

    def __getattr__(self, name: str) -> Any:
        return getattr(self.file, name)


@contextmanager
def global_spinner(desc: str) -> Generator[None, None, None]:
    if not tqdm or not sys.stderr.isatty():
        yield
        return

    stop = threading.Event()
    spinner_symbols = ["⠋", "⠙", "⠹", "⠸", "⠼", "⠴", "⠦", "⠧", "⠇", "⠏"]
    spinner_index = 0
    spinner_bar = None

    def worker() -> None:
        nonlocal spinner_index, spinner_bar
        with tqdm.tqdm(
            desc=desc,
            dynamic_ncols=True,
            bar_format=(
                f"{colorama.Fore.CYAN}{{desc}} {{elapsed}}{colorama.Style.RESET_ALL}"
            ),
            leave=True,
            file=sys.stderr,
        ) as bar:
            spinner_bar = bar
            while not stop.is_set():
                bar.set_description_str(f"⏳ {spinner_symbols[spinner_index]} {desc}")
                spinner_index = (spinner_index + 1) % len(spinner_symbols)
                bar.refresh()
                time.sleep(0.1)

    thread = threading.Thread(target=worker, daemon=True)
    thread.start()

    while spinner_bar is None:
        time.sleep(0.01)

    sys_stdout = sys.stdout
    sys_stderr = sys.stderr

    log_handlers = []
    for w_handler in logging.root.handlers[:]:
        if isinstance(w_handler, logging.StreamHandler):
            log_handlers.append((w_handler, w_handler.stream))
            w_handler.setStream(TqdmStream(w_handler.stream))

    try:
        sys.stdout = TqdmStream(sys_stdout)
        sys.stderr = TqdmStream(sys_stderr)
        yield
    finally:
        sys.stdout = sys_stdout
        sys.stderr = sys_stderr

        for w_handler, original_stream in log_handlers:
            w_handler.setStream(original_stream)

        stop.set()
        thread.join()
