#
# Copyright 2025 Tabs Data Inc.
#

import signal
import sys
import threading
import time
from types import FrameType
from typing import Optional

import colorama
import tqdm

colorama.init()

event: Optional[threading.Event] = None
thread: Optional[threading.Thread] = None


def handler(_signum: int, _frame: Optional[FrameType]) -> None:
    if event:
        event.set()
    if thread:
        thread.join(timeout=1)
    sys.exit(0)


def main(message: str) -> int:
    global event, thread

    event = threading.Event()
    symbols = ["⠋", "⠙", "⠹", "⠸", "⠼", "⠴", "⠦", "⠧", "⠇", "⠏"]
    index = 0

    def worker() -> None:
        nonlocal index

        with tqdm.tqdm(
            desc=message,
            dynamic_ncols=True,
            bar_format=(
                f"{colorama.Fore.CYAN}{{desc}} {{elapsed}}{colorama.Style.RESET_ALL}"
            ),
            leave=True,
            file=sys.stderr,
        ) as spinner:
            while not event.is_set():
                spinner.set_description_str(f"⏳ {symbols[index]} {message}")
                index = (index + 1) % len(symbols)
                spinner.refresh()
                time.sleep(0.1)

    signal.signal(signal.SIGTERM, handler)
    signal.signal(signal.SIGINT, handler)
    thread = threading.Thread(target=worker, daemon=True)
    thread.start()
    while not event.is_set():
        time.sleep(1)
    return 0


if __name__ == "__main__":
    caption = sys.argv[1]
    sys.exit(main(caption))
