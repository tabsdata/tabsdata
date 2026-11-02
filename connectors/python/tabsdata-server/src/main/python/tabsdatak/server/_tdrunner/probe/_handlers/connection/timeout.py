#
# Copyright 2026 Tabsdata Inc.
#

from __future__ import annotations

import contextvars
import threading
from collections.abc import Callable
from typing import Any


class ProbeTimeout(Exception):
    pass


# The call runs on a daemon thread that is abandoned past the timeout: a driver
# with no socket timeout of its own would otherwise hold the pod until its
# deadline.
def call_with_timeout(call: Callable[[], Any], timeout: float) -> Any:
    outcome: list[tuple[bool, Any]] = []

    def run() -> None:
        try:
            outcome.append((True, call()))
        except BaseException as error:  # noqa: BLE001 - raised again by the caller
            outcome.append((False, error))

    worker = threading.Thread(
        target=contextvars.copy_context().run,
        args=(run,),
        daemon=True,
    )
    worker.start()
    worker.join(timeout)
    if worker.is_alive():
        raise ProbeTimeout(f"exceeded {timeout:g}s")
    succeeded, value = outcome[0]
    if not succeeded:
        raise value
    return value
