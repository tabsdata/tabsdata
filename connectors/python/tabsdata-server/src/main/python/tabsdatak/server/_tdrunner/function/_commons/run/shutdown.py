#
# Copyright 2026 Tabsdata Inc.
#

from __future__ import annotations

# Set from signal handlers, which may interrupt each other: a lock here (as in
# threading.Event) deadlocks when a second signal lands while the first holds it.
requested = False


def request_shutdown() -> None:
    global requested
    requested = True


def is_shutdown_requested() -> bool:
    return requested
