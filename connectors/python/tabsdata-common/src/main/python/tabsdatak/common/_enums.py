#
# Copyright 2026 Tabs Data Inc.
#

from __future__ import annotations

from enum import Enum


class WorkerCallbackStatus(str, Enum):
    """Mirror of the Rust ``WorkerCallbackStatus`` enum in
    ``td-common/src/execution_status.rs``."""

    RUNNING = "Running"
    DONE = "Done"
    FAILED = "Failed"
    STOPPED = "Stopped"


class MessageAction(str, Enum):
    """Mirror of the Rust ``MessageAction`` enum in
    ``td-common/src/server.rs`` (serialized lowercase)."""

    START = "start"
    STOP = "stop"
    NOTIFY = "notify"
