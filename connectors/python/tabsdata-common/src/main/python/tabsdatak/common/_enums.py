#
# Copyright 2026 Tabsdata Inc.
#

from __future__ import annotations

from enum import Enum


class WorkerCallbackStatus(str, Enum):
    RUNNING = "Running"
    DONE = "Done"
    FAILED = "Failed"
    STOPPED = "Stopped"


class MessageAction(str, Enum):
    START = "start"
    STOP = "stop"
    NOTIFY = "notify"


class DestinationWriteOp(str, Enum):
    DESTINATION_WRITE = "destination_write"
