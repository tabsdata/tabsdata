#
# Copyright 2026 Tabsdata Inc.
#

from __future__ import annotations

import logging
from dataclasses import dataclass, field

from tabsdatak.api import BufferThresholds, PollThresholds
from tabsdatak.conn.common._stream.constants import (
    DEFAULT_BUFFER_MAX_BYTES,
    DEFAULT_BUFFER_MAX_ROWS,
    DEFAULT_BUFFER_MAX_SECS,
    DEFAULT_POLL_IDLE_SECS,
    DEFAULT_POLL_MAX_SECS,
)


def default_buffer_thresholds() -> BufferThresholds:
    return BufferThresholds(
        max_rows=DEFAULT_BUFFER_MAX_ROWS,
        max_bytes=DEFAULT_BUFFER_MAX_BYTES,
        max_secs=DEFAULT_BUFFER_MAX_SECS,
    )


def default_poll_thresholds() -> PollThresholds:
    return PollThresholds(
        max_secs=DEFAULT_POLL_MAX_SECS, idle_secs=DEFAULT_POLL_IDLE_SECS
    )


@dataclass(frozen=True, kw_only=True)
class StreamRunConfig:
    buffer: BufferThresholds = field(default_factory=default_buffer_thresholds)
    poll: PollThresholds = field(default_factory=default_poll_thresholds)
    logging_level: int = logging.ERROR
