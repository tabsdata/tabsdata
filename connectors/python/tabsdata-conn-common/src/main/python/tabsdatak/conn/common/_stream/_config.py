#
# Copyright 2026 Tabsdata Inc.
#

from __future__ import annotations

import logging
from dataclasses import dataclass

from tabsdatak.conn.common._stream.constants import (
    DEFAULT_BUFFER_MAX_BYTES,
    DEFAULT_BUFFER_MAX_ROWS,
    DEFAULT_BUFFER_MAX_SECS,
    DEFAULT_POLL_IDLE_SECS,
    DEFAULT_POLL_MAX_SECS,
    DEFAULT_TRIGGER_MAX_BYTES,
    DEFAULT_TRIGGER_MAX_ROWS,
    DEFAULT_TRIGGER_MAX_SECS,
)


@dataclass(frozen=True, kw_only=True)
class StreamRunConfig:
    buffer_max_rows: int = DEFAULT_BUFFER_MAX_ROWS
    buffer_max_bytes: int | None = DEFAULT_BUFFER_MAX_BYTES
    buffer_max_secs: int = DEFAULT_BUFFER_MAX_SECS
    trigger_max_rows: int | None = DEFAULT_TRIGGER_MAX_ROWS
    trigger_max_bytes: int | None = DEFAULT_TRIGGER_MAX_BYTES
    trigger_max_secs: int = DEFAULT_TRIGGER_MAX_SECS
    poll_max_secs: int = DEFAULT_POLL_MAX_SECS
    poll_idle_secs: int = DEFAULT_POLL_IDLE_SECS
    logging_level: int = logging.ERROR
