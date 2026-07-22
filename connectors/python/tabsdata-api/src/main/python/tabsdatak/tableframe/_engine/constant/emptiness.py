#
# Copyright 2026 Tabsdata Inc.
#

from __future__ import annotations

from datetime import datetime, timezone

EMPTY_STRING = ""

EMPTY_UUID_V7 = "00000000000000000000000000"
EMPTY_EXECUTION = EMPTY_UUID_V7
EMPTY_TRANSACTION = EMPTY_UUID_V7
EMPTY_VERSION = EMPTY_UUID_V7
EMPTY_TIMESTAMP = datetime.fromtimestamp(0, tz=timezone.utc)
