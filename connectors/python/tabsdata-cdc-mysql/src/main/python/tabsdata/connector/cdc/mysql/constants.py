#
# Copyright 2026 Tabsdata Inc.
#

from __future__ import annotations

import logging
from datetime import datetime, timezone

DEFAULT_SERVER_ID = 512

PENDING_TIMESTAMP = datetime.min.replace(tzinfo=timezone.utc)

logger = logging.getLogger(__name__)
logger.setLevel(logging.DEBUG)
