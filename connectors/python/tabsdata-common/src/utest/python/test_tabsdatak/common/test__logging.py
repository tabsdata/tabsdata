#
# Copyright 2026 Tabsdata Inc.
#

import logging
from datetime import datetime, timezone

from tabsdatak._logging import NoFormatFormatter

FORMAT = "%(asctime)s.%(msecs)03dZ %(message)s"
DATEFMT = "%Y-%m-%dT%H:%M:%S"


def test_the_z_stamped_time_is_utc():
    record = logging.LogRecord("x", logging.INFO, "f.py", 1, "hello", None, None)
    stamp = NoFormatFormatter(FORMAT, DATEFMT).format(record).split(".")[0]
    utc = datetime.fromtimestamp(record.created, timezone.utc).strftime(DATEFMT)
    assert stamp == utc
