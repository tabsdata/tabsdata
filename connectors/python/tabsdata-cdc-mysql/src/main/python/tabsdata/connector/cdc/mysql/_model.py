#
# Copyright 2026 Tabsdata Inc.
#

from __future__ import annotations

from enum import auto

from tabsdata.connector.cdc.common._model import CdcArchitecture


class MySQLCdcArchitecture(CdcArchitecture):
    BINLOG = auto()
