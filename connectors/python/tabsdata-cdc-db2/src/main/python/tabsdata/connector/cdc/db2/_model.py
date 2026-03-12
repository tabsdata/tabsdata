#
# Copyright 2026 Tabs Data Inc.
#

from __future__ import annotations

from enum import auto

from tabsdata.connector.cdc.common._model import CdcArchitecture


class Db2CdcArchitecture(CdcArchitecture):
    SQL_REPLICATION = auto()
