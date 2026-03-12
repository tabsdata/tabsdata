#
# Copyright 2026 Tabs Data Inc.
#

from __future__ import annotations

import logging
from typing import Any

from tabsdata.connector.cdc.common._connector import (
    CdcIngester,
)
from tabsdata.connector.cdc.common._model import (
    CdcEvent,
    TrackedTable,
)
from tabsdata.connector.cdc.common.typing import QualifiedTablesSpec
from tabsdata.connector.cdc.db2._connector import Db2CdcConn
from tabsdata.connector.cdc.db2.typing import Db2CdcStartFromSpec

logger = logging.getLogger(__name__)
logger.setLevel(logging.DEBUG)


class SqlReplicationIngester(CdcIngester):
    def __init__(
        self,
        conn: Db2CdcConn,
        tables: QualifiedTablesSpec,
        start_from: Db2CdcStartFromSpec | None = None,
    ):
        super().__init__()
        self._conn = conn
        self._tables = tables
        self._start_from = start_from

    @property
    def conn(self) -> Db2CdcConn:
        return self._conn

    @property
    def tables(self) -> QualifiedTablesSpec:
        return self._tables

    @property
    def start_from(self) -> Db2CdcStartFromSpec | None:
        return self._start_from

    def open_session(self) -> Any:
        raise NotImplementedError

    def close_session(self, session: Any) -> None:
        raise NotImplementedError

    def fetch_schemas(self, session: Any) -> dict[str, TrackedTable]:
        raise NotImplementedError

    def poll_events(self, session: Any) -> list[CdcEvent]:
        raise NotImplementedError

    def retrieve_position(self) -> Any:
        raise NotImplementedError

    def save_position(self, position: Any) -> None:
        raise NotImplementedError
