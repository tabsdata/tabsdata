#
# Copyright 2026 Tabs Data Inc.
#

from __future__ import annotations

import logging
from typing import Any

from tabsdata.connector.cdc.common._ingester import (
    CdcIngester,
)
from tabsdata.connector.cdc.common._model import (
    CdcEvent,
)
from tabsdata.connector.cdc.common.typing import QualifiedTablesSpec
from tabsdata.connector.cdc.db2._connector import Db2CdcConn
from tabsdata.connector.cdc.db2.typing import Db2CdcStartFromSpec
from tabsdata.connector.sql.common._database.db2.dialect import Db2Dialect

logger = logging.getLogger(__name__)
logger.setLevel(logging.DEBUG)


class SqlReplicationIngester(CdcIngester):
    def __init__(
        self,
        conn: Db2CdcConn,
        tables: QualifiedTablesSpec,
        start_from: Db2CdcStartFromSpec | None = None,
    ):
        super().__init__(tables)
        self._conn = conn
        self._start_from = start_from

    @property
    def conn(self) -> Db2CdcConn:
        return self._conn

    @property
    def start_from(self) -> Db2CdcStartFromSpec | None:
        return self._start_from

    @property
    def dialect(self) -> Db2Dialect:
        return Db2Dialect()

    def open_session(self) -> Any:
        return self._conn._connect()

    def close_session(self, session: Any) -> None:
        session.dispose()

    def poll_events(self, session: Any) -> list[CdcEvent]:
        raise NotImplementedError

    def retrieve_position(self) -> Any:
        raise NotImplementedError

    def save_position(self, position: Any) -> None:
        raise NotImplementedError
