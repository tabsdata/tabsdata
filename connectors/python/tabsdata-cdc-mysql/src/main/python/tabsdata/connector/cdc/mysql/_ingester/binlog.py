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
from tabsdata.connector.cdc.common.constants import (
    DEFAULT_BLOCKING_TIMEOUT_SECONDS,
)
from tabsdata.connector.cdc.common.typing import QualifiedTablesSpec
from tabsdata.connector.cdc.mysql._connection import MySQLCdcConn
from tabsdata.connector.cdc.mysql.typing import MySQLCdcStartFromSpec

logger = logging.getLogger(__name__)
logger.setLevel(logging.DEBUG)


class BinlogIngester(CdcIngester):
    def __init__(
        self,
        conn: MySQLCdcConn,
        tables: QualifiedTablesSpec,
        start_from: MySQLCdcStartFromSpec | None = None,
        blocking_timeout_seconds: float = DEFAULT_BLOCKING_TIMEOUT_SECONDS,
        server_id: int = 100,
    ):
        super().__init__()
        self._conn = conn
        self._tables = tables
        self._start_from = start_from
        self._blocking_timeout_seconds = blocking_timeout_seconds
        self._server_id = server_id

    @property
    def conn(self) -> MySQLCdcConn:
        return self._conn

    @property
    def tables(self) -> QualifiedTablesSpec:
        return self._tables

    @property
    def start_from(self) -> MySQLCdcStartFromSpec | None:
        return self._start_from

    @property
    def blocking_timeout_seconds(self) -> float | None:
        return self._blocking_timeout_seconds

    @property
    def server_id(self) -> int:
        return self._server_id

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
