#
# Copyright 2026 Tabs Data Inc.
#

from __future__ import annotations

import logging
from typing import Any

from tabsdata.connector.cdc.common._ingester import CdcIngester
from tabsdata.connector.cdc.common._model import (
    CdcChange,
)
from tabsdata.connector.cdc.common.constants import DEFAULT_BLOCKING_TIMEOUT_SECONDS
from tabsdata.connector.cdc.common.typing import QualifiedTablesSpec
from tabsdata.connector.cdc.postgres._connection import PostgresCdcConn
from tabsdata.connector.cdc.postgres.typing import (
    PostgresCdcStartFromSpec,
    SlotBehaviorSpec,
)
from tabsdata.connector.sql.common._database.postgres.dialect import PostgresDialect

logger = logging.getLogger(__name__)
logger.setLevel(logging.DEBUG)


class Wal2JsonIngester(CdcIngester):
    def __init__(
        self,
        conn: PostgresCdcConn,
        tables: QualifiedTablesSpec,
        start_from: PostgresCdcStartFromSpec | None = None,
        blocking_timeout_seconds: float = DEFAULT_BLOCKING_TIMEOUT_SECONDS,
        replication_slot: str | None = None,
        publication_name: str | None = None,
        slot_behavior: SlotBehaviorSpec = "reuse",
    ):
        super().__init__(tables)
        self._conn = conn
        self._start_from = start_from
        self._blocking_timeout_seconds = blocking_timeout_seconds
        self._replication_slot = replication_slot
        self._publication_name = publication_name
        self._slot_behavior = slot_behavior

    @property
    def conn(self) -> PostgresCdcConn:
        return self._conn

    @property
    def start_from(self) -> PostgresCdcStartFromSpec | None:
        return self._start_from

    @property
    def blocking_timeout_seconds(self) -> float | None:
        return self._blocking_timeout_seconds

    @property
    def replication_slot(self) -> str | None:
        return self._replication_slot

    @property
    def publication_name(self) -> str | None:
        return self._publication_name

    @property
    def slot_behavior(self) -> SlotBehaviorSpec:
        return self._slot_behavior

    @property
    def dialect(self) -> PostgresDialect:
        return PostgresDialect()

    def open_session(self) -> Any:
        return self._conn._connect()

    def close_session(self, session: Any) -> None:
        session.dispose()

    def poll_changes(self, session: Any) -> list[CdcChange]:
        raise NotImplementedError

    def retrieve_position(self) -> Any:
        raise NotImplementedError

    def save_position(self, position: Any) -> None:
        raise NotImplementedError
