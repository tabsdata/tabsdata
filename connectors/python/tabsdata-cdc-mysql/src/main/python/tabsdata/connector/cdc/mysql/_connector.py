#
# Copyright 2026 Tabs Data Inc.
#

from __future__ import annotations

import logging
from collections.abc import Iterable
from datetime import datetime
from typing import Any

from typing_extensions import deprecated

from tabsdata._utils.annotations import unstable, unstable_category

# noinspection PyProtectedMember
from tabsdata._utils.validation import _td_validate_call
from tabsdata.connector.cdc.common._connector import CdcTrigger
from tabsdata.connector.cdc.common._model import (
    CdcChange,
    CdcOffset,
    TrackedTable,
)
from tabsdata.connector.cdc.common.constants import (
    DEFAULT_BLOCKING_TIMEOUT_SECONDS,
    DEFAULT_BUFFER_MAX_BYTES,
    DEFAULT_BUFFER_MAX_ROWS,
    DEFAULT_BUFFER_MAX_SECONDS,
    DEFAULT_POLL_INTERVAL_SECONDS,
    DEFAULT_TRIGGER_MAX_BYTES,
    DEFAULT_TRIGGER_MAX_ROWS,
    DEFAULT_TRIGGER_MAX_SECONDS,
)
from tabsdata.connector.cdc.common.typing import CdcFormatSpec, QualifiedTablesSpec
from tabsdata.connector.cdc.mysql._connection import MySQLCdcConn
from tabsdata.connector.cdc.mysql._ingester.binlog import BinlogIngester
from tabsdata.connector.cdc.mysql.constants import DEFAULT_SERVER_ID
from tabsdata.connector.cdc.mysql.typing import MySQLCdcStartFromSpec
from tabsdata.exceptions import ErrorCode

logger = logging.getLogger(__name__)
logger.setLevel(logging.DEBUG)


@unstable()
@deprecated(
    """Class 'MySQLCdcTrigger' is considered unstable and is subject to change in future releases.""",  # noqa: E231,E241,E501
    category=unstable_category(
        reason=(
            "Class 'MySQLCdcTrigger' may undergo API and feature changes as it "
            "evolves toward full capability and stability."
        ),
        since="1.8.0",
        replacement="There is no current stable replacement for this class.",
    ),
)
class MySQLCdcTrigger(CdcTrigger):
    @_td_validate_call(ErrorCode.CDCMYSQL2)
    def __init__(
        self,
        conn: MySQLCdcConn,
        tables: QualifiedTablesSpec,
        start_from: MySQLCdcStartFromSpec,
        cdc_format: CdcFormatSpec = CdcFormatSpec(),
        buffer_max_rows: int = DEFAULT_BUFFER_MAX_ROWS,
        buffer_max_bytes: int | None = DEFAULT_BUFFER_MAX_BYTES,
        buffer_max_seconds: float = DEFAULT_BUFFER_MAX_SECONDS,
        trigger_max_rows: int | None = DEFAULT_TRIGGER_MAX_ROWS,
        trigger_max_bytes: int | None = DEFAULT_TRIGGER_MAX_BYTES,
        trigger_max_seconds: float = DEFAULT_TRIGGER_MAX_SECONDS,
        poll_interval_seconds: float = DEFAULT_POLL_INTERVAL_SECONDS,
        blocking_timeout_seconds: float = DEFAULT_BLOCKING_TIMEOUT_SECONDS,
        server_id: int = DEFAULT_SERVER_ID,
        start: datetime | None = None,
        end: datetime | None = None,
    ):
        super().__init__(
            conn=conn,
            tables=tables,
            cdc_format=cdc_format,
            buffer_max_rows=buffer_max_rows,
            buffer_max_bytes=buffer_max_bytes,
            buffer_max_seconds=buffer_max_seconds,
            trigger_max_rows=trigger_max_rows,
            trigger_max_bytes=trigger_max_bytes,
            trigger_max_seconds=trigger_max_seconds,
            poll_interval_seconds=poll_interval_seconds,
            start=start,
            end=end,
        )
        self._start_from = start_from
        self._blocking_timeout_seconds = blocking_timeout_seconds
        self._server_id = server_id
        self._ingester = BinlogIngester(
            conn=conn,
            tables=tables,
            start_from=start_from,
            blocking_timeout_seconds=blocking_timeout_seconds,
            server_id=server_id,
        )

    @property
    def start_from(self) -> MySQLCdcStartFromSpec | None:
        return self._start_from

    @property
    def blocking_timeout_seconds(self) -> float:
        return self._blocking_timeout_seconds

    @property
    def server_id(self) -> int:
        return self._server_id

    def _open_session(self) -> Any:
        return self._ingester.open_session()

    def _close_session(self, session: Any) -> None:
        self._ingester.close_session(session)

    def collect_tables(
        self,
        session: Any,
        cdc_format: CdcFormatSpec = CdcFormatSpec(),
    ) -> dict[str, TrackedTable]:
        return self._ingester.collect_tables(session, cdc_format)

    def _base_offset(self) -> CdcOffset:
        return self._ingester.base_offset()

    def _commit_offset(self, offset: CdcOffset) -> None:
        self._ingester.commit_offset(offset)

    def _poll_changes(self, session: Any) -> Iterable[CdcChange]:
        return self._ingester.poll_changes(session)
