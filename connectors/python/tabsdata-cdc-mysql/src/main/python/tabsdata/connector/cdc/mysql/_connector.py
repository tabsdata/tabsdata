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
    DEFAULT_BUFFER_MAX_SECS,
    DEFAULT_POLL_INTERVAL_SECONDS,
    DEFAULT_TRIGGER_MAX_BYTES,
    DEFAULT_TRIGGER_MAX_ROWS,
    DEFAULT_TRIGGER_MAX_SECS,
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
    """
    :categories: trigger

    A `CdcTrigger` that captures changes from a MySQL database using binlog replication.

    This trigger connects to a MySQL server, reads the binary log for change data
    capture events (inserts, updates, deletes) on specified tables, and stages them.
    """

    @_td_validate_call(ErrorCode.CDCMYSQL2)
    def __init__(
        self,
        conn: MySQLCdcConn,
        tables: QualifiedTablesSpec,
        start_from: MySQLCdcStartFromSpec,
        cdc_format: CdcFormatSpec = CdcFormatSpec(),
        buffer_max_rows: int = DEFAULT_BUFFER_MAX_ROWS,
        buffer_max_bytes: int | None = DEFAULT_BUFFER_MAX_BYTES,
        buffer_max_secs: int = DEFAULT_BUFFER_MAX_SECS,
        trigger_max_rows: int | None = DEFAULT_TRIGGER_MAX_ROWS,
        trigger_max_bytes: int | None = DEFAULT_TRIGGER_MAX_BYTES,
        trigger_max_secs: int = DEFAULT_TRIGGER_MAX_SECS,
        poll_interval_seconds: int = DEFAULT_POLL_INTERVAL_SECONDS,
        blocking_timeout_seconds: int = DEFAULT_BLOCKING_TIMEOUT_SECONDS,
        server_id: int = DEFAULT_SERVER_ID,
        start: datetime | None = None,
        end: datetime | None = None,
    ):
        """
        Initialize a MySQLCdcTrigger instance.

        Args:
            conn: The MySQL CDC connection configuration.
            tables: Qualified table names to track for changes.
            start_from: The position to start reading changes from (GTID, binlog
                position, timestamp, or relative position).
            cdc_format: The output format specification for CDC data.
            buffer_max_rows: Maximum rows in memory before flushing to the working
                directory.
            buffer_max_bytes: Maximum bytes in memory before flushing to the working
                directory.
            buffer_max_secs: Maximum seconds before flushing memory data to the
                working directory.
            trigger_max_rows: Maximum total rows before staging from working directory
                to the stage location.
            trigger_max_bytes: Maximum total bytes before staging from working directory
                to the stage location.
            trigger_max_secs: Maximum seconds before staging from working directory
                to the stage location.
            poll_interval_seconds: Interval in seconds between polling for new changes.
            blocking_timeout_seconds: Timeout in seconds for blocking reads from the
                binlog stream.
            server_id: The MySQL server ID for binlog replication. Defaults to 512.
            start: Optional datetime for the trigger to start working.
            end: Optional datetime for the trigger to stop working.
        """
        super().__init__(
            conn=conn,
            tables=tables,
            cdc_format=cdc_format,
            buffer_max_rows=buffer_max_rows,
            buffer_max_bytes=buffer_max_bytes,
            buffer_max_secs=buffer_max_secs,
            trigger_max_rows=trigger_max_rows,
            trigger_max_bytes=trigger_max_bytes,
            trigger_max_secs=trigger_max_secs,
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
        """
        Position to start reading changes from.
        """
        return self._start_from

    @property
    def blocking_timeout_seconds(self) -> int:
        """
        Timeout in seconds for blocking reads from the binlog stream.
        """
        return self._blocking_timeout_seconds

    @property
    def server_id(self) -> int:
        """
        MySQL server ID for binlog replication.
        """
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
