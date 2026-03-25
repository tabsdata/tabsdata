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
from tabsdata.connector.cdc.common.typing import CdcFormat, QualifiedTablesSpec
from tabsdata.connector.cdc.db2._connection import Db2CdcConn
from tabsdata.connector.cdc.db2._ingester.sqlreplica import SqlReplicationIngester
from tabsdata.connector.cdc.db2.typing import Db2CdcStartFromSpec
from tabsdata.exceptions import ErrorCode

logger = logging.getLogger(__name__)
logger.setLevel(logging.DEBUG)


@unstable()
@deprecated(
    """Class 'Db2CdcTrigger' is considered unstable and is subject to change in future releases.""",  # noqa: E231,E241,E501
    category=unstable_category(
        reason=(
            "Class 'Db2CdcTrigger' may undergo API and feature changes as it "
            "evolves toward full capability and stability."
        ),
        since="1.8.0",
        replacement="There is no current stable replacement for this class.",
    ),
)
class Db2CdcTrigger(CdcTrigger):
    """
    :categories: trigger

    A `CdcTrigger` that captures changes from a Db2 database using SQL Replication.

    This trigger connects to a Db2 server, reads change data capture events (inserts,
    updates, deletes) on specified tables via SQL Replication, and stages them.
    """

    @_td_validate_call(ErrorCode.CDCDB22)
    def __init__(
        self,
        conn: Db2CdcConn,
        tables: QualifiedTablesSpec,
        start_from: Db2CdcStartFromSpec,
        cdc_format: CdcFormat = CdcFormat(),
        buffer_max_rows: int = DEFAULT_BUFFER_MAX_ROWS,
        buffer_max_bytes: int | None = DEFAULT_BUFFER_MAX_BYTES,
        buffer_max_secs: int = DEFAULT_BUFFER_MAX_SECS,
        trigger_max_rows: int | None = DEFAULT_TRIGGER_MAX_ROWS,
        trigger_max_bytes: int | None = DEFAULT_TRIGGER_MAX_BYTES,
        trigger_max_secs: int = DEFAULT_TRIGGER_MAX_SECS,
        poll_interval_seconds: int = DEFAULT_POLL_INTERVAL_SECONDS,
        blocking_timeout_seconds: int = DEFAULT_BLOCKING_TIMEOUT_SECONDS,
        start: datetime | None = None,
        end: datetime | None = None,
    ):
        """
        Initialize a Db2CdcTrigger instance.

        Args:
            conn: The Db2 CDC connection configuration.
            tables: Qualified table names to track for changes.
            start_from: The position to start reading changes from (commit sequence,
                per-table commit sequences, timestamp, or relative position).
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
                replication stream.
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
        self._setup()

    def _setup(self):
        super()._setup()
        self._ingester = SqlReplicationIngester(
            conn=self._conn,
            tables=self._tables,
            start_from=self._start_from,
            blocking_timeout_seconds=self._blocking_timeout_seconds,
        )

    def __getstate__(self):
        state = super().__getstate__()
        state.pop("_ingester", None)
        return state

    @property
    def start_from(self) -> Db2CdcStartFromSpec | None:
        """
        Position to start reading changes from.
        """
        return self._start_from

    @property
    def blocking_timeout_seconds(self) -> int:
        """
        Timeout in seconds for blocking reads from the replication stream.
        """
        return self._blocking_timeout_seconds

    def _open_session(self) -> Any:
        return self._ingester.open_session()

    def _close_session(self, session: Any) -> None:
        self._ingester.close_session(session)

    def collect_tables(
        self,
        session: Any,
        cdc_format: CdcFormat = CdcFormat(),
    ) -> dict[str, TrackedTable]:
        return self._ingester.collect_tables(session, cdc_format)

    def harvest_tables(
        self,
        session: Any,
        cdc_format: CdcFormat = CdcFormat(),
    ) -> dict[str, TrackedTable]:
        return self._ingester.harvest_tables(session, cdc_format)

    def _base_offset(self) -> CdcOffset:
        return self._ingester.base_offset()

    def _commit_offset(self, offset: CdcOffset) -> None:
        self._ingester.commit_offset(offset)

    def _poll_changes(self, session: Any) -> Iterable[CdcChange]:
        return self._ingester.poll_changes(session)
