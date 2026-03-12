#
# Copyright 2026 Tabs Data Inc.
#

from __future__ import annotations

import logging
from datetime import datetime
from typing import Any

from typing_extensions import deprecated

from tabsdata._utils.annotations import unstable, unstable_category

# noinspection PyProtectedMember
from tabsdata._utils.validation import _td_validate_call
from tabsdata.connector.cdc.common._connector import CdcTrigger
from tabsdata.connector.cdc.common._model import (
    CdcEvent,
    TrackedTable,
)
from tabsdata.connector.cdc.common.constants import (
    DEFAULT_BUFFER_MAX_BYTES,
    DEFAULT_BUFFER_MAX_ROWS,
    DEFAULT_BUFFER_MAX_SECONDS,
    DEFAULT_POLL_INTERVAL_SECONDS,
    DEFAULT_TRIGGER_MAX_BYTES,
    DEFAULT_TRIGGER_MAX_ROWS,
    DEFAULT_TRIGGER_MAX_SECONDS,
)
from tabsdata.connector.cdc.common.typing import CdcFormatSpec, QualifiedTablesSpec
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
    @_td_validate_call(ErrorCode.STCE108)
    def __init__(
        self,
        conn: Db2CdcConn,
        tables: QualifiedTablesSpec,
        cdc_format: CdcFormatSpec = CdcFormatSpec(),
        start_from: Db2CdcStartFromSpec | None = None,
        buffer_max_rows: int = DEFAULT_BUFFER_MAX_ROWS,
        buffer_max_bytes: int | None = DEFAULT_BUFFER_MAX_BYTES,
        buffer_max_seconds: float = DEFAULT_BUFFER_MAX_SECONDS,
        trigger_max_rows: int | None = DEFAULT_TRIGGER_MAX_ROWS,
        trigger_max_bytes: int | None = DEFAULT_TRIGGER_MAX_BYTES,
        trigger_max_seconds: float = DEFAULT_TRIGGER_MAX_SECONDS,
        poll_interval_seconds: float = DEFAULT_POLL_INTERVAL_SECONDS,
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
        self._ingester = SqlReplicationIngester(
            conn=conn,
            tables=tables,
            start_from=start_from,
        )

    @property
    def start_from(self) -> Db2CdcStartFromSpec | None:
        return self._start_from

    def _open_session(self) -> Any:
        return self._ingester.open_session()

    def _close_session(self, session: Any) -> None:
        self._ingester.close_session(session)

    def _fetch_tracked_tables(self, session: Any) -> dict[str, TrackedTable]:
        return self._ingester.fetch_schemas(session)

    def _poll_events(self, session: Any) -> list[CdcEvent]:
        return self._ingester.poll_events(session)

    def _get_position(self) -> Any:
        return self._ingester.retrieve_position()

    def _save_position(self, position: Any) -> None:
        self._ingester.save_position(position)
