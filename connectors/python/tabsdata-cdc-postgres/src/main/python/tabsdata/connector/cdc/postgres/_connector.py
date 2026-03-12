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
from tabsdata.connector.cdc.postgres._connection import PostgresCdcConn
from tabsdata.connector.cdc.postgres._ingester.wal2json import (
    DEFAULT_WAL2JSON_FORMAT_VERSION,
    Wal2JsonIngester,
)
from tabsdata.connector.cdc.postgres.typing import (
    PostgresCdcStartFromSpec,
    ReplicationSlotBehaviorSpec,
)
from tabsdata.exceptions import ErrorCode

logger = logging.getLogger(__name__)
logger.setLevel(logging.DEBUG)


@unstable()
@deprecated(
    """Class 'PostgresCdcTrigger' is considered unstable and is subject to change in future releases.""",  # noqa: E231,E241,E501
    category=unstable_category(
        reason=(
            "Class 'PostgresCdcTrigger' may undergo API and feature changes as it "
            "evolves toward full capability and stability."
        ),
        since="1.8.0",
        replacement="There is no current stable replacement for this class.",
    ),
)
class PostgresCdcTrigger(CdcTrigger):
    @_td_validate_call(ErrorCode.CDCPOSTGRES2)
    def __init__(
        self,
        conn: PostgresCdcConn,
        tables: QualifiedTablesSpec,
        start_from: PostgresCdcStartFromSpec,
        cdc_format: CdcFormatSpec = CdcFormatSpec(),
        buffer_max_rows: int = DEFAULT_BUFFER_MAX_ROWS,
        buffer_max_bytes: int | None = DEFAULT_BUFFER_MAX_BYTES,
        buffer_max_seconds: float = DEFAULT_BUFFER_MAX_SECONDS,
        trigger_max_rows: int | None = DEFAULT_TRIGGER_MAX_ROWS,
        trigger_max_bytes: int | None = DEFAULT_TRIGGER_MAX_BYTES,
        trigger_max_seconds: float = DEFAULT_TRIGGER_MAX_SECONDS,
        poll_interval_seconds: float = DEFAULT_POLL_INTERVAL_SECONDS,
        blocking_timeout_seconds: float = DEFAULT_BLOCKING_TIMEOUT_SECONDS,
        replication_slot: str | None = None,
        publication_name: str | None = None,
        slot_behavior: ReplicationSlotBehaviorSpec = "reuse",
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
        self._replication_slot = replication_slot
        self._publication_name = publication_name
        self._slot_behavior = slot_behavior
        self._ingester = Wal2JsonIngester(
            conn=conn,
            tables=tables,
            start_from=start_from,
            blocking_timeout_seconds=blocking_timeout_seconds,
            replication_slot=replication_slot,
            publication_name=publication_name,
            replication_slot_behavior=slot_behavior,
            wal2json_format_version=DEFAULT_WAL2JSON_FORMAT_VERSION,
        )

    @property
    def start_from(self) -> PostgresCdcStartFromSpec | None:
        return self._start_from

    @property
    def blocking_timeout_seconds(self) -> float:
        return self._blocking_timeout_seconds

    @property
    def replication_slot(self) -> str | None:
        return self._replication_slot

    @property
    def publication_name(self) -> str | None:
        return self._publication_name

    @property
    def slot_behavior(self) -> ReplicationSlotBehaviorSpec:
        return self._slot_behavior

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

    def _poll_changes(self, session: Any) -> Iterable[CdcChange]:
        return self._ingester.poll_changes(session)

    def _confirm_position(self) -> None:
        self._ingester.confirm_position()

    def _get_position(self) -> Any:
        return self._ingester.retrieve_position()

    def _save_position(self, position: Any) -> None:
        self._ingester.save_position(position)
