#
# Copyright 2026 Tabs Data Inc.
#

from __future__ import annotations

import logging
import signal
import sys
import threading
from abc import abstractmethod
from collections.abc import Iterable
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

# noinspection PyProtectedMember
from tabsdata._io.connections.connections import Conn

# noinspection PyProtectedMember
from tabsdata._io.stage_trigger import (
    TRIGGER_RETURN_ON_END_ATTRIBUTE,
    StageTrigger,
    StageTriggerContext,
    StageTriggerStatus,
)
from tabsdata.connector.cdc.common._model import (
    CdcChange,
    CdcOffset,
    TrackedTable,
)
from tabsdata.connector.cdc.common._sentinel.buffer import BufferSentinel
from tabsdata.connector.cdc.common._sentinel.disk import DiskSentinel
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
from tabsdata.exceptions import ErrorCode, StageTriggerExecutionError

logger = logging.getLogger(__name__)
logger.setLevel(logging.DEBUG)


class CdcTrigger(StageTrigger):
    """
    Base `StageTrigger` for Change Data Capture (CDC) connectors.

    This abstract trigger provides the common polling loop, buffering, and staging
    logic for all CDC connectors. Database-specific subclasses implement the
    ingestion details for their respective replication protocols.
    """

    def __init__(
        self,
        conn: Conn,
        tables: QualifiedTablesSpec,
        cdc_format: CdcFormatSpec = CdcFormatSpec(),
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
        """
        Initialize a CdcTrigger instance.

        Args:
            conn: The CDC connection configuration.
            tables: Qualified table names to track for changes.
            cdc_format: The output format specification for CDC data.
            buffer_max_rows: Maximum rows in memory before flushing to the working
                directory.
            buffer_max_bytes: Maximum bytes in memory before flushing to the working
                directory.
            buffer_max_seconds: Maximum seconds before flushing memory data to the
                working directory.
            trigger_max_rows: Maximum total rows before staging from working directory
                to the stage location.
            trigger_max_bytes: Maximum total bytes before staging from working directory
                to the stage location.
            trigger_max_seconds: Maximum seconds before staging from working directory
                to the stage location.
            poll_interval_seconds: Interval in seconds between polling for new changes.
            start: Optional datetime for the trigger to start working.
            end: Optional datetime for the trigger to stop working.
        """
        super().__init__(start=start, end=end)
        self._conn = conn
        self._tables = tables
        self._cdc_format = cdc_format
        self._buffer_max_rows = buffer_max_rows
        self._buffer_max_bytes = buffer_max_bytes
        self._buffer_max_seconds = buffer_max_seconds
        self._trigger_max_rows = trigger_max_rows
        self._trigger_max_bytes = trigger_max_bytes
        self._trigger_max_seconds = trigger_max_seconds
        self._poll_interval_seconds = poll_interval_seconds
        self._buffer_sentinel: BufferSentinel | None = None

    @property
    def conn(self) -> Conn:
        """
        CDC connection configuration.
        """
        return self._conn

    @property
    def tables(self) -> list[str] | None:
        """
        Qualified table names being tracked for changes.
        """
        return self._tables

    @property
    def out(self) -> dict[int, str] | None:
        """
        Mapping of table indices to qualified table names.
        """
        if self._tables is None:
            return None
        return {idx: table for idx, table in enumerate(self._tables)}

    @property
    def cdc_format(self) -> CdcFormatSpec:
        """
        Output format specification for CDC data.
        """
        return self._cdc_format

    @property
    def buffer_max_rows(self) -> int:
        """
        Maximum rows in memory before flushing to the working directory.
        """
        return self._buffer_max_rows

    @property
    def buffer_max_bytes(self) -> int | None:
        """
        Maximum bytes in memory before flushing to the working directory.
        """
        return self._buffer_max_bytes

    @property
    def buffer_max_seconds(self) -> float:
        """
        Maximum seconds before flushing memory data to the working directory.
        """
        return self._buffer_max_seconds

    @property
    def trigger_max_rows(self) -> int | None:
        """
        Maximum total rows before staging from working directory to the stage location.
        """
        return self._trigger_max_rows

    @property
    def trigger_max_bytes(self) -> int | None:
        """
        Maximum total bytes before staging from working directory to the stage location.
        """
        return self._trigger_max_bytes

    @property
    def trigger_max_seconds(self) -> float:
        """
        Maximum seconds before staging from working directory to the stage location.
        """
        return self._trigger_max_seconds

    @property
    def poll_interval_seconds(self) -> float:
        """
        Interval in seconds between polling for new changes.
        """
        return self._poll_interval_seconds

    @property
    def _buffered_rows(self) -> int:
        if self._buffer_sentinel is None:
            return 0
        return self._buffer_sentinel._total_rows

    @abstractmethod
    def _open_session(self) -> Any: ...

    @abstractmethod
    def _close_session(self, session: Any) -> None: ...

    @abstractmethod
    def collect_tables(
        self,
        session: Any,
        cdc_format: CdcFormatSpec = CdcFormatSpec(),
    ) -> dict[str, TrackedTable]: ...

    @abstractmethod
    def _base_offset(self) -> CdcOffset: ...

    @abstractmethod
    def _commit_offset(self, offset: CdcOffset) -> None: ...

    @abstractmethod
    def _poll_changes(self, session: Any) -> Iterable[CdcChange]: ...

    @staticmethod
    def _setup_signal_handlers() -> threading.Event:
        stop_event = threading.Event()

        if threading.current_thread() is not threading.main_thread():
            return stop_event

        def handle_signal(signum, _frame):
            logger.info(f"Received termination signal ({signum}). Shutting down...")
            stop_event.set()

        signal.signal(signal.SIGINT, handle_signal)
        signal.signal(signal.SIGTERM, handle_signal)

        if sys.platform == "win32":
            signal.signal(signal.SIGBREAK, handle_signal)
            try:
                # noinspection PyPackageRequirements,PyUnresolvedReferences
                import win32api

                win32api.SetConsoleCtrlHandler(handle_signal, True)
            except ImportError:
                pass

        return stop_event

    def _await_start(self, stop_signal: threading.Event) -> None:
        if self._start is not None:
            delay = (self._start - datetime.now(timezone.utc)).total_seconds()
            if delay > 0:
                logger.info(f"CdcTrigger waiting {delay:.1f}s until start time.")
                stop_signal.wait(timeout=delay)

    def run(self, context: StageTriggerContext):
        job_status = StageTriggerStatus.SUCCESS
        buffer_sentinel = None
        disk_sentinel = None
        stop_signal = None
        database_connection = None
        try:
            stop_signal = self._setup_signal_handlers()
            database_connection = self._open_session()
            tracked_tables = self.collect_tables(
                session=database_connection,
                cdc_format=self._cdc_format,
            )
            disk_sentinel = DiskSentinel(
                context=context,
                landing=Path(context.working_dir),
                max_rows=self._trigger_max_rows,
                max_bytes=self._trigger_max_bytes,
                max_seconds=self._trigger_max_seconds,
                tables=tracked_tables,
            )
            buffer_sentinel = BufferSentinel(
                disk_sentinel=disk_sentinel,
                max_rows=self._buffer_max_rows,
                max_bytes=self._buffer_max_bytes,
                max_seconds=self._buffer_max_seconds,
                tables=tracked_tables,
                offset=self._base_offset(),
            )
            self._buffer_sentinel = buffer_sentinel

            self._await_start(
                stop_signal=stop_signal,
            )

            buffer_sentinel.reset_timer()
            disk_sentinel.reset_timer()

            while not stop_signal.is_set() and context.is_active():
                disk_sentinel.retry_trigger()
                buffer_sentinel.process_changes(
                    changes=self._poll_changes(database_connection),
                )
                buffer_sentinel.expire()
                if disk_sentinel.flush(force=False):
                    self._commit_offset(buffer_sentinel.offset)
                    context.checkpoint.write(buffer_sentinel.offset.upload())
                stop_signal.wait(
                    timeout=self._poll_interval_seconds,
                )
            if stop_signal.is_set():
                job_status = StageTriggerStatus.STOPPED
                logger.info("CdcTrigger interrupted; exiting loop.")
        except Exception as exception:
            job_status = StageTriggerStatus.ERROR
            logger.error(f"Error in CdcTrigger run loop: {exception}")
            raise
        finally:
            if stop_signal is not None and not stop_signal.is_set():
                if job_status == StageTriggerStatus.SUCCESS:
                    if buffer_sentinel is not None:
                        buffer_sentinel.flush()
                    if disk_sentinel is not None:
                        if disk_sentinel.flush(force=True):
                            self._commit_offset(buffer_sentinel.offset)
                            context.checkpoint.write(buffer_sentinel.offset.upload())
            if database_connection is not None:
                self._close_session(database_connection)
            if getattr(self, TRIGGER_RETURN_ON_END_ATTRIBUTE, False):
                if job_status != StageTriggerStatus.SUCCESS:
                    raise StageTriggerExecutionError(
                        ErrorCode.STR1,
                        f"CdcTrigger execution failed with status: {job_status.name}",
                    )
            else:
                sys.exit(job_status.value)
