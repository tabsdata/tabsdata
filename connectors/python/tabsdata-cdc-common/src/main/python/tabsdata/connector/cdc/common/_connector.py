#
# Copyright 2026 Tabs Data Inc.
#

from __future__ import annotations

import json
import logging
import signal
import sys
import time
from abc import ABC, abstractmethod
from dataclasses import dataclass, field
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

import pyarrow as pa
import pyarrow.parquet as pq

# noinspection PyProtectedMember
from tabsdata._io.connections.connections import Conn

# noinspection PyProtectedMember
from tabsdata._io.stage_trigger import (
    StageTrigger,
    StageTriggerContext,
    StageTriggerStatus,
)
from tabsdata.connector.cdc.common._model import (
    CdcDataChange,
    CdcEvent,
    TrackedTable,
)
from tabsdata.connector.cdc.common.constants import (
    CDC_DATA_COL_NEW_PREFIX,
    CDC_DATA_COL_OLD_PREFIX,
    CDC_DATA_COL_PREFIX,
    CDC_DATA_MAP_NEW,
    CDC_DATA_MAP_OLD,
    CDC_DATA_ROW_NEW,
    CDC_DATA_ROW_OLD,
    CDC_META_OP,
    CDC_META_SQ,
    CDC_META_TX,
    DEFAULT_BUFFER_MAX_BYTES,
    DEFAULT_BUFFER_MAX_ROWS,
    DEFAULT_BUFFER_MAX_SECONDS,
    DEFAULT_POLL_INTERVAL_SECONDS,
    DEFAULT_TRIGGER_MAX_BYTES,
    DEFAULT_TRIGGER_MAX_ROWS,
    DEFAULT_TRIGGER_MAX_SECONDS,
    DOT,
    OP_UPDATE,
)
from tabsdata.connector.cdc.common.typing import CdcFormatSpec, QualifiedTablesSpec
from tabsdata.exceptions import ErrorCode

logger = logging.getLogger(__name__)
logger.setLevel(logging.DEBUG)


class CdcIngester(ABC):
    def __init__(self):
        pass

    @abstractmethod
    def open_session(self) -> Any: ...

    @abstractmethod
    def close_session(self, session: Any) -> None: ...

    @abstractmethod
    def fetch_schemas(self, session: Any) -> dict[str, TrackedTable]: ...

    @abstractmethod
    def poll_events(self, session: Any) -> list[CdcEvent]: ...

    @abstractmethod
    def retrieve_position(self) -> Any: ...

    @abstractmethod
    def save_position(self, position: Any) -> None: ...


@dataclass
class _L1Buffer:
    rows: list[CdcDataChange] = field(default_factory=list)
    bytes: int = 0


@dataclass
class _L2Tracker:
    files: list[Path] = field(default_factory=list)
    rows: int = 0
    bytes: int = 0
    last_fired: float = field(default_factory=time.time)


def _build_parquet_schema(
    tracked_table: TrackedTable,
    cdc_format: CdcFormatSpec,
) -> pa.Schema:
    """Build a pyarrow schema for the parquet file of *tracked_table*.

    The layout depends on *cdc_format*:
      - Control columns are always first.
      - New values are flat ``@td.cdc.col.*`` when *flat_new_values* is True,
        otherwise stored according to *values_format*.
      - Old values are stored according to *values_format*.
    """
    fields: list[pa.Field] = [
        pa.field(CDC_META_OP, pa.utf8()),
        pa.field(CDC_META_TX, pa.utf8()),
        pa.field(CDC_META_SQ, pa.int64()),
    ]

    fmt = cdc_format.values_format

    # --- new values ---
    if cdc_format.flat_new_values:
        for col in tracked_table.columns:
            fields.append(pa.field(f"{CDC_DATA_COL_PREFIX}{DOT}{col.name}", col.patype))
    elif fmt == "columns":
        for col in tracked_table.columns:
            fields.append(
                pa.field(f"{CDC_DATA_COL_NEW_PREFIX}{DOT}{col.name}", col.patype)
            )
    elif fmt == "struct":
        struct_fields = [
            pa.field(col.name, col.patype) for col in tracked_table.columns
        ]
        fields.append(pa.field(CDC_DATA_ROW_NEW, pa.struct(struct_fields)))
    elif fmt == "map":
        fields.append(pa.field(CDC_DATA_MAP_NEW, pa.utf8()))

    # --- old values ---
    if fmt == "columns":
        prefix = CDC_DATA_COL_OLD_PREFIX
        for col in tracked_table.columns:
            fields.append(
                pa.field(f"{prefix}{DOT}{col.name}", col.patype, nullable=True)
            )
    elif fmt == "struct":
        struct_fields = [
            pa.field(col.name, col.patype) for col in tracked_table.columns
        ]
        fields.append(
            pa.field(CDC_DATA_ROW_OLD, pa.struct(struct_fields), nullable=True)
        )
    elif fmt == "map":
        fields.append(pa.field(CDC_DATA_MAP_OLD, pa.utf8(), nullable=True))

    return pa.schema(fields)


def _row_to_record(
    change: CdcDataChange,
    tracked_table: TrackedTable,
    cdc_format: CdcFormatSpec,
) -> dict[str, Any]:
    """Convert a ``CdcChange`` into a flat dict matching the parquet schema."""
    record: dict[str, Any] = {
        CDC_META_OP: change.operation,
        CDC_META_TX: change.transaction,
        CDC_META_SQ: change.sequence,
    }

    fmt = cdc_format.values_format

    # --- new values ---
    if cdc_format.flat_new_values:
        for col in tracked_table.columns:
            record[f"{CDC_DATA_COL_PREFIX}{DOT}{col.name}"] = change.new_values.get(
                col.name
            )
    elif fmt == "columns":
        for col in tracked_table.columns:
            record[f"{CDC_DATA_COL_NEW_PREFIX}{DOT}{col.name}"] = change.new_values.get(
                col.name
            )
    elif fmt == "struct":
        record[CDC_DATA_ROW_NEW] = {
            col.name: change.new_values.get(col.name) for col in tracked_table.columns
        }
    elif fmt == "map":
        record[CDC_DATA_MAP_NEW] = json.dumps({
            col.name: change.new_values.get(col.name) for col in tracked_table.columns
        })

    # --- old values ---
    has_old = change.operation == OP_UPDATE and change.old_values
    if fmt == "columns":
        prefix = CDC_DATA_COL_OLD_PREFIX
        if has_old:
            for col in tracked_table.columns:
                record[f"{prefix}{DOT}{col.name}"] = change.old_values.get(col.name)
        else:
            for col in tracked_table.columns:
                record[f"{prefix}{DOT}{col.name}"] = None
    elif fmt == "struct":
        if has_old:
            record[CDC_DATA_ROW_OLD] = {
                col.name: change.old_values.get(col.name)
                for col in tracked_table.columns
            }
        else:
            record[CDC_DATA_ROW_OLD] = None
    elif fmt == "map":
        if has_old:
            record[CDC_DATA_MAP_OLD] = json.dumps({
                col.name: change.old_values.get(col.name)
                for col in tracked_table.columns
            })
        else:
            record[CDC_DATA_MAP_OLD] = None

    return record


def _estimate_row_bytes(change: CdcDataChange) -> int:
    """Rough byte estimate for a single change — used for L1 threshold checks."""
    size = 0
    for v in change.new_values.values():
        if isinstance(v, str):
            size += len(v)
        elif isinstance(v, bytes):
            size += len(v)
        else:
            size += 8  # numeric types
    if change.old_values:
        for v in change.old_values.values():
            if isinstance(v, str):
                size += len(v)
            elif isinstance(v, bytes):
                size += len(v)
            else:
                size += 8
    return size


class CdcTrigger(StageTrigger):
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
        self._start = start
        self._end = end

    @property
    def conn(self) -> Conn:
        return self._conn

    @property
    def tables(self) -> list[str] | None:
        return self._tables

    @property
    def cdc_format(self) -> CdcFormatSpec:
        return self._cdc_format

    @property
    def buffer_max_rows(self) -> int:
        return self._buffer_max_rows

    @property
    def buffer_max_bytes(self) -> int | None:
        return self._buffer_max_bytes

    @property
    def buffer_max_seconds(self) -> float:
        return self._buffer_max_seconds

    @property
    def trigger_max_rows(self) -> int | None:
        return self._trigger_max_rows

    @property
    def trigger_max_bytes(self) -> int | None:
        return self._trigger_max_bytes

    @property
    def trigger_max_seconds(self) -> float:
        return self._trigger_max_seconds

    @property
    def start(self) -> datetime | None:
        return self._start

    @property
    def poll_interval_seconds(self) -> float:
        return self._poll_interval_seconds

    @property
    def end(self) -> datetime | None:
        return self._end

    @abstractmethod
    def _open_session(self) -> Any: ...

    @abstractmethod
    def _close_session(self, session: Any) -> None: ...

    @abstractmethod
    def _fetch_tracked_tables(self, session: Any) -> dict[str, TrackedTable]: ...

    @abstractmethod
    def _poll_events(self, session: Any) -> list[CdcEvent]: ...

    @abstractmethod
    def _get_position(self) -> Any: ...

    @abstractmethod
    def _save_position(self, position: Any) -> None: ...

    @staticmethod
    def _setup_sigterm_handler():
        def _sigterm_handler(_signum, _frame):
            raise KeyboardInterrupt

        signal.signal(signal.SIGTERM, _sigterm_handler)

    def run(self, context: StageTriggerContext):
        self._setup_sigterm_handler()
        session = self._open_session()
        stage_trigger_status = StageTriggerStatus.SUCCESS

        try:
            tracked_tables = self._fetch_tracked_tables(session)
            pa_schemas: dict[str, pa.Schema] = {
                name: _build_parquet_schema(ts, self._cdc_format)
                for name, ts in tracked_tables.items()
            }

            # L1: per-table memory buffers
            l1: dict[str, _L1Buffer] = {name: _L1Buffer() for name in tracked_tables}
            # L2: accumulated file tracker
            l2 = _L2Tracker()
            # Sequence counter per table (for file naming)
            file_seq: dict[str, int] = {name: 0 for name in tracked_tables}

            working_dir = Path(context.working_dir)

            while True:
                if self._end is not None and datetime.now(timezone.utc) > self._end:
                    logger.info("CdcTrigger end time reached, exiting loop.")
                    break

                context.trigger_monitor()

                events = self._poll_events(session)

                for event in events:
                    if isinstance(event, CdcDataChange):
                        self._append_to_l1(event, l1, tracked_tables)

                # L1 buffer check (per-table)
                self._check_l1_flush(
                    l1, l2, tracked_tables, pa_schemas, file_seq, working_dir
                )

                # L2 fire check
                self._check_l2_fire(
                    context, l1, l2, tracked_tables, pa_schemas, file_seq, working_dir
                )

        except KeyboardInterrupt:
            stage_trigger_status = StageTriggerStatus.STOPPED
            logger.info("CdcTrigger interrupted, exiting loop.")
        except Exception as e:
            stage_trigger_status = StageTriggerStatus.ERROR
            logger.error(f"Error in CdcTrigger run loop: {e}")
            raise
        finally:
            self._final_flush_and_fire(
                context, l1, l2, tracked_tables, pa_schemas, file_seq, working_dir
            )
            self._close_session(session)
            if getattr(self, "return_on_end", False):
                if stage_trigger_status != StageTriggerStatus.SUCCESS:
                    from tabsdata.exceptions import StageTriggerExecutionError

                    raise StageTriggerExecutionError(
                        ErrorCode.STR1,
                        f"CdcTrigger execution failed with status:"
                        f" {stage_trigger_status.name}",
                    )
            else:
                sys.exit(stage_trigger_status.value)

    @staticmethod
    def _append_to_l1(
        change: CdcDataChange,
        l1: dict[str, _L1Buffer],
        tracked_tables: dict[str, TrackedTable],
    ):
        table = change.table
        if table not in l1:
            logger.warning(f"Received change for unknown table {table!r}, skipping.")
            return
        buf = l1[table]
        buf.rows.append(change)
        buf.bytes += _estimate_row_bytes(change)

    def _check_l1_flush(
        self,
        l1: dict[str, _L1Buffer],
        l2: _L2Tracker,
        tracked_tables: dict[str, TrackedTable],
        pa_schemas: dict[str, pa.Schema],
        file_seq: dict[str, int],
        working_dir: Path,
    ):
        for table_name, buf in l1.items():
            if not buf.rows:
                continue
            rows_hit = len(buf.rows) >= self._buffer_max_rows
            bytes_hit = (
                self._buffer_max_bytes is not None
                and buf.bytes >= self._buffer_max_bytes
            )
            if rows_hit or bytes_hit:
                self._flush_l1_table(
                    table_name,
                    buf,
                    l2,
                    tracked_tables[table_name],
                    pa_schemas[table_name],
                    file_seq,
                    working_dir,
                    self._cdc_format,
                )

    @staticmethod
    def _flush_l1_table(
        table_name: str,
        buf: _L1Buffer,
        l2: _L2Tracker,
        tracked_table: TrackedTable,
        pa_schema: pa.Schema,
        file_seq: dict[str, int],
        working_dir: Path,
        cdc_format: CdcFormatSpec = CdcFormatSpec(),
    ):
        if not buf.rows:
            return
        records = [_row_to_record(c, tracked_table, cdc_format) for c in buf.rows]
        table = pa.Table.from_pylist(records, schema=pa_schema)

        seq = file_seq[table_name]
        file_seq[table_name] = seq + 1
        file_name = f"{table_name}_{seq:08d}.parquet"
        file_path = working_dir / file_name
        pq.write_table(table, file_path)

        file_size = file_path.stat().st_size
        l2.files.append(file_path)
        l2.rows += len(buf.rows)
        l2.bytes += file_size

        logger.debug(
            f"L1 flush: {table_name} → {file_name} "
            f"({len(buf.rows)} rows, {file_size} bytes)"
        )

        buf.rows.clear()
        buf.bytes = 0

    def _check_l2_fire(
        self,
        context: StageTriggerContext,
        l1: dict[str, _L1Buffer],
        l2: _L2Tracker,
        tracked_tables: dict[str, TrackedTable],
        pa_schemas: dict[str, pa.Schema],
        file_seq: dict[str, int],
        working_dir: Path,
    ):
        rows_hit = (
            self._trigger_max_rows is not None and l2.rows >= self._trigger_max_rows
        )
        bytes_hit = (
            self._trigger_max_bytes is not None and l2.bytes >= self._trigger_max_bytes
        )
        time_hit = (time.time() - l2.last_fired) >= self._fire_max_seconds

        if not (rows_hit or bytes_hit or time_hit):
            return
        # Only fire if there is data (files or non-empty L1 buffers)
        has_data = l2.files or any(buf.rows for buf in l1.values())
        if not has_data:
            # Reset timer even if nothing to fire
            l2.last_fired = time.time()
            return

        self._fire_l2(
            context, l1, l2, tracked_tables, pa_schemas, file_seq, working_dir
        )

    def _fire_l2(
        self,
        context: StageTriggerContext,
        l1: dict[str, _L1Buffer],
        l2: _L2Tracker,
        tracked_tables: dict[str, TrackedTable],
        pa_schemas: dict[str, pa.Schema],
        file_seq: dict[str, int],
        working_dir: Path,
    ):
        # Flush all remaining L1 buffers first
        for table_name, buf in l1.items():
            self._flush_l1_table(
                table_name,
                buf,
                l2,
                tracked_tables[table_name],
                pa_schemas[table_name],
                file_seq,
                working_dir,
                self._cdc_format,
            )

        if l2.files:
            logger.debug(
                f"L2 fire: staging {len(l2.files)} files "
                f"({l2.rows} rows, {l2.bytes} bytes)"
            )
            context.stage(l2.files)
            context.trigger()
            self._save_position(self._get_position())

        # Reset L2
        l2.files.clear()
        l2.rows = 0
        l2.bytes = 0
        l2.last_fired = time.time()

    def _final_flush_and_fire(
        self,
        context: StageTriggerContext,
        l1: dict[str, _L1Buffer],
        l2: _L2Tracker,
        tracked_tables: dict[str, TrackedTable],
        pa_schemas: dict[str, pa.Schema],
        file_seq: dict[str, int],
        working_dir: Path,
    ):
        """Flush remaining L1 buffers and fire L2 if there are any files."""
        for table_name, buf in l1.items():
            self._flush_l1_table(
                table_name,
                buf,
                l2,
                tracked_tables[table_name],
                pa_schemas[table_name],
                file_seq,
                working_dir,
                self._cdc_format,
            )
        if l2.files:
            logger.debug(
                f"Final L2 fire: staging {len(l2.files)} files "
                f"({l2.rows} rows, {l2.bytes} bytes)"
            )
            context.stage(l2.files)
            context.trigger()
            self._save_position(self._get_position())
