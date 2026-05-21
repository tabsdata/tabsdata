"""Disk-side CDC sentinel.

Receives flushed buffers from the in-memory ``BufferSentinel`` and
materialises them as parquet files in the runner's working dir.
On its own row/byte/time threshold, it builds a
``StreamManifestSpec``, hands it to ``StreamSrcPluginCtx.stage`` to
deposit the files atomically, commits the staged ref, and signals
the downstream trigger.

The disk sentinel does NOT call ``ctx.read_state`` /
``ctx.write_state`` -- offset persistence is the runner's job and
flows through the buffer sentinel's running ``offset``."""

from __future__ import annotations

import logging
import time
from pathlib import Path
from uuid import uuid4

import pyarrow as pa
import pyarrow.parquet as pq

from tabsdatak.conn.common.sql.cdc._model import CdcDataChange, TrackedTable
from tabsdatak.conn.common.sql.cdc._sentinel import CdcBuffer, row_to_record
from tabsdatak.conn.common.sql.cdc._utils import sanitize_file_name
from tabsdatak.conn.common.sql.cdc.typing import CdcFormat
from tabsdatak.spi import ParquetFile, StreamSrcPluginCtx, TableManifest

logger = logging.getLogger(__name__)


class DiskSentinel:
    def __init__(
        self,
        ctx: StreamSrcPluginCtx,
        landing: Path,
        max_rows: int | None,
        max_bytes: int | None,
        max_secs: int,
        tables: dict[str, TrackedTable],
        cdc_format: CdcFormat,
    ):
        self._ctx = ctx
        self._landing = landing
        self._max_rows = max_rows
        self._max_bytes = max_bytes
        self._max_secs = max_secs
        self._tables = tables
        self._cdc_format = cdc_format
        # Per-table list of parquet file paths accumulated since
        # last stage call.
        self._files: dict[str, list[Path]] = {table: [] for table in tables}
        self._total_rows: int = 0
        self._total_bytes: int = 0
        self._last_flushed: float = time.time()
        # Set when ``ctx.trigger()`` returned False (downstream not
        # ready); the next ``retry_trigger`` call polls
        # ``ctx.trigger_monitor()`` to resume.
        self._trigger_pending: bool = False

    @property
    def is_empty(self) -> bool:
        return self._total_rows == 0

    @property
    def has_data(self) -> bool:
        return self._total_rows > 0

    @property
    def trigger_pending(self) -> bool:
        return self._trigger_pending

    def reset_timer(self) -> None:
        self._last_flushed = time.time()

    def retry_trigger(self) -> None:
        if self._trigger_pending:
            if self._ctx.trigger_monitor():
                self._trigger_pending = False

    # ------------------------------------------------------------------
    # Buffer drain -- the buffer sentinel calls this on each flush
    # ------------------------------------------------------------------

    def process_buffers(
        self,
        buffers: dict[str, CdcBuffer],
    ) -> None:
        for table, buffer in buffers.items():
            if len(buffer.rows) == 0:
                continue
            self.process_buffer(table, buffer.rows)

    def process_buffer(
        self,
        table: str,
        rows: list[CdcDataChange],
    ) -> None:
        # Group adjacent rows by schema hash so each parquet file
        # contains a single schema version. Schema changes mid-
        # batch produce multiple files for the same table.
        start = 0
        current_hash = rows[0].schema.hash
        for i in range(1, len(rows)):
            row_hash = rows[i].schema.hash
            if row_hash != current_hash:
                self.process_section(table, rows, start, i)
                start = i
                current_hash = row_hash
        self.process_section(table, rows, start, len(rows))

    def process_section(
        self,
        table: str,
        rows: list[CdcDataChange],
        start: int,
        stop: int,
    ) -> None:
        pa_schema = rows[start].schema.pa_schema
        records = [
            row_to_record(rows[i], self._cdc_format)
            for i in range(start, stop)
        ]
        # noinspection PyArgumentList
        pa_table = pa.Table.from_pylist(mapping=records, schema=pa_schema)

        file_prefix = sanitize_file_name(table)
        file_suffix = uuid4().hex
        file_name = f"{file_prefix}_{file_suffix}.parquet"
        file_path = self._landing / file_name
        pq.write_table(pa_table, file_path)

        file_bytes = file_path.stat().st_size
        file_rows = stop - start

        self._files[table].append(file_path)
        self._total_rows += file_rows
        self._total_bytes += file_bytes

        logger.debug(
            "Buffer flush: wrote %s -> %s (%d rows, %d bytes)",
            table, file_name, file_rows, file_bytes,
        )

    # ------------------------------------------------------------------
    # Stage -> commit -> trigger
    # ------------------------------------------------------------------

    def stage_data(self) -> None:
        """Build the per-table manifest from accumulated parquet
        files and hand it to ``ctx.stage`` -- which deposits the
        files in the staging area and returns a ``StageRef``. The
        ref is then committed and the downstream trigger fired.

        ``TableManifest.table`` accepts only the framework's
        identifier shape (``[A-Za-z][A-Za-z0-9_]{0,63}``), so we
        emit the bare table name (not the fully qualified
        ``schema.table``). Schema-side collisions across tracked
        tables are not supported in this layer -- a CDC source
        that tracks ``a.users`` and ``b.users`` simultaneously
        would clash here. Revisit with a wider validator on
        ``TableManifest.table`` if/when that's needed."""
        manifest: list[TableManifest] = []
        for table_key, tracked_table in self._tables.items():
            table_file_paths = self._files.get(table_key, [])
            if not table_file_paths:
                continue
            manifest.append(
                TableManifest(
                    table=tracked_table.data_table.table,
                    files=[ParquetFile(str(p)) for p in table_file_paths],
                )
            )
        if not manifest:
            return

        stage_ref = self._ctx.stage(manifest)
        self._ctx.commit(stage_ref)
        # Trigger downstream; remember whether it was acknowledged
        # so the next loop iteration can ``retry_trigger``.
        self._trigger_pending = not self._ctx.trigger()

        for files in self._files.values():
            files.clear()
        self._total_rows = 0
        self._total_bytes = 0
        self._last_flushed = time.time()

    def flush(self, force: bool = False) -> bool:
        rows_hit = (
            self._max_rows is not None and self._total_rows >= self._max_rows
        )
        bytes_hit = (
            self._max_bytes is not None and self._total_bytes >= self._max_bytes
        )
        time_hit = (time.time() - self._last_flushed) >= self._max_secs
        if self.has_data and (force or rows_hit or bytes_hit or time_hit):
            logger.debug(
                "Disk flush: staging (%d rows, %d bytes)",
                self._total_rows, self._total_bytes,
            )
            self.stage_data()
            return True
        return False
