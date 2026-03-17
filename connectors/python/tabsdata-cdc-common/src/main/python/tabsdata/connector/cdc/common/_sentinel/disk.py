#
#  Copyright 2026 Tabs Data Inc.
#

from __future__ import annotations

import logging
import time
from pathlib import Path

import pyarrow as pa
import pyarrow.parquet as pq

from tabsdata._io.inputs.stage_input import (
    STAGE_MANIFEST_FILE_EXTENSION,
    StageManifest,
    StageManifestEntry,
    _serialize_manifest,
    _write_manifest_file,
)
from tabsdata._io.stage_trigger import StagedFilesSpec, StageTriggerContext
from tabsdata._utils.id import encode_id
from tabsdata.connector.cdc.common._model import (
    CdcDataChange,
    TrackedTable,
)
from tabsdata.connector.cdc.common._sentinel import CdcBuffer, row_to_record
from tabsdata.connector.cdc.common._utils import sanitize_file_name
from tabsdata.exceptions import CdcError, ErrorCode

logger = logging.getLogger(__name__)
logger.setLevel(logging.DEBUG)


class DiskSentinel:
    def __init__(
        self,
        context: StageTriggerContext,
        landing: Path,
        max_rows: int,
        max_bytes: int | None,
        max_seconds: float,
        tables: dict[str, TrackedTable],
    ):
        self._context = context
        self._landing = landing
        self._max_rows = max_rows
        self._max_bytes = max_bytes
        self._max_seconds = max_seconds
        self._tables = tables
        self._files: dict[str, list[Path]] = {table: [] for table in tables}
        self._total_rows: int = 0
        self._total_bytes: int = 0
        self._last_flushed: float = time.time()
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
            if self._context.trigger_monitor():
                self._trigger_pending = False

    def process_buffers(self, buffers: dict[str, CdcBuffer]) -> None:
        for table, buffer in buffers.items():
            if len(buffer.rows) == 0:
                continue
            self.process_buffer(table, buffer.rows)

    def process_buffer(self, table: str, rows: list[CdcDataChange]) -> None:
        tracked_table = self._tables[table]
        records = [
            row_to_record(
                row,
                tracked_table,
                tracked_table.cdc_format,
            )
            for row in rows
        ]
        # noinspection PyArgumentList
        pa_table = pa.Table.from_pylist(
            mapping=records,
            schema=tracked_table.pa_schema,
        )

        _, file_suffix = encode_id()
        file_prefix = sanitize_file_name(table)
        file_name = f"{file_prefix}_{file_suffix}.parquet"
        file_path = self._landing / file_name
        pq.write_table(pa_table, file_path)

        file_bytes = file_path.stat().st_size
        file_rows = len(rows)

        self._files[table].append(file_path)
        self._total_rows += file_rows
        self._total_bytes += file_bytes

        logger.debug(
            f"Buffer flush: writen {table} → {file_name} "
            f"({file_rows} rows & {file_bytes} bytes)"
        )

    def stage_data(self) -> StagedFilesSpec:
        files = [file for files in self._files.values() for file in files]
        staged_files = self._context.stage(files)
        self.stage_meta(staged_files)
        self._trigger_pending = not self._context.trigger()
        for files in self._files.values():
            files.clear()
        self._total_rows = 0
        self._total_bytes = 0
        self._last_flushed = time.time()
        return staged_files

    def stage_meta(self, staged_files: StagedFilesSpec) -> None:
        _, name = encode_id()
        manifest_name = f"{name}{STAGE_MANIFEST_FILE_EXTENSION}"
        manifest_path = self._landing / manifest_name

        entries: list[StageManifestEntry] = []
        for table_key, tracked_table in self._tables.items():
            table_file_paths = self._files.get(table_key, [])
            staged_uris = []
            for file_path in table_file_paths:
                file_key = str(file_path)
                if file_key not in staged_files:
                    raise CdcError(
                        ErrorCode.CDC9,
                        file_key,
                        tracked_table.data_table.qualified,
                    )
                staged_uris.append(staged_files[file_key])
            entries.append(
                StageManifestEntry(
                    table=tracked_table.data_table.qualified,
                    files=staged_uris,
                )
            )

        manifest = StageManifest(entries=entries)
        content = _serialize_manifest(manifest)
        _write_manifest_file(str(manifest_path), content)

        self._context.commit(manifest_path)
        logger.debug(f"Manifest committed: {manifest_name}")

    def flush(
        self,
        force: bool = False,
    ) -> bool:
        rows_hit = self._max_rows is not None and self._total_rows >= self._max_rows
        bytes_hit = self._max_bytes is not None and self._total_bytes >= self._max_bytes
        time_hit = (time.time() - self._last_flushed) >= self._max_seconds
        if self.has_data and (force or rows_hit or bytes_hit or time_hit):
            logger.debug(
                f"Disk flush: staging {self._files} "
                f"({self._total_rows} rows & {self._total_bytes} bytes)"
            )
            self.stage_data()
            return True
        return False
