#
# Copyright 2026 Tabsdata Inc.
#

from __future__ import annotations

import logging
import re
import time
from decimal import Decimal
from typing import Any, Callable, Iterable

import polars as pl
import pyarrow as pa
import pyarrow.parquet as pq

# noinspection PyProtectedMember
from tabsdatak.conn.clickhouse._cloud_storage import CloudStorage, StagedFile

# noinspection PyProtectedMember
from tabsdatak.conn.clickhouse._dialect_base import Dialect

# noinspection PyProtectedMember
from tabsdatak.conn.clickhouse._errors import BulkLoadError

# noinspection PyProtectedMember
from tabsdatak.conn.clickhouse._modes import LoadMode, WriteMode

# noinspection PyProtectedMember
from tabsdatak.conn.clickhouse._results import BulkLoadResult

# noinspection PyProtectedMember
from tabsdatak.conn.clickhouse._statuses import BulkLoadStatus

# noinspection PyProtectedMember
from tabsdatak.conn.common.sql._schema.delta import Action, SchemaChange, SchemaDelta

# noinspection PyProtectedMember
from tabsdatak.conn.common.sql._schema.footer import column_range

# noinspection PyProtectedMember
from tabsdatak.conn.common.sql._schema.policy import (
    STRICT,
    SchemaEvolution,
    governed,
)

logger = logging.getLogger(__name__)

_INTEGER = re.compile(r"^(?:Nullable\()?(U?)Int(8|16|32|64|128|256)\)?$")
_DECIMAL = re.compile(r"^(?:Nullable\()?Decimal\((\d+),\s*(\d+)\)\)?$")


def _holds(stored: str, batch: str) -> Callable[[Any], bool] | None:
    """Whether a ``stored`` column holds a given value of the ``batch`` type,
    when both are integers or both decimals; ``None`` for any other pair, and
    for a batch decimal with more fractional digits than the column keeps,
    which ClickHouse would round away."""
    integer = _INTEGER.match(stored.strip())
    if integer and _INTEGER.match(batch.strip()):
        bits = int(integer[2])
        if integer[1]:
            low, high = 0, 2**bits - 1
        else:
            low, high = -(2 ** (bits - 1)), 2 ** (bits - 1) - 1
        return lambda value: low <= value <= high
    held, wider = _DECIMAL.match(stored.strip()), _DECIMAL.match(batch.strip())
    if held and wider and int(wider[2]) <= int(held[2]):
        limit = Decimal(10) ** (int(held[1]) - int(held[2]))
        return lambda value: -limit < Decimal(value) < limit
    return None


def _fitting_numbers(delta: SchemaDelta, footers) -> SchemaDelta:
    """``delta`` with each change of an integer or decimal column to a wider
    type of its kind marked as fitting when every value of the batch fits the
    column as it is, which the footers' statistics tell without reading the
    data. ClickHouse would cut an integer that does not fit to the column's
    width rather than refuse it, so under strict a batch that needs the wider
    type is refused."""
    changes = []
    for change in delta.changes:
        if change.action is Action.ALTER_TYPE:
            holds = _holds(
                change.detail.get("old_type", ""), change.detail.get("new_type", "")
            )
            values = column_range(footers, change.column) if holds else None
            if values is not None and (
                values[0] is None or (holds(values[0]) and holds(values[1]))
            ):
                change = SchemaChange(
                    action=change.action,
                    column=change.column,
                    safe=change.safe,
                    detail={**change.detail, "fits": True},
                )
        changes.append(change)
    return SchemaDelta(
        table_name=delta.table_name, is_create=delta.is_create, changes=changes
    )


class CloudStagedBulkLoader:
    """Implements the cloud-staged bulk loading pattern.

    Each phase is a separate method that can be overridden by subclasses
    to customize behavior. The main entry point is :meth:`load`, which
    orchestrates all phases.
    """

    def __init__(
        self,
        cloud_storage: CloudStorage,
        dialect: Dialect,
        write_mode: WriteMode,
        schema_evolution: SchemaEvolution,
        staging_folder: str,
        poll_interval: float = 5.0,
        poll_timeout: float = 1800.0,
    ):
        self.cloud_storage = cloud_storage
        self.dialect = dialect
        self.write_mode = write_mode
        self.schema_evolution = schema_evolution
        self.staging_folder = staging_folder
        self.poll_interval = poll_interval
        self.poll_timeout = poll_timeout
        self.load_mode = LoadMode(
            write_mode=write_mode,
            schema_evolution=schema_evolution,
        )

    # --- Main entry point --------------------------------------------

    def load(self, tables: dict[str, list[tuple[str, pl.LazyFrame]]]) -> None:
        """Execute the full cloud-staged bulk loading pattern.

        ``tables`` maps each table name to the ``(filename, frame)`` pairs
        staged for it, under ``staging_folder``. The folder is cleared before
        the upload, of whatever a failed earlier run left there, and again
        however the load ends. It must be the loader's own: two loads into
        one folder at once would delete each other's files.
        """
        logger.info("Starting cloud-staged bulk load")
        self.clear_staging()
        try:
            staged_files = self.phase_upload(tables)
            columns, swaps = self.phase_validate_and_prepare(staged_files)
            self.phase_bulk_load(
                {
                    table: [staged.ref for staged in files]
                    for table, files in staged_files.items()
                },
                columns,
                swaps,
            )
            self.phase_swap(swaps)
        finally:
            self.phase_cleanup()
        logger.info("Cloud-staged bulk load complete")

    # --- Phase 1: Upload to Storage ----------------------------------

    def phase_upload(
        self, tables: dict[str, list[tuple[str, pl.LazyFrame]]]
    ) -> dict[str, list[StagedFile]]:
        """Phase 1 -- Stage every table's frames as Parquet in cloud
        storage."""
        logger.info("Phase 1: Upload to Storage")
        staged_files: dict[str, list[StagedFile]] = {}
        for table_name, frames in tables.items():
            staged_files[table_name] = self.cloud_storage.upload(
                self.staging_folder, frames
            )
        total = sum(len(files) for files in staged_files.values())
        logger.info("Phase 1 complete: all files staged (%d).", total)
        return staged_files

    # --- Phase 2: Validate & Prepare ---------------------------------

    def phase_validate_and_prepare(
        self, staged_files: dict[str, list[StagedFile]]
    ) -> tuple[dict[str, list[str]], dict[str, str]]:
        """Phase 2 -- Validate every table against its staged files'
        footers, then prepare it: create a missing table and evolve an
        existing one. 'replace' leaves an existing table as it is: it copies
        it to a swap table and evolves that. Nothing is created or changed
        until every table is validated. Returns each table's staged column
        names, and the swap table of each replaced table."""
        logger.info("Phase 2: Validate & Prepare")
        columns: dict[str, list[str]] = {}
        missing: dict[str, pa.Schema] = {}
        deltas: dict[str, SchemaDelta] = {}
        for table_name, files in staged_files.items():
            footers = [self.cloud_storage.footer(staged.key) for staged in files]
            # All files of a table share its schema.
            arrow_schema = footers[0].schema.to_arrow_schema()
            columns[table_name] = list(arrow_schema.names)
            if self.dialect.table_exists(self.load_mode, table_name):
                deltas[table_name] = self.evolution(
                    table_name, arrow_schema, self._detect_nullability(footers), footers
                )
            else:
                missing[table_name] = arrow_schema

        replacing = self.write_mode == WriteMode.REPLACE
        if replacing:
            for table_name in deltas:
                self.dialect.check_swappable(self.load_mode, table_name)

        for table_name, arrow_schema in missing.items():
            logger.debug("Creating table: %s", table_name)
            self.dialect.create_table(self.load_mode, table_name, arrow_schema)

        swaps: dict[str, str] = {}
        try:
            for table_name, delta in deltas.items():
                target = table_name
                if replacing:
                    target = self.dialect.swap_table_name(table_name)
                    self.dialect.drop_table(self.load_mode, target)
                    self.dialect.copy_table(self.load_mode, target, table_name)
                    swaps[table_name] = target
                if delta.changes:
                    self.dialect.apply_schema_delta(self.load_mode, target, delta)
                    logger.info("clickhouse: evolved the schema of table %r", target)
        except Exception:
            self._drop_swaps(swaps.values())
            raise

        logger.info("Phase 2 complete: all tables validated and prepared.")
        return columns, swaps

    def evolution(
        self,
        table_name: str,
        arrow_schema: pa.Schema,
        column_nulls: dict[str, bool | None],
        footers: list[pq.FileMetaData],
    ) -> SchemaDelta:
        """The changes the batch makes to ``table_name``'s schema, refused
        when its schema evolution does not allow them."""
        delta = self.dialect.compute_evolution_delta(
            self.load_mode, table_name, arrow_schema, column_nulls
        )
        if self.schema_evolution == STRICT:
            delta = _fitting_numbers(delta, footers)
        return governed(delta, self.schema_evolution)

    @staticmethod
    def _detect_nullability(
        footers: list[pq.FileMetaData],
    ) -> dict[str, bool | None]:
        """Detect per-column nullability from Parquet file statistics.

        Reads row-group-level statistics from all provided Parquet footers
        to determine whether each column actually contains null values,
        rather than relying on Arrow's schema-level nullable flag (which
        is always ``True`` for Polars-produced files).

        Returns a dict mapping column names to:
          - ``True``  -- at least one null found across all row groups
          - ``False`` -- zero nulls across all row groups
          - ``None``  -- stats missing or unreliable for this column
        """
        null_counts: dict[str, int] = {}
        stats_missing: set[str] = set()
        all_columns: dict[str, None] = {}  # insertion-ordered set

        for meta in footers:
            schema = meta.schema.to_arrow_schema()

            for i in range(len(schema)):
                all_columns[schema.field(i).name] = None

            for rg_idx in range(meta.num_row_groups):
                rg = meta.row_group(rg_idx)
                for col_idx in range(rg.num_columns):
                    col = rg.column(col_idx)
                    name = col.path_in_schema
                    stats = col.statistics
                    if stats is None or not stats.has_null_count:
                        stats_missing.add(name)
                    else:
                        null_counts[name] = null_counts.get(name, 0) + stats.null_count

        result: dict[str, bool | None] = {}
        for name in all_columns:
            if name in stats_missing:
                result[name] = None
            else:
                result[name] = null_counts.get(name, 0) > 0
        return result

    # --- Phase 3: Bulk Load ------------------------------------------

    def phase_bulk_load(
        self,
        staged_files: dict[str, list[str]],
        columns: dict[str, list[str]],
        swaps: dict[str, str],
    ) -> None:
        """Phase 3 -- Bulk load all staged files into their target
        tables, or a replaced table's swap table. Uses dialect batch methods
        and handles transactions and async polling. A failure drops the swap
        tables, leaving every replaced table as it was."""
        logger.info("Phase 3: Bulk Load")
        items = [
            (swaps.get(table, table), refs, columns[table])
            for table, refs in staged_files.items()
        ]
        try:
            if self.dialect.supports_transactions():
                self.dialect.begin_transaction()

            results = self.dialect.bulk_load_tables(self.load_mode, items)

            if self.dialect.is_bulk_load_async():
                results = self.poll_all_until_complete(results)

            self.check_bulk_load_results(items, results)

            if self.dialect.supports_transactions():
                self.dialect.commit_transaction()
        except Exception:
            if self.dialect.supports_transactions():
                try:
                    self.dialect.rollback_transaction()
                    logger.info("Rollback complete")
                except Exception:
                    logger.error("Rollback failed")
            self._drop_swaps(swaps.values())
            raise
        logger.info("Phase 3 complete: all tables loaded.")

    def poll_all_until_complete(
        self, results: list[BulkLoadResult]
    ) -> list[BulkLoadResult]:
        """Poll all async bulk loads until they complete or any fails."""
        deadline = time.monotonic() + self.poll_timeout
        while True:
            pending_indices = [
                i for i, r in enumerate(results) if r.status == BulkLoadStatus.PENDING
            ]
            if not pending_indices:
                break
            pending_handles: list[str] = []
            for index in pending_indices:
                handle = results[index].job_handle
                if handle is None:
                    raise BulkLoadError(f"Pending bulk load {index} has no job handle")
                pending_handles.append(handle)
            if time.monotonic() >= deadline:
                raise BulkLoadError(
                    f"Polling timed out after {self.poll_timeout}s. "
                    f"Still pending: {pending_handles}"
                )
            time.sleep(self.poll_interval)
            polled = self.dialect.poll_bulk_load_all(self.load_mode, pending_handles)
            for idx, polled_result in zip(pending_indices, polled):
                results[idx] = polled_result
                if polled_result.status == BulkLoadStatus.FAILED:
                    return results  # early exit, caller checks
        return results

    def check_bulk_load_results(
        self,
        items: list[tuple[str, list[str], list[str]]],
        results: list[BulkLoadResult],
    ) -> None:
        """Check results and raise on any failure."""
        for (table_name, _, _), result in zip(items, results):
            if result.status == BulkLoadStatus.FAILED:
                raise BulkLoadError(
                    f"Bulk load failed for table '{table_name}': {result.error}"
                )
            logger.info(
                "Bulk load complete for '%s': %s rows loaded",
                table_name,
                result.rows_loaded,
            )

    # --- Phase 4: Swap -----------------------------------------------

    def phase_swap(self, swaps: dict[str, str]) -> None:
        """Phase 4 -- Exchange each replaced table with its loaded swap
        table, then drop the swap tables, which now hold the old rows. Each
        exchange is atomic, but not the set of them: a failure between two
        leaves some tables replaced until the next successful run."""
        if not swaps:
            return
        logger.info("Phase 4: Swap")
        for table_name, swap in swaps.items():
            self.dialect.exchange_tables(self.load_mode, swap, table_name)
        self._drop_swaps(swaps.values())
        logger.info("Phase 4 complete: all replaced tables swapped.")

    def _drop_swaps(self, swaps: Iterable[str]) -> None:
        for swap in swaps:
            try:
                self.dialect.drop_table(self.load_mode, swap)
            except Exception:
                logger.warning(
                    "Could not drop swap table %r (the next replace drops it)",
                    swap,
                    exc_info=True,
                )

    # --- Phase 5: Cleanup --------------------------------------------

    def phase_cleanup(self) -> None:
        """Phase 5 -- Clean up staged files if the dialect does not
        auto-cleanup."""
        if self.dialect.supports_auto_cleanup():
            logger.info("Phase 5: Skipped (dialect supports auto-cleanup).")
            return
        logger.info("Phase 5: Cleanup")
        self.clear_staging()
        logger.info("Phase 5 complete.")

    def clear_staging(self) -> None:
        """Delete every file under the staging folder. A failure is only
        logged: the next run clears the folder again."""
        if self.dialect.supports_auto_cleanup():
            return
        try:
            self.cloud_storage.delete(self.staging_folder)
        except Exception:
            logger.warning(
                "Cleanup failed (non-fatal, will be retried on next run)",
                exc_info=True,
            )
