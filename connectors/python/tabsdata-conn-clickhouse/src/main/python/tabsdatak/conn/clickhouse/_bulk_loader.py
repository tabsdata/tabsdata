#
# Copyright 2026 Tabsdata Inc.
#

from __future__ import annotations

import logging
import time

import polars as pl
import pyarrow.parquet as pq

# noinspection PyProtectedMember
from tabsdatak.conn.clickhouse._cloud_storage import CloudStorage, StagedFile

# noinspection PyProtectedMember
from tabsdatak.conn.clickhouse._dialect_base import Dialect

# noinspection PyProtectedMember
from tabsdatak.conn.clickhouse._errors import BulkLoadError, SchemaError

# noinspection PyProtectedMember
from tabsdatak.conn.clickhouse._modes import (
    LoadMode,
    SchemaEvolutionMode,
    WriteMode,
)

# noinspection PyProtectedMember
from tabsdatak.conn.clickhouse._results import BulkLoadResult

# noinspection PyProtectedMember
from tabsdatak.conn.clickhouse._statuses import BulkLoadStatus

logger = logging.getLogger(__name__)


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
        schema_evolution: SchemaEvolutionMode,
        upload_id: str,
        poll_interval: float = 5.0,
        poll_timeout: float = 1800.0,
    ):
        self.cloud_storage = cloud_storage
        self.dialect = dialect
        self.write_mode = write_mode
        self.schema_evolution = schema_evolution
        self.upload_id = upload_id
        self.poll_interval = poll_interval
        self.poll_timeout = poll_timeout
        self.load_mode = LoadMode(
            write_mode=write_mode,
            schema_evolution=schema_evolution,
            upload_id=upload_id,
        )

    # --- Main entry point --------------------------------------------

    def load(self, tables: dict[str, list[tuple[str, pl.LazyFrame]]]) -> None:
        """Execute the full cloud-staged bulk loading pattern.

        ``tables`` maps each table name to the ``(filename, frame)`` pairs
        staged for it. The staged files are cleaned up however the load
        ends.
        """
        logger.info("Starting cloud-staged bulk load")
        try:
            staged_files = self.phase_upload(tables)
            columns = self.phase_validate_and_prepare(staged_files)
            self.phase_bulk_load(
                {
                    table: [staged.ref for staged in files]
                    for table, files in staged_files.items()
                },
                columns,
            )
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
            staged_files[table_name] = self.cloud_storage.upload(self.upload_id, frames)
        total = sum(len(files) for files in staged_files.values())
        logger.info("Phase 1 complete: all files staged (%d).", total)
        return staged_files

    # --- Phase 2: Validate & Prepare ---------------------------------

    def phase_validate_and_prepare(
        self, staged_files: dict[str, list[StagedFile]]
    ) -> dict[str, list[str]]:
        """Phase 2 -- Validate all tables against their staged files'
        footers and prepare them (create/evolve), before any load. Returns
        each table's staged column names."""
        logger.info("Phase 2: Validate & Prepare")
        # 'replace' drops the target up front so it is recreated fresh from the
        # incoming schema (honouring a changed schema too); the create path below
        # then sees no table and rebuilds it.
        if self.write_mode == WriteMode.REPLACE:
            table_names = list(staged_files.keys())
            logger.debug(f"Dropping tables for replace: {table_names}")
            self.dialect.drop_tables(self.load_mode, table_names)

        columns: dict[str, list[str]] = {}
        for table_name, files in staged_files.items():
            footers = [self.cloud_storage.footer(staged.key) for staged in files]
            columns[table_name] = self.validate_and_prepare_table(table_name, footers)

        logger.info("Phase 2 complete: all tables validated and prepared.")
        return columns

    def validate_and_prepare_table(
        self, table_name: str, footers: list[pq.FileMetaData]
    ) -> list[str]:
        """Validate and prepare a single table, returning the staged column
        names."""
        # Read schema from the first file (all files for the same table
        # share the same schema).
        arrow_schema = footers[0].schema.to_arrow_schema()

        # Detect per-column nullability from parquet stats only when
        # schema evolution is enabled. For NONE mode we preserve the
        # legacy behavior (Arrow's always-nullable flag) so that
        # ``verify_schema`` -- which also uses the Arrow flag -- stays
        # consistent.
        if self.schema_evolution != SchemaEvolutionMode.NONE:
            column_nulls = self._detect_nullability(footers)
        else:
            column_nulls = None

        if not self.dialect.table_exists(self.load_mode, table_name):
            logger.debug("Creating table: %s", table_name)
            self.dialect.create_table(
                self.load_mode, table_name, arrow_schema, column_nulls
            )
        else:
            self.ensure_schema_compatible(table_name, arrow_schema, column_nulls)
        return list(arrow_schema.names)

    def ensure_schema_compatible(
        self,
        table_name: str,
        arrow_schema,
        column_nulls: dict[str, bool | None] | None = None,
    ) -> None:
        """Check schema compatibility and apply evolution if configured."""
        try:
            self.dialect.verify_schema(self.load_mode, table_name, arrow_schema)
        except SchemaError:
            if self.schema_evolution == SchemaEvolutionMode.NONE:
                raise

            if self.schema_evolution == SchemaEvolutionMode.NATIVE:
                if not self.dialect.supports_native_schema_evolution():
                    raise SchemaError(
                        f"Schema mismatch for '{table_name}' and dialect"
                        f" does not support native schema evolution."
                    )
                old_schema = self.dialect.get_table_schema(self.load_mode, table_name)
                self.dialect.apply_native_schema_evolution(
                    self.load_mode, table_name, old_schema, arrow_schema
                )
                logger.info(
                    "clickhouse: applied native schema evolution to table %r",
                    table_name,
                )

            elif self.schema_evolution == SchemaEvolutionMode.ICEBERG:
                if not self.dialect.supports_tabsdata_schema_evolution():
                    raise SchemaError(
                        f"Schema mismatch for '{table_name}' and dialect"
                        f" does not support Tabsdata (Iceberg-style)"
                        f" schema evolution."
                    )
                delta = self.dialect.compute_evolution_delta(
                    self.load_mode,
                    table_name,
                    arrow_schema,
                    column_nulls,
                )
                self.dialect.apply_schema_delta(self.load_mode, table_name, delta)
                logger.info(
                    "clickhouse: applied iceberg schema evolution to table %r",
                    table_name,
                )

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
        self, staged_files: dict[str, list[str]], columns: dict[str, list[str]]
    ) -> None:
        """Phase 3 -- Bulk load all staged files into their target
        tables. Uses dialect batch methods and handles transactions and
        async polling."""
        logger.info("Phase 3: Bulk Load")
        items = [(table, refs, columns[table]) for table, refs in staged_files.items()]
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

    # --- Phase 4: Cleanup --------------------------------------------

    def phase_cleanup(self) -> None:
        """Phase 4 -- Clean up staged files if the dialect does not
        auto-cleanup."""
        if self.dialect.supports_auto_cleanup():
            logger.info("Phase 4: Skipped (dialect supports auto-cleanup).")
            return

        logger.info("Phase 4: Cleanup")
        try:
            self.cloud_storage.delete(self.upload_id)
            logger.info("Phase 4 complete.")
        except Exception:
            logger.warning(
                "Cleanup failed (non-fatal, will be retried on next run)",
                exc_info=True,
            )
