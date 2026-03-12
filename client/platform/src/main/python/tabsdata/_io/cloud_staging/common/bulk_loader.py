#
# Copyright 2026 Tabs Data Inc.
#

import logging
import time
from pathlib import Path

import pyarrow.parquet as pq

from tabsdata._io.cloud_staging.common.cloud_storage import CloudStorage
from tabsdata._io.cloud_staging.common.dialect import Dialect
from tabsdata._io.cloud_staging.common.errors import BulkLoadError, SchemaError
from tabsdata._io.cloud_staging.common.modes import (
    LoadMode,
    SchemaEvolutionMode,
    WriteMode,
)
from tabsdata._io.cloud_staging.common.results import BulkLoadResult
from tabsdata._io.cloud_staging.common.statuses import BulkLoadStatus

logger = logging.getLogger(__name__)


class CloudStagedBulkLoader:
    """Implements the cloud-staged bulk loading pattern.

    Each phase is a separate method that can be overridden by subclasses to customize
        behavior.
    The main entry point is load(), which orchestrates all phases.
    """

    def __init__(
        self,
        cloud_storage: CloudStorage,
        dialect: Dialect,
        write_mode: WriteMode,
        schema_evolution: SchemaEvolutionMode,
        upload_id: str,
        poll_interval: float = 5.0,
    ):
        self.cloud_storage = cloud_storage
        self.dialect = dialect
        self.write_mode = write_mode
        self.schema_evolution = schema_evolution
        self.upload_id = upload_id
        self.poll_interval = poll_interval
        self.load_mode = LoadMode(
            write_mode=write_mode,
            schema_evolution=schema_evolution,
            upload_id=upload_id,
        )

    # ── Main entry point ──────────────────────────────────────────────────────────────

    def load(self, tables: dict[str, list[Path]]) -> None:
        """Execute the full cloud-staged bulk loading pattern.

        Args:
            tables: Mapping of table_name -> list of local Parquet file paths.
        """
        logger.info("Starting cloud-staged bulk load")
        self.phase_validate_and_prepare(tables)
        staged_files = self.phase_upload(tables)
        self.phase_bulk_load(staged_files)
        self.phase_cleanup()
        logger.info("Cloud-staged bulk load complete")

    # ── Phase 1: Validate & Prepare ───────────────────────────────────────────────────

    def phase_validate_and_prepare(self, tables: dict[str, list[Path]]) -> None:
        """Phase 1 -- Validate all tables and prepare them (create/evolve/truncate).
        Fails fast before any data movement."""
        logger.info("Phase 1: Validate & Prepare")
        for table_name, parquet_paths in tables.items():
            self.validate_and_prepare_table(table_name, parquet_paths)

        if self.write_mode == WriteMode.REPLACE:
            table_names = list(tables.keys())
            logger.debug(f"Truncating tables: {table_names}")
            self.dialect.truncate_tables(self.load_mode, table_names)

        logger.info("Phase 1 complete: all tables validated and prepared.")

    def validate_and_prepare_table(
        self, table_name: str, parquet_paths: list[Path]
    ) -> None:
        """Validate and prepare a single table. Override to customize per-table
        logic."""
        for parquet_path in parquet_paths:
            if not parquet_path.exists():
                raise FileNotFoundError(f"Parquet file not found: {parquet_path}")

        # Read schema from the first file (all files for the same table share the same
        # schema)
        arrow_schema = pq.read_schema(parquet_paths[0])

        if not self.dialect.table_exists(self.load_mode, table_name):
            logger.debug("Creating table: %s", table_name)
            self.dialect.create_table(self.load_mode, table_name, arrow_schema)
        else:
            self.ensure_schema_compatible(table_name, arrow_schema)

    def ensure_schema_compatible(self, table_name: str, arrow_schema) -> None:
        """Check schema compatibility and apply evolution if configured.
        Override to customize schema comparison logic."""
        try:
            self.dialect.verify_schema(self.load_mode, table_name, arrow_schema)
        except SchemaError:
            if self.schema_evolution == SchemaEvolutionMode.NONE:
                raise

            if self.schema_evolution == SchemaEvolutionMode.NATIVE:
                if not self.dialect.supports_native_schema_evolution():
                    raise SchemaError(
                        f"Schema mismatch for '{table_name}' and dialect does not "
                        f"support native schema evolution."
                    )
                old_schema = self.dialect.get_table_schema(self.load_mode, table_name)
                self.dialect.apply_native_schema_evolution(
                    self.load_mode, table_name, old_schema, arrow_schema
                )
                logger.debug(f"Applied native schema evolution to table '{table_name}'")

            elif self.schema_evolution == SchemaEvolutionMode.ICEBERG:
                if not self.dialect.supports_tabsdata_schema_evolution():
                    raise SchemaError(
                        f"Schema mismatch for '{table_name}' and dialect does not "
                        f"support Tabsdata (Iceberg-style) schema evolution."
                    )
                old_schema = self.dialect.get_table_schema(self.load_mode, table_name)
                delta = self.compute_iceberg_delta(old_schema, arrow_schema)
                self.dialect.apply_schema_delta(self.load_mode, table_name, delta)
                logger.debug(
                    f"Applied Iceberg schema evolution to table '{table_name}'"
                )

    def compute_iceberg_delta(self, old_schema, new_schema):
        """Compute a TED4 SchemaDelta between old and new schemas.
        Override to customize delta computation (e.g. custom widen map merging)."""
        # TODO: Add implementation for iceberg delta computation

    # ── Phase 2: Upload to Storage ────────────────────────────────────────────────────

    def phase_upload(self, tables: dict[str, list[Path]]) -> dict[str, list[str]]:
        """Phase 2 -- Upload all Parquet files to cloud storage.
        Returns a mapping of table_name -> list of staged_file_refs (URIs or logical
            paths)."""
        logger.info("Phase 2: Upload to Storage")
        staged_files: dict[str, list[str]] = {}
        total_files = 0
        for table_name, local_paths in tables.items():
            staged_uris = self.cloud_storage.upload(self.upload_id, local_paths)
            staged_files[table_name] = staged_uris
            total_files += len(staged_uris)
        logger.info(f"Phase 2 complete: all files staged ({total_files}).")
        return staged_files

    # ── Phase 3: Bulk Load ────────────────────────────────────────────────────────────

    def phase_bulk_load(self, staged_files: dict[str, list[str]]) -> None:
        """Phase 3 -- Bulk load all staged files into their target tables.
        Uses dialect batch methods and handles transactions and async polling."""
        logger.info("Phase 3: Bulk Load")
        items = list(staged_files.items())
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
        """Poll all async bulk loads until they complete or any fails.
        Override to customize polling (e.g. exponential backoff)."""
        while True:
            pending_indices = [
                i for i, r in enumerate(results) if r.status == BulkLoadStatus.PENDING
            ]
            if not pending_indices:
                break
            time.sleep(self.poll_interval)
            pending_handles = [results[i].job_handle for i in pending_indices]
            polled = self.dialect.poll_bulk_load_all(self.load_mode, pending_handles)
            for idx, polled_result in zip(pending_indices, polled):
                results[idx] = polled_result
                if polled_result.status == BulkLoadStatus.FAILED:
                    return results  # early exit, caller will check
        return results

    def check_bulk_load_results(
        self, items: list[tuple[str, list[str]]], results: list[BulkLoadResult]
    ) -> None:
        """Check results and raise on any failure."""
        for (table_name, _), result in zip(items, results):
            if result.status == BulkLoadStatus.FAILED:
                raise BulkLoadError(
                    f"Bulk load failed for table '{table_name}': {result.error}"
                )
            logger.info(
                f"Bulk load complete for '{table_name}': {result.rows_loaded} rows "
                f"loaded"
            )

    # ── Phase 4: Cleanup ──────────────────────────────────────────────────────────────

    def phase_cleanup(self) -> None:
        """Phase 4 -- Clean up staged files if the dialect does not auto-cleanup."""
        if self.dialect.supports_auto_cleanup():
            logger.info("Phase 4: Skipped (dialect supports auto-cleanup).")
            return

        logger.info("Phase 4: Cleanup")
        try:
            self.cloud_storage.delete(self.upload_id)
            logger.info("Phase 4 complete.")
        except Exception:
            logger.error("Cleanup failed (non-fatal, will be retried on next run)")
