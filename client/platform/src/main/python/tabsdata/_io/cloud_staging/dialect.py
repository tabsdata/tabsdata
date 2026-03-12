#
# Copyright 2026 Tabs Data Inc.
#

from abc import ABC, abstractmethod

from pyarrow import schema

from tabsdata._io.cloud_staging.modes import LoadMode
from tabsdata._io.cloud_staging.results import BulkLoadResult


class Dialect(ABC):
    """Abstract interface that each target system must implement.

    Every method that operates on a table has two variants:
    - A single-table method (abstract, must be implemented by each dialect).
    - A batch method (default: loops over the single-table version).
    Dialects that support parallelism or batch operations should override the batch
        methods.
    """

    # ── Capability flags ──────────────────────────────────────────────────────────────

    @abstractmethod
    def supports_tabsdata_schema_evolution(self) -> bool:
        """Return True if TED4/Iceberg-style schema evolution is supported."""

    @abstractmethod
    def supports_native_schema_evolution(self) -> bool:
        """Return True if the target system has its own schema evolution mechanism."""

    @abstractmethod
    def supports_transactions(self) -> bool:
        """Return True if the target system supports multi-statement transactions."""

    @abstractmethod
    def supports_auto_cleanup(self) -> bool:
        """Return True if the system automatically cleans up staged files after bulk
        load."""

    @abstractmethod
    def is_bulk_load_async(self) -> bool:
        """Return True if bulk_load() returns PENDING and requires polling."""

    # ── Schema operations (single-table, abstract) ────────────────────────────────────
    @abstractmethod
    def table_exists(self, load_mode: LoadMode, table_name: str) -> bool:
        """Check existence of a table."""

    @abstractmethod
    def get_table_schema(self, load_mode: LoadMode, table_name: str) -> schema:
        """Return the current TableSchema for the given table."""

    @abstractmethod
    def create_table(
        self, load_mode: LoadMode, table_name: str, arrow_schema: schema
    ) -> None:
        """Create a table from a PyArrow schema."""

    @abstractmethod
    def verify_schema(
        self, load_mode: LoadMode, table_name: str, arrow_schema: schema
    ) -> bool:
        """Return True if the existing table is compatible with the PyArrow schema.
        Raise SchemaError if incompatible."""

    # ── Schema operations (batch, default: loop over single-table) ────────────────────

    def tables_exist(self, load_mode: LoadMode, table_names: list[str]) -> list[bool]:
        """Check existence of multiple tables. Default: sequential loop."""
        return [self.table_exists(load_mode, name) for name in table_names]

    def get_table_schemas(
        self, load_mode: LoadMode, table_names: list[str]
    ) -> list[schema]:
        """Get schemas for multiple tables. Default: sequential loop."""
        return [self.get_table_schema(load_mode, name) for name in table_names]

    def create_tables(
        self, load_mode: LoadMode, items: list[tuple[str, object]]
    ) -> None:
        """Create multiple tables. Default: sequential loop.
        items is a list of (table_name, arrow_schema) tuples."""
        for table_name, arrow_schema in items:
            self.create_table(load_mode, table_name, arrow_schema)

    def verify_schemas(
        self, load_mode: LoadMode, items: list[tuple[str, object]]
    ) -> list[bool]:
        """Verify schemas for multiple tables. Default: sequential loop.
        items is a list of (table_name, arrow_schema) tuples."""
        return [self.verify_schema(load_mode, name, schema) for name, schema in items]

    # ── Schema evolution (single-table) ───────────────────────────────────────────────

    def apply_native_schema_evolution(
        self,
        load_mode: LoadMode,
        table_name: str,
        old_schema: schema,
        new_schema: schema,
    ) -> None:
        """Apply schema evolution using the target system's native mechanism.
        May be a no-op for systems where the native mechanism handles it implicitly.
        Override in dialects that support native schema evolution."""
        pass  # no-op by default

    def get_type_widening_map(self) -> dict[str, set[str]]:
        """Return the TED4 TypeWideningMap for this dialect. Used by ICEBERG mode."""
        return {}

    def apply_schema_delta(self, load_mode: LoadMode, table_name: str, delta) -> None:
        """Apply a TED4 SchemaDelta as dialect-specific DDL.
        Override in dialects to render ALTER statements."""
        raise NotImplementedError("Dialect does not implement apply_schema_delta")

    # ── Schema evolution (batch, default: loop over single-table) ─────────────────────

    def apply_native_schema_evolutions(
        self, load_mode: LoadMode, items: list[tuple[str, object, object]]
    ) -> None:
        """Apply native schema evolution to multiple tables. Default: sequential loop.
        items is a list of (table_name, old_schema, new_schema) tuples."""
        for table_name, old_schema, new_schema in items:
            self.apply_native_schema_evolution(
                load_mode, table_name, old_schema, new_schema
            )

    def apply_schema_deltas(
        self, load_mode: LoadMode, items: list[tuple[str, object]]
    ) -> None:
        """Apply TED4 SchemaDelta to multiple tables. Default: sequential loop.
        items is a list of (table_name, delta) tuples."""
        for table_name, delta in items:
            self.apply_schema_delta(load_mode, table_name, delta)

    # ── Data operations (single-table, abstract) ──────────────────────────────────────

    @abstractmethod
    def truncate_table(self, load_mode: LoadMode, table_name: str) -> None:
        """Truncate a table."""

    @abstractmethod
    def bulk_load(
        self, load_mode: LoadMode, table_name: str, staged_file_refs: list[str]
    ) -> BulkLoadResult:
        """Start a bulk load from the staged files. Returns immediately with result
            status.
        Each table may have multiple staged files (e.g. partitioned source data)."""

    def poll_bulk_load(self, load_mode: LoadMode, job_handle: str) -> BulkLoadResult:
        """Poll an async bulk load job. Override for async dialects."""
        raise NotImplementedError("Dialect does not support async bulk loads")

    # ── Data operations (batch, default: loop over single-table) ──────────────────────

    def truncate_tables(self, load_mode: LoadMode, table_names: list[str]) -> None:
        """Truncate multiple tables. Default: sequential loop."""
        for name in table_names:
            self.truncate_table(load_mode, name)

    def bulk_load_tables(
        self, load_mode: LoadMode, items: list[tuple[str, list[str]]]
    ) -> list[BulkLoadResult]:
        """Bulk load multiple tables. Default: sequential loop.
        items is a list of (table_name, staged_file_refs) tuples."""
        return [
            self.bulk_load(load_mode, table_name, refs) for table_name, refs in items
        ]

    def poll_bulk_load_all(
        self, load_mode: LoadMode, job_handles: list[str]
    ) -> list[BulkLoadResult]:
        """Poll multiple async bulk load jobs. Default: sequential loop."""
        return [self.poll_bulk_load(load_mode, handle) for handle in job_handles]

    # ── Transaction operations ────────────────────────────────────────────────────────

    def begin_transaction(self) -> None:
        """Begin a transaction. Override for dialects that support transactions."""
        raise NotImplementedError("Dialect does not support transactions")

    def commit_transaction(self) -> None:
        """Commit the current transaction."""
        raise NotImplementedError("Dialect does not support transactions")

    def rollback_transaction(self) -> None:
        """Rollback the current transaction."""
        raise NotImplementedError("Dialect does not support transactions")
