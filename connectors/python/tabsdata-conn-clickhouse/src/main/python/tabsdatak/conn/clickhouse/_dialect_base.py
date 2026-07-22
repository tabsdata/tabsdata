#
# Copyright 2026 Tabsdata Inc.
#

from __future__ import annotations

from abc import ABC, abstractmethod

from pyarrow import schema

# noinspection PyProtectedMember
from tabsdatak.conn.clickhouse._modes import LoadMode

# noinspection PyProtectedMember
from tabsdatak.conn.clickhouse._results import BulkLoadResult


class Dialect(ABC):
    """Abstract interface that each target system must implement.

    Every method that operates on a table has two variants:
    - A single-table method (abstract, must be implemented by each dialect).
    - A batch method (default: loops over the single-table version).
    Dialects that support parallelism or batch operations should override
    the batch methods.
    """

    # --- Capability flags --------------------------------------------

    @abstractmethod
    def supports_tabsdata_schema_evolution(self) -> bool:
        """Return True if Iceberg-style schema evolution is supported."""

    @abstractmethod
    def supports_native_schema_evolution(self) -> bool:
        """Return True if the target system has its own schema evolution
        mechanism."""

    @abstractmethod
    def supports_transactions(self) -> bool:
        """Return True if the target system supports multi-statement
        transactions."""

    @abstractmethod
    def supports_auto_cleanup(self) -> bool:
        """Return True if the system automatically cleans up staged files
        after bulk load."""

    @abstractmethod
    def is_bulk_load_async(self) -> bool:
        """Return True if ``bulk_load()`` returns PENDING and requires
        polling."""

    # --- Schema operations (single-table, abstract) ------------------

    @abstractmethod
    def table_exists(self, load_mode: LoadMode, table_name: str) -> bool:
        """Check existence of a table."""

    @abstractmethod
    def get_table_schema(self, load_mode: LoadMode, table_name: str) -> schema:
        """Return the current schema for the given table."""

    @abstractmethod
    def create_table(
        self,
        load_mode: LoadMode,
        table_name: str,
        arrow_schema: schema,
        column_nulls: dict[str, bool | None] | None = None,
    ) -> None:
        """Create a table from a PyArrow schema."""

    @abstractmethod
    def verify_schema(
        self, load_mode: LoadMode, table_name: str, arrow_schema: schema
    ) -> bool:
        """Return True if the existing table is compatible with the
        PyArrow schema. Raise ``SchemaError`` if incompatible."""

    # --- Schema operations (batch, default loop) ---------------------

    def tables_exist(self, load_mode: LoadMode, table_names: list[str]) -> list[bool]:
        return [self.table_exists(load_mode, name) for name in table_names]

    def get_table_schemas(
        self, load_mode: LoadMode, table_names: list[str]
    ) -> list[schema]:
        return [self.get_table_schema(load_mode, name) for name in table_names]

    def create_tables(
        self, load_mode: LoadMode, items: list[tuple[str, object]]
    ) -> None:
        for table_name, arrow_schema in items:
            self.create_table(load_mode, table_name, arrow_schema)

    def verify_schemas(
        self, load_mode: LoadMode, items: list[tuple[str, object]]
    ) -> list[bool]:
        return [self.verify_schema(load_mode, name, schema) for name, schema in items]

    # --- Schema evolution (single-table) -----------------------------

    def apply_native_schema_evolution(
        self,
        load_mode: LoadMode,
        table_name: str,
        old_schema: schema,
        new_schema: schema,
    ) -> None:
        pass  # no-op by default

    def get_type_widening_map(self) -> dict[str, set[str]]:
        return {}

    def compute_evolution_delta(
        self,
        load_mode: LoadMode,
        table_name: str,
        new_arrow_schema: schema,
        column_nulls: dict[str, bool | None] | None = None,
    ):
        raise NotImplementedError("Dialect does not implement compute_evolution_delta")

    def apply_schema_delta(self, load_mode: LoadMode, table_name: str, delta) -> None:
        raise NotImplementedError("Dialect does not implement apply_schema_delta")

    # --- Schema evolution (batch) ------------------------------------

    def apply_native_schema_evolutions(
        self,
        load_mode: LoadMode,
        items: list[tuple[str, object, object]],
    ) -> None:
        for table_name, old_schema, new_schema in items:
            self.apply_native_schema_evolution(
                load_mode, table_name, old_schema, new_schema
            )

    def apply_schema_deltas(
        self, load_mode: LoadMode, items: list[tuple[str, object]]
    ) -> None:
        for table_name, delta in items:
            self.apply_schema_delta(load_mode, table_name, delta)

    # --- Data operations (single-table, abstract) --------------------

    @abstractmethod
    def drop_table(self, load_mode: LoadMode, table_name: str) -> None:
        """Drop a table if it exists."""

    @abstractmethod
    def bulk_load(
        self,
        load_mode: LoadMode,
        table_name: str,
        staged_file_refs: list[str],
    ) -> BulkLoadResult:
        """Start a bulk load from the staged files."""

    def poll_bulk_load(self, load_mode: LoadMode, job_handle: str) -> BulkLoadResult:
        raise NotImplementedError("Dialect does not support async bulk loads")

    # --- Data operations (batch) -------------------------------------

    def drop_tables(self, load_mode: LoadMode, table_names: list[str]) -> None:
        for name in table_names:
            self.drop_table(load_mode, name)

    def bulk_load_tables(
        self,
        load_mode: LoadMode,
        items: list[tuple[str, list[str]]],
    ) -> list[BulkLoadResult]:
        return [
            self.bulk_load(load_mode, table_name, refs) for table_name, refs in items
        ]

    def poll_bulk_load_all(
        self, load_mode: LoadMode, job_handles: list[str]
    ) -> list[BulkLoadResult]:
        return [self.poll_bulk_load(load_mode, handle) for handle in job_handles]

    # --- Transactions ------------------------------------------------

    def begin_transaction(self) -> None:
        raise NotImplementedError("Dialect does not support transactions")

    def commit_transaction(self) -> None:
        raise NotImplementedError("Dialect does not support transactions")

    def rollback_transaction(self) -> None:
        raise NotImplementedError("Dialect does not support transactions")
