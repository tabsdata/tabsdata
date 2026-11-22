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
    ) -> None:
        """Create a table from a PyArrow schema."""

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

    # --- Schema evolution (single-table) -----------------------------

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

    def apply_schema_deltas(
        self, load_mode: LoadMode, items: list[tuple[str, object]]
    ) -> None:
        for table_name, delta in items:
            self.apply_schema_delta(load_mode, table_name, delta)

    # --- Swap operations (single-table, abstract) --------------------

    @abstractmethod
    def check_swappable(self, load_mode: LoadMode, table_name: str) -> None:
        """Refuse a table whose database cannot exchange tables."""

    @abstractmethod
    def swap_table_name(self, table_name: str) -> str:
        """The name of the table a replace of ``table_name`` loads into."""

    @abstractmethod
    def copy_table(self, load_mode: LoadMode, table_name: str, source: str) -> None:
        """Create an empty table with ``source``'s whole definition."""

    @abstractmethod
    def exchange_tables(self, load_mode: LoadMode, table_name: str, other: str) -> None:
        """Swap the names of two tables, atomically."""

    @abstractmethod
    def drop_table(self, load_mode: LoadMode, table_name: str) -> None:
        """Drop a table, if it exists, and wait until it is gone."""

    # --- Data operations (single-table, abstract) --------------------

    @abstractmethod
    def bulk_load(
        self,
        load_mode: LoadMode,
        table_name: str,
        staged_file_refs: list[str],
        columns: list[str],
    ) -> BulkLoadResult:
        """Start a bulk load from the staged files, matching ``columns`` --
        the staged files' own -- onto the table by name."""

    def poll_bulk_load(self, load_mode: LoadMode, job_handle: str) -> BulkLoadResult:
        raise NotImplementedError("Dialect does not support async bulk loads")

    # --- Data operations (batch) -------------------------------------

    def bulk_load_tables(
        self,
        load_mode: LoadMode,
        items: list[tuple[str, list[str], list[str]]],
    ) -> list[BulkLoadResult]:
        return [
            self.bulk_load(load_mode, table_name, refs, columns)
            for table_name, refs, columns in items
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
