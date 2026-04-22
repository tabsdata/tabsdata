#
# Copyright 2026 Tabs Data Inc.
#

from __future__ import annotations

import logging
from typing import List

from tabsdata._io.connections.named_connection.named_connection import NamedConn
from tabsdata._io.outputs.shared_enums import (
    IfTableExistStrategySpec,
    SchemaStrategySpec,
)
from tabsdata._io.plugin import DestinationPlugin
from tabsdata._utils.validation import _td_validate_call
from tabsdata.connector.databricks._connection import DatabricksConn
from tabsdata.connector.databricks._connector import DatabricksDestination
from tabsdata.exceptions import ErrorCode

logger = logging.getLogger(__name__)
logger.setLevel(logging.DEBUG)


class DatabricksDest(DestinationPlugin):
    """
    Destination plugin for writing data to Databricks.
    """

    @_td_validate_call(ErrorCode.DECE101)
    def __init__(
        self,
        conn: DatabricksConn | NamedConn,
        tables: str | list[str],
        volume: str,
        catalog: str | None = None,
        schema: str | None = None,
        warehouse: str | None = None,
        warehouse_id: str | None = None,
        if_table_exists: IfTableExistStrategySpec = "append",
        schema_strategy: SchemaStrategySpec = "update",
        **kwargs,
    ):
        """
        Initialize a DatabricksDest instance.

        Args:
            conn: The Databricks connection configuration (host_url and token).
            tables: Tables to create in Databricks. Each name must either be
                fully qualified (``catalog.schema.table_name``) or use the
                ``catalog`` / ``schema`` parameters to fill in the missing
                pieces.
            volume: Name of the Databricks managed volume used for staging.
            catalog: Optional catalog name used to qualify table names.
            schema: Optional schema name used to qualify table names.
            warehouse: Warehouse name. Provide either ``warehouse`` or
                ``warehouse_id``, not both.
            warehouse_id: Warehouse ID. Use ``None`` if ``warehouse`` is
                provided.
            if_table_exists: ``"append"`` or ``"replace"``. Defaults to
                ``"append"``.
            schema_strategy: ``"update"`` or ``"strict"``. Defaults to
                ``"update"``.
        """
        # Stash all non-conn state first so the conn setter can rebuild the
        # inner DatabricksDestination with a complete picture.
        self._tables = tables
        self._volume = volume
        self._catalog = catalog
        self._schema = schema
        self._warehouse = warehouse
        self._warehouse_id = warehouse_id
        self._if_table_exists = if_table_exists
        self._schema_strategy = schema_strategy
        self._kwargs = kwargs
        self._databricks_destination: DatabricksDestination | None = None
        # Assigning conn triggers the setter, which (re)builds the inner
        # DatabricksDestination when conn is a resolved DatabricksConn.
        self.conn = conn

    def _build_destination(self) -> DatabricksDestination:
        return DatabricksDestination(
            host_url=self._conn.host_url,
            token=self._conn.token,
            tables=self._tables,
            volume=self._volume,
            catalog=self._catalog,
            schema=self._schema,
            warehouse=self._warehouse,
            warehouse_id=self._warehouse_id,
            if_table_exists=self._if_table_exists,
            schema_strategy=self._schema_strategy,
            **self._kwargs,
        )

    @property
    def conn(self) -> DatabricksConn | NamedConn:
        """
        The Databricks connection configuration.
        """
        return self._conn

    @conn.setter
    @_td_validate_call(ErrorCode.DECE101)
    def conn(self, conn: DatabricksConn | NamedConn):
        self._conn = conn
        if isinstance(conn, DatabricksConn):
            self._databricks_destination = self._build_destination()
        else:
            self._databricks_destination = None

    @property
    def tables(self) -> List[str]:
        """
        The fully qualified target table names.
        """
        if self._databricks_destination is not None:
            return self._databricks_destination.tables
        return [self._tables] if isinstance(self._tables, str) else self._tables

    @tables.setter
    def tables(self, tables: str | list[str]):
        if self._databricks_destination is not None:
            self._databricks_destination.tables = tables
        self._tables = tables

    @property
    def volume(self) -> str:
        """
        The Databricks managed volume used for staging.
        """
        if self._databricks_destination is not None:
            return self._databricks_destination.volume
        return self._volume

    @volume.setter
    def volume(self, volume: str):
        if self._databricks_destination is not None:
            self._databricks_destination.volume = volume
        self._volume = volume

    @property
    def catalog(self) -> str | None:
        """
        The catalog name used to qualify table names.
        """
        if self._databricks_destination is not None:
            return self._databricks_destination.catalog
        return self._catalog

    @catalog.setter
    def catalog(self, catalog: str | None):
        if self._databricks_destination is not None:
            self._databricks_destination.catalog = catalog
        self._catalog = catalog

    @property
    def schema(self) -> str | None:
        """
        The schema name used to qualify table names.
        """
        if self._databricks_destination is not None:
            return self._databricks_destination.schema
        return self._schema

    @schema.setter
    def schema(self, schema: str | None):
        if self._databricks_destination is not None:
            self._databricks_destination.schema = schema
        self._schema = schema

    @property
    def warehouse(self) -> str | None:
        """
        The warehouse name used for uploads.
        """
        if self._databricks_destination is not None:
            return self._databricks_destination.warehouse
        return self._warehouse

    @warehouse.setter
    def warehouse(self, warehouse: str | None):
        if self._databricks_destination is not None:
            self._databricks_destination.warehouse = warehouse
        self._warehouse = warehouse

    @property
    def warehouse_id(self) -> str | None:
        """
        The warehouse ID used for uploads.
        """
        if self._databricks_destination is not None:
            return self._databricks_destination.warehouse_id
        return self._warehouse_id

    @warehouse_id.setter
    def warehouse_id(self, warehouse_id: str | None):
        if self._databricks_destination is not None:
            self._databricks_destination.warehouse_id = warehouse_id
        self._warehouse_id = warehouse_id

    @property
    def if_table_exists(self) -> IfTableExistStrategySpec:
        """
        The strategy to follow when the target table already exists.
        """
        if self._databricks_destination is not None:
            return self._databricks_destination.if_table_exists
        return self._if_table_exists

    @if_table_exists.setter
    def if_table_exists(self, if_table_exists: IfTableExistStrategySpec):
        if self._databricks_destination is not None:
            self._databricks_destination.if_table_exists = if_table_exists
        self._if_table_exists = if_table_exists

    @property
    def schema_strategy(self) -> SchemaStrategySpec:
        """
        The strategy to follow when appending to a table with an existing
        schema.
        """
        if self._databricks_destination is not None:
            return self._databricks_destination.schema_strategy
        return self._schema_strategy

    @schema_strategy.setter
    def schema_strategy(self, schema_strategy: SchemaStrategySpec):
        if self._databricks_destination is not None:
            self._databricks_destination.schema_strategy = schema_strategy
        self._schema_strategy = schema_strategy

    # --- Framework-facing stream() ---

    def stream(self, working_dir: str, *results):
        """
        Write the results to Databricks.
        """
        if self._databricks_destination is None:
            raise RuntimeError(
                "Cannot call stream() before the conn has been resolved to a "
                "DatabricksConn."
            )
        return self._databricks_destination.stream(working_dir, *results)

    def __repr__(self) -> str:
        return (
            f"{self.__class__.__name__}("
            f"conn={self.conn}, "
            f"tables={self.tables}, "
            f"volume={self.volume!r}, "
            f"catalog={self.catalog!r}, "
            f"schema={self.schema!r}, "
            f"warehouse={self.warehouse!r}, "
            f"warehouse_id={self.warehouse_id!r}, "
            f"if_table_exists={self.if_table_exists!r}, "
            f"schema_strategy={self.schema_strategy!r})"
        )
