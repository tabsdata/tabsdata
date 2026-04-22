#
# Copyright 2026 Tabs Data Inc.
#

from __future__ import annotations

import logging
from typing import List, Union

from tabsdata._io.connections.named_connection.named_connection import NamedConn
from tabsdata._io.plugin import SourcePlugin
from tabsdata._tabsserver.function.offset_utils import OffsetReturn
from tabsdata._utils.validation import _td_validate_call
from tabsdata.connector.databricks._connection import DatabricksConn
from tabsdata.connector.databricks._connector import DatabricksSource
from tabsdata.exceptions import ErrorCode

logger = logging.getLogger(__name__)
logger.setLevel(logging.DEBUG)


class DatabricksSrc(SourcePlugin):
    """
    Source plugin for reading data from Databricks.
    """

    @_td_validate_call(ErrorCode.SOCE102)
    def __init__(
        self,
        conn: DatabricksConn | NamedConn,
        queries: str | list[str],
        initial_values: dict | None = None,
        warehouse: str | None = None,
        warehouse_id: str | None = None,
        **kwargs,
    ):
        """
        Initialize a DatabricksSrc instance.

        Args:
            conn: The Databricks connection configuration (host_url and token).
            queries: A SQL query or list of queries to execute. Each query
                produces one output. Queries may contain ``:param_name``
                placeholders that get replaced with ``initial_values`` entries
                to support incremental ingestion.
            initial_values: Initial values for query parameters. Placeholders
                of the form ``:param_name`` are replaced with the matching
                value. The framework updates these between runs.
            warehouse: Warehouse name. Provide either ``warehouse`` or
                ``warehouse_id``, not both.
            warehouse_id: Warehouse ID. Use ``None`` if ``warehouse`` is
                provided.
        """
        # Stash all non-conn state first so the conn setter can rebuild the
        # inner DatabricksSource with a complete picture.
        self._queries = queries
        self._initial_values = initial_values
        self._warehouse = warehouse
        self._warehouse_id = warehouse_id
        self._kwargs = kwargs
        self._databricks_source: DatabricksSource | None = None
        # Assigning conn triggers the setter, which (re)builds the inner
        # DatabricksSource when conn is a resolved DatabricksConn.
        self.conn = conn

    def _build_source(self) -> DatabricksSource:
        return DatabricksSource(
            host_url=self._conn.host_url,
            token=self._conn.token,
            queries=self._queries,
            initial_values=self._initial_values,
            warehouse=self._warehouse,
            warehouse_id=self._warehouse_id,
            **self._kwargs,
        )

    @property
    def conn(self) -> DatabricksConn | NamedConn:
        """
        The Databricks connection configuration.
        """
        return self._conn

    @conn.setter
    @_td_validate_call(ErrorCode.SOCE102)
    def conn(self, conn: DatabricksConn | NamedConn):
        self._conn = conn
        # Rebuild the inner source so its host_url and token reflect the new
        # conn. When conn is a NamedConn (not yet resolved), skip rebuilding.
        if isinstance(conn, DatabricksConn):
            self._databricks_source = self._build_source()
        else:
            self._databricks_source = None

    @property
    def queries(self) -> List[str]:
        """
        The SQL queries to execute.
        """
        if self._databricks_source is not None:
            return self._databricks_source.queries
        return [self._queries] if isinstance(self._queries, str) else self._queries

    @queries.setter
    def queries(self, queries: str | list[str]):
        if self._databricks_source is not None:
            self._databricks_source.queries = queries
        self._queries = queries

    @property
    def initial_values(self) -> dict:
        """
        The initial values for query-parameter placeholders.
        """
        if self._databricks_source is not None:
            return self._databricks_source.initial_values
        return self._initial_values or {}

    @initial_values.setter
    def initial_values(self, initial_values: dict | None):
        if self._databricks_source is not None:
            self._databricks_source.initial_values = initial_values
        self._initial_values = initial_values

    @property
    def warehouse(self) -> str | None:
        """
        The warehouse name used for query execution.
        """
        if self._databricks_source is not None:
            return self._databricks_source.warehouse
        return self._warehouse

    @warehouse.setter
    def warehouse(self, warehouse: str | None):
        if self._databricks_source is not None:
            self._databricks_source.warehouse = warehouse
        self._warehouse = warehouse

    @property
    def warehouse_id(self) -> str | None:
        """
        The warehouse ID used for query execution.
        """
        if self._databricks_source is not None:
            return self._databricks_source.warehouse_id
        return self._warehouse_id

    @warehouse_id.setter
    def warehouse_id(self, warehouse_id: str | None):
        if self._databricks_source is not None:
            self._databricks_source.warehouse_id = warehouse_id
        self._warehouse_id = warehouse_id

    # --- Framework-facing state ---

    @property
    def _offset_return(self) -> str:
        """
        How offset updates are returned to the framework. Mirrors
        DatabricksSource's contract.
        """
        return OffsetReturn.FUNCTION.value

    def chunk(self, working_dir: str) -> Union[str, List[str]]:
        """
        Execute the configured queries and return result files.
        """
        if self._databricks_source is None:
            raise RuntimeError(
                "Cannot call chunk() before the conn has been resolved to a "
                "DatabricksConn."
            )
        return self._databricks_source.chunk(working_dir)

    def __repr__(self) -> str:
        return (
            f"{self.__class__.__name__}("
            f"conn={self.conn}, "
            f"queries={self.queries}, "
            f"initial_values={self.initial_values}, "
            f"warehouse={self.warehouse!r}, "
            f"warehouse_id={self.warehouse_id!r})"
        )
