#
# Copyright 2025 Tabs Data Inc.
#

from __future__ import annotations

import logging
from typing import Any, Optional, get_args

from pydantic import PositiveInt

from tabsdata._io.connections.mysql_connection import MySQLConn
from tabsdata._io.constants import _LoaderSpec
from tabsdata._io.inputs.sql.shared_types import (
    MySQLSrcLoaderSpec,
    QueriesSpec,
    SchemaOverrideSpec,
)
from tabsdata._io.plugin import SourcePlugin
from tabsdata._io.sql_io_helpers.sql_helper_factories import (
    get_sql_source_helper,
)
from tabsdata._tabsserver.function.offset_utils import OffsetReturn
from tabsdata._utils.sql_utils import (
    get_tableframes_from_sources,
    validate_schema_overrides_dict,
    validate_schema_overrides_list,
)
from tabsdata._utils.validation import _td_validate_call
from tabsdata.exceptions import (
    ErrorCode,
    SourceConfigurationError,
)
from tabsdata.tableframe import TableFrame
from tabsdata.tableframe.typing import DataType

logger = logging.getLogger(__name__)
logger.setLevel(logging.DEBUG)


class MySQLSrc(SourcePlugin):
    """
    :categories: source
    Source plugin for reading data from MySQL.
    """

    @_td_validate_call(ErrorCode.SOCE102)
    def __init__(
        self,
        conn: MySQLConn,
        queries: QueriesSpec,
        initial_values: Optional[dict[str, Any]] = None,
        transactional: bool = True,
        chunk_size: PositiveInt = 100000,
        loader: MySQLSrcLoaderSpec = "polars_sqlalchemy",
        schema_overrides: SchemaOverrideSpec = None,
    ):
        """
        Initialize a MySQLSource for reading data from MySQL.

        Args:
            conn: The MySQL connection configuration.
            queries: The SQL query or list of queries to execute.
            initial_values: Initial values for query parameters. Defaults to None.
            transactional: Whether to use transactions for reading to ensure
                consistency. Defaults to True.
            chunk_size: The chunk size in records for reading large queries in batches.
                Defaults to 100000.
            loader: The data processing loader to use for executing SQL imports.
            schema_overrides: Type mapping overrides for the columns of the queries. It
                can be a single mapping, a dictionary of mappings or a list of mappings.
                It can be a single dictionary mapping only when the queries is a single
                query. If using a dictionary of mappings, the keys must match any of
                names of the Tabsdata tables produced by the publisher. If using a list
                of mappings, the list of mappings must have the same size as the queries
                list; for queries there is no need for mapping the corresponding element
                in the list can be None. The mapping dictionary must have column names
                from the corresponding query as keys and the TableFrame DataType to cast
                the column to. The type casting from the original column type to the
                desired type must be supported.
        """
        self._conn = conn
        self._queries = self._normalize_queries(queries)
        self._initial_values = initial_values or {}
        self._transactional = transactional
        self._chunk_size = chunk_size
        self._loader = self._validate_loader(loader)
        self._schema_overrides = self._validate_schema_overrides(schema_overrides)

    @property
    def conn(self) -> MySQLConn:
        """
        The MySQL connection configuration.
        """
        return self._conn

    @property
    def queries(self) -> list[str]:
        """
        The SQL queries to execute to read data.
        """
        return self._queries

    def _normalize_queries(self, queries: QueriesSpec):
        """
        Normalize the "queries" parameter to a list of strings.

        Args:
            queries: The SQL queries to execute to read data.
        """
        if isinstance(queries, str):
            queries = [queries]
        return queries

    @property
    def initial_values(self) -> dict:
        """
        The initial values for the parameters in the SQL queries.
        """
        return self._initial_values

    @initial_values.setter
    @_td_validate_call(ErrorCode.SOCE103)
    def initial_values(self, initial_values: Optional[dict[str, Any]]):
        """
        Set the initial values for the parameters in the SQL queries.

        Args:
            initial_values: The initial values for the parameters in the SQL queries.
        """
        self._initial_values = initial_values or {}

    @property
    def _offset_return(self) -> str:
        """
        Indicates whether the offset is returned by modifying the
        'initial_values' attribute of the plugin, or if it is part
        of the function return"""
        return OffsetReturn.FUNCTION.value

    @property
    def transactional(self) -> bool:
        """
        Whether to use transactions for reading to ensure consistency.
        """
        return self._transactional

    @property
    def chunk_size(self) -> int:
        """
        The chunk size in records for reading large queries in batches.
        """
        return self._chunk_size

    @property
    def loader(self) -> MySQLSrcLoaderSpec:
        """
        The data processing loader to use for executing SQL imports.
        """
        return self._loader.value

    def _validate_loader(self, loader: MySQLSrcLoaderSpec):
        """
        Validate the loader parameter used to execute SQL imports.
        Args:
            loader: The data processing loader to use for executing SQL imports.
        """
        if loader not in get_args(MySQLSrcLoaderSpec):
            raise SourceConfigurationError(
                ErrorCode.SOCE52,
                self.__class__.__name__,
                get_args(MySQLSrcLoaderSpec),
                loader,
            )
        return _LoaderSpec(loader)

    @property
    def schema_overrides(self) -> Optional[list[dict[str, DataType]]]:
        """
        The schema overrides for the tables being read.
        """
        return self._schema_overrides

    def _validate_schema_overrides(self, schema_overrides: SchemaOverrideSpec):
        """
        Validate the schema overrides for the tables being read.
        Args:
            schema_overrides: The schema overrides for the tables being read.
        """
        if schema_overrides is None:
            return None
        elif isinstance(schema_overrides, list):
            validate_schema_overrides_list(schema_overrides, self.queries)
        elif isinstance(schema_overrides, dict):
            validate_schema_overrides_dict(schema_overrides, self.queries)
            schema_overrides = [schema_overrides]
        else:
            raise SourceConfigurationError(
                ErrorCode.SOCE53, self.__class__.__name__, type(schema_overrides)
            )

        return schema_overrides

    def stream(self, working_dir: str) -> list[TableFrame]:
        """
        Execute the query and load the results as a list of TableFrames.

        Args:
            working_dir: The directory where the Parquet files will be saved.
        Returns:
            A list of TableFrames containing the query results.
        """
        logger.debug(f"Triggering {self}")
        helper = get_sql_source_helper(self)

        local_sources = helper._chunk(working_dir=working_dir)
        logger.debug(f"Chunked sources in: {working_dir}")

        table_frames = get_tableframes_from_sources(local_sources)

        logger.debug(f"Returning {len(table_frames)} TableFrames from {self}")
        return table_frames
