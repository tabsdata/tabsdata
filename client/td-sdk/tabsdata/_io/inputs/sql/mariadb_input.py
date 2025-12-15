#
# Copyright 2025 Tabs Data Inc.
#

from __future__ import annotations

import logging
from typing import Any, Literal, get_args

from tabsdata._io.connections.mariadb_connection import MariaDBConn
from tabsdata._io.inputs.helpers.helper_constants import _LoaderSpec
from tabsdata._io.inputs.helpers.source_helper_factory import (
    get_sql_source_helper,
)
from tabsdata._io.inputs.sql.shared_types import QueriesSpec, SchemaOverrideSpec
from tabsdata._io.inputs.sql_inputs import _validate_initial_values_type
from tabsdata._io.plugin import SourcePlugin
from tabsdata._tabsserver.function.offset_utils import OffsetReturn
from tabsdata._utils.sql_utils import (
    get_tableframes_from_sources,
    validate_schema_overrides_dict,
    validate_schema_overrides_list,
)
from tabsdata.exceptions import (
    ErrorCode,
    SourceConfigurationError,
)
from tabsdata.tableframe import TableFrame
from tabsdata.tableframe.typing import DataType

logger = logging.getLogger(__name__)
logger.setLevel(logging.DEBUG)

MariaDBLoaderSpec = Literal["polars_sqlalchemy", "pandas"]


class MariaDBSrc(SourcePlugin):
    """
    Source plugin for reading data from MariaDB.
    """

    def __init__(
        self,
        conn: MariaDBConn,
        queries: QueriesSpec,
        initial_values: dict[str, Any] | None = None,
        transactional: bool = True,
        chunk_size: int = 100000,
        loader: MariaDBLoaderSpec = "polars_sqlalchemy",
        schema_overrides: SchemaOverrideSpec = None,
    ):
        """
        Initialize a MariaDBSrc instance.

        Args:
            conn (MariaDBConn): The MariaDB connection configuration.
            queries (str | list[str]): The SQL query or list of queries to execute.
            initial_values (dict | None, optional): Initial values for query parameters.
                Defaults to None.
            transactional (bool, optional): Whether to use transactions for reading to
                ensure consistency. Defaults to True.
            chunk_size (int, optional): The chunk size in records for reading large
                queries in batches. Defaults to 100000.
            loader (MariaDBLoaderSpec, optional): The data processing loader to
                use for executing SQL imports.
            schema_overrides (dict[str, DataType] | list[dict[str, DataType]] | None,
                optional):
                Type mapping overrides for the columns of the queries.
                It can be a single mapping, a dictionary of mappings or a list
                of mappings. It can be a single dictionary mapping only when the
                queries is a single query. If using a dictionary of mappings,
                the keys must be match any of names of  the Tabsdata tables
                produced by the publisher. If using a a list of mappings, the
                list of mappings must have the same size as the queries list;
                for queries there is no need for mapping the corresponding
                element in the list can be None.The mapping dictionary must have
                column names from the corresponding query as keys and the
                TableFrame DataType to cast the column to. The type casting from
                the original column type to the desired type must be supported.

        Raises:
            SourceConfigurationError: If any source configurations are invalid.
        """
        self.conn = conn
        self.queries = queries
        self.initial_values = initial_values or {}
        self.transactional = transactional
        self.chunk_size = chunk_size
        self.loader = loader
        self.schema_overrides = schema_overrides

    @property
    def conn(self) -> MariaDBConn:
        """
        MariaDBConn: The MariaDB connection configuration.
        """
        return self._conn

    @conn.setter
    def conn(self, conn: MariaDBConn):
        """
        Set the MariaDB connection configuration.
        Args:
            conn (MariaDBConn): The MariaDB connection configuration.
        """
        if not isinstance(conn, MariaDBConn):
            raise SourceConfigurationError(
                ErrorCode.SOCE46,
                self.__class__.__name__,
                MariaDBConn.__name__,
                type(conn),
            )
        self._conn = conn

    @property
    def queries(self) -> list[str]:
        """
        list[str]: The SQL queries to execute to read data.
        """
        return self._queries

    @queries.setter
    def queries(self, queries: QueriesSpec):
        """
        Sets the SQL queries to execute to read data.

        Args:
            queries (str | list[str]): The SQL queries to execute to read data.
        """

        if not isinstance(queries, (str, list)):
            raise SourceConfigurationError(
                ErrorCode.SOCE47, self.__class__.__name__, type(queries)
            )
        if isinstance(queries, list) and not all(
            isinstance(single_query, str) for single_query in queries
        ):
            raise SourceConfigurationError(
                ErrorCode.SOCE47, self.__class__.__name__, type(queries)
            )
        if isinstance(queries, str):
            queries = [queries]
        self._queries = queries

    @property
    def initial_values(self) -> dict:
        """
        dict: The initial values for the parameters in the SQL queries.
        """
        return self._initial_values

    @initial_values.setter
    def initial_values(self, initial_values: dict | None):
        """
        Sets the initial values for the parameters in the SQL queries.

        Args:
            initial_values (dict): The initial values for the parameters in the SQL
                queries.
        """
        if not initial_values:
            self._initial_values = {}
        elif not isinstance(initial_values, dict):
            raise SourceConfigurationError(
                ErrorCode.SOCE48, self.__class__.__name__, type(initial_values)
            )
        else:
            _validate_initial_values_type(initial_values)
            self._initial_values = initial_values

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
        bool: Whether to use transactions for reading to ensure consistency.
        """
        return self._transactional

    @transactional.setter
    def transactional(self, transactional: bool):
        """
        Sets the transactional property.

        Args:
            transactional (bool): Whether to use transactions for reading to ensure
            consistency.
        """
        if not isinstance(transactional, bool):
            raise SourceConfigurationError(
                ErrorCode.SOCE49, self.__class__.__name__, type(transactional)
            )
        self._transactional = transactional

    @property
    def chunk_size(self) -> int:
        """
        int: The chunk size in records for reading large queries in batches.
        """
        return self._chunk_size

    @chunk_size.setter
    def chunk_size(self, chunk_size: int):
        """
        Sets the chunk size reading large queries in batches.

        Args:
            chunk_size (int): The chunk size in records for reading large queries in
            batches.
        """
        if not isinstance(chunk_size, int):
            raise SourceConfigurationError(
                ErrorCode.SOCE50, self.__class__.__name__, type(chunk_size)
            )
        if chunk_size <= 0:
            raise SourceConfigurationError(
                ErrorCode.SOCE51, self.__class__.__name__, chunk_size
            )
        self._chunk_size = chunk_size

    @property
    def loader(self) -> MariaDBLoaderSpec:
        """
        str: The data processing loader to use for executing SQL imports.
        """
        return self._loader.value

    @loader.setter
    def loader(self, loader: MariaDBLoaderSpec):
        """
        Sets the loader used to execute SQL imports.
        Args:
            loader (LoaderSpecLiteral): The data processing loader to use for executing
                SQL imports.
        """
        if loader not in get_args(MariaDBLoaderSpec):
            raise SourceConfigurationError(
                ErrorCode.SOCE52,
                self.__class__.__name__,
                get_args(MariaDBLoaderSpec),
                loader,
            )
        self._loader = _LoaderSpec(loader)

    @property
    def schema_overrides(self) -> list[dict[str, DataType]] | None:
        """
        list[dict[str, DataType]] | None:
            The schema overrides for the tables being read.
        """
        return self._schema_overrides

    @schema_overrides.setter
    def schema_overrides(self, schema_overrides: SchemaOverrideSpec):
        """
        Sets the schema overrides for the tables being read.
        Args:
            schema_overrides (dict[str, DataType] | list[dict[str, DataType]] | None):
                The schema overrides for the tables being read.
        """
        if schema_overrides is not None:
            valid_types = (dict, list)
            if not isinstance(schema_overrides, valid_types):
                raise SourceConfigurationError(
                    ErrorCode.SOCE53, self.__class__.__name__, type(schema_overrides)
                )
            if isinstance(schema_overrides, list):
                validate_schema_overrides_list(schema_overrides, self.queries)
            else:
                validate_schema_overrides_dict(schema_overrides, self.queries)

        if isinstance(schema_overrides, dict):
            schema_overrides = [schema_overrides]
        self._schema_overrides = schema_overrides

    def stream(self, working_dir: str) -> list[TableFrame]:
        """
        Execute the query and load the results as a list of TableFrames.

        Args:
            working_dir (str): The directory where the Parquet files will be saved.
        Returns:
            list[TableFrame]: A list of TableFrames containing the query results.
        """
        logger.debug(f"Triggering {self}")
        helper = get_sql_source_helper(self)

        local_sources = helper._chunk(working_dir=working_dir)
        logger.debug(f"Chunked sources in: {working_dir}")

        table_frames = get_tableframes_from_sources(local_sources)

        logger.debug(f"Returning {len(table_frames)} TableFrames from {self}")
        return table_frames
