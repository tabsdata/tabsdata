#
# Copyright 2025 Tabsdata Inc.
#

from __future__ import annotations

import io
import logging
import os
import time
import uuid
from concurrent.futures import ThreadPoolExecutor
from dataclasses import dataclass, field
from typing import TYPE_CHECKING, Any, List, Union

import polars as pl
from tenacity import retry, stop_after_attempt, wait_fixed
from typing_extensions import deprecated

from tabsdata._io.inputs.sql_inputs import (
    _replace_initial_values,
    _validate_initial_values_type,
)
from tabsdata._io.outputs.shared_enums import (
    IfTableExistsStrategy,
    IfTableExistStrategySpec,
    SchemaStrategy,
    SchemaStrategySpec,
)
from tabsdata._io.plugin import DestinationPlugin, SourcePlugin
from tabsdata._secret import DirectSecret, Secret
from tabsdata._tabsserver.function.offset_utils import OffsetReturn
from tabsdata._utils.annotations import unstable, unstable_category
from tabsdata.exceptions import ErrorCode, SourceConfigurationError

if TYPE_CHECKING:
    import databricks.sdk as dbsdk
    import databricks.sql as dbsql

logger = logging.getLogger(__name__)
logger.setLevel(logging.DEBUG)

DEFAULT_POLL_INTERVAL_SECONDS = 2
DEFAULT_DOWNLOAD_THREADS = 4
DOWNLOAD_CONNECT_TIMEOUT_SECONDS = 10
DOWNLOAD_READ_TIMEOUT_SECONDS = 300
DOWNLOAD_RETRY_ATTEMPTS = 12
DOWNLOAD_RETRY_WAIT_SECONDS = 5
PARQUET_COMPRESSION = "snappy"
ASYNC_EXECUTION_WAIT_TIMEOUT = "0s"


def _table_fqn_4sdk(table: str) -> str:
    """
    Returns the fully qualified name of the table for Databricks SDK.
    """
    try:
        catalog, schema, table_name = table.split(".")
        return f"{catalog}.{schema}.{table_name}"
    except ValueError:
        raise ValueError(
            "The table name must be fully qualified in the form "
            "'catalog.schema.table_name'. Tried to fully qualify the table, "
            f"but got '{table}' instead"
        )


def _table_fqn_4sql(table: str) -> str:
    """
    Returns the fully qualified name of the table for Databricks SQL.
    """
    try:
        catalog, schema, table_name = table.split(".")
        return f"`{catalog}`.`{schema}`.`{table_name}`"
    except ValueError:
        raise ValueError(
            "The table name must be fully qualified in the form "
            "'catalog.schema.table_name'. Tried to fully qualify the table, "
            f"but got '{table}' instead"
        )


@unstable()
@deprecated(
    """Class 'DatabricksSource' is considered unstable and is subject to change in future releases.""",  # noqa: E231,E241,E501
    category=unstable_category(
        reason=(
            "Class 'DatabricksSource' may undergo API and feature changes as it evolves"
            " toward full capability and stability."
        ),
        since="1.9.0",
        replacement="There is no current stable replacement for this class.",
    ),
)
class DatabricksSource(SourcePlugin):
    """Legacy: Read from Databricks. Use DatabricksSrc instead.
    :categories: source

    Databricks based data inputs.
    """

    def __init__(
        self,
        host_url: str,
        token: str | Secret,
        queries: list[str] | str,
        initial_values: dict | None = None,
        warehouse: str | None = None,
        warehouse_id: str | None = None,
        **kwargs,
    ):
        """
        Initializes the DatabricksSource with the configuration desired to read
        the data.

        Args:
            host_url: Databricks URL

            token: Databricks Personal Account Token (PAT). The user owning the PAT
                requires 'USE CATALOG', 'USE SCHEMA' & 'SELECT' privileges.

            queries: SQL query or queries to execute. Each query will produce one
                output. Queries can contain placeholders in the form ':param_name'
                that will be replaced with the values from 'initial_values'.

            initial_values: Initial values for query parameters. Placeholders in
                queries of the form ':param_name' will be replaced with the
                corresponding value. These values are updated between runs to support
                incremental ingestion. Defaults to None.

            warehouse: Warehouse name (use either this or warehouse_id) that will be
                used for executing queries. The user has to have privileges to use the
                warehouse.

            warehouse_id: Warehouse Id. Use `null` if warehouse name is provided.
        """
        try:
            # noinspection PyShadowingNames
            import databricks.sdk as dbsdk  # noqa: F401
            from databricks.sdk.core import Config, pat_auth  # noqa: F401
        except ImportError as e:
            raise ImportError(
                "The 'tabsdata-databricks' package is missing some dependencies. "
                "You can get them by installing 'tabsdata['databricks']'"
                f" (cause: {e})"
            ) from e
        self.host_url = host_url
        self.token = token
        self.queries = queries
        self.initial_values = initial_values
        self.warehouse = warehouse
        self.warehouse_id = warehouse_id
        if warehouse and warehouse_id:
            raise ValueError(
                "You cannot provide both 'warehouse' and 'warehouse_id'. "
                "Please provide exactly one of them."
            )
        elif not warehouse and not warehouse_id:
            raise ValueError(
                "You must provide either 'warehouse' or 'warehouse_id'. "
                "Please provide exactly one of them."
            )

        self.kwargs = kwargs
        self._support_databricks_logging_level = self.kwargs.get(
            "support_databricks_logging_level",
            logging.ERROR,
        )
        self._support_poll_interval_seconds = self.kwargs.get(
            "support_poll_interval_seconds",
            DEFAULT_POLL_INTERVAL_SECONDS,
        )
        self._download_threads = self.kwargs.get(
            "download_threads",
            DEFAULT_DOWNLOAD_THREADS,
        )

    @property
    def token(self) -> Secret:
        return self._token

    @token.setter
    def token(self, token: str | Secret):
        if isinstance(token, Secret):
            self._token = token
        elif isinstance(token, str):
            self._token = DirectSecret(token)
        else:
            raise TypeError(
                f"The token must be a string or a Secret;got {type(token)} instead."
            )

    @property
    def host_url(self) -> str:
        return self._host

    @host_url.setter
    def host_url(self, host: str):
        if not isinstance(host, str):
            raise TypeError(f"The host must be a string; got {type(host)} instead.")
        self._host = host

    @property
    def queries(self) -> List[str]:
        return self._queries

    @queries.setter
    def queries(self, value: str | list[str]):
        if isinstance(value, str):
            self._queries = [value]
        elif isinstance(value, list) and all(isinstance(q, str) for q in value):
            self._queries = value
        else:
            raise TypeError(
                "The 'queries' parameter must be a string or a list of "
                f"strings; "
                f"got a parameter of type '{type(value)}' instead."
            )

    @property
    def initial_values(self) -> dict:
        """
        The initial values for the parameters in the SQL queries.
        """
        return self._initial_values

    @initial_values.setter
    def initial_values(self, initial_values: dict | None):
        """
        Sets the initial values for the parameters in the SQL queries.

        Args:
            initial_values: The initial values for the parameters in the SQL
                queries.
        """
        if not initial_values:
            self._initial_values = {}
        elif not isinstance(initial_values, dict):
            raise SourceConfigurationError(ErrorCode.SOCE34, type(initial_values))
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

    def _get_workspace_client(self) -> dbsdk.WorkspaceClient:
        # noinspection PyShadowingNames
        import databricks.sdk as dbsdk

        # noinspection DuplicatedCode
        ws_client = dbsdk.WorkspaceClient(
            host=self.host_url,
            token=self.token.secret_value,
        )
        logger.debug(f"Workspace client created: {ws_client}")
        if self.warehouse_id is None:
            for warehouse in ws_client.warehouses.list():
                if warehouse.name == self.warehouse:
                    self.warehouse_id = warehouse.id
                    break
        return ws_client

    def chunk(self, working_dir: str) -> Union[str, List[str]]:
        logging.getLogger("databricks").setLevel(self._support_databricks_logging_level)
        ws_client = self._get_workspace_client()
        resulting_files = []
        for index, statement in enumerate(self._queries):
            if self.initial_values:
                statement = _replace_initial_values(statement, self.initial_values)
            logger.info(f"Executing query {index}: '{statement}'")
            result_file = self._execute_single_query(
                ws_client, statement, working_dir, index
            )
            resulting_files.append(result_file)
        logger.info(
            f"All queries executed successfully; results saved to '{resulting_files}'"
        )
        return resulting_files

    @staticmethod
    def _chunk_links_iterator(ws_client, response):
        import threading

        @dataclass
        class ChunkIteratorState:
            statement_id: str
            payload: Any | None
            links: list = field(default_factory=list)
            position: int = 0
            chunk: int = 0
            exhausted: bool = False

        lock = threading.Lock()
        state = ChunkIteratorState(
            statement_id=response.statement_id,
            payload=response.result,
        )

        def next_link():
            with lock:
                while True:
                    if state.position < len(state.links):
                        link = state.links[state.position]
                        chunk = state.chunk
                        state.position += 1
                        state.chunk += 1
                        return chunk, link
                    if state.exhausted:
                        return None
                    payload = state.payload
                    if payload is not None and payload.external_links:
                        state.links = list(payload.external_links)
                        state.position = 0
                        last_link = state.links[-1]
                        next_chunk_index = last_link.next_chunk_index
                        if next_chunk_index is not None:
                            state.payload = ws_client.statement_execution.get_statement_result_chunk_n(  # noqa: E501
                                state.statement_id,
                                next_chunk_index,
                            )
                        else:
                            state.payload = None
                    else:
                        state.exhausted = True
                        return None

        return next_link

    @staticmethod
    @retry(
        stop=stop_after_attempt(DOWNLOAD_RETRY_ATTEMPTS),
        wait=wait_fixed(DOWNLOAD_RETRY_WAIT_SECONDS),
        reraise=True,
    )
    def _download_chunk(link):
        import pyarrow.ipc as ipc
        import requests

        with requests.get(
            link.external_link,
            stream=True,
            timeout=(
                DOWNLOAD_CONNECT_TIMEOUT_SECONDS,
                DOWNLOAD_READ_TIMEOUT_SECONDS,
            ),
        ) as response:
            response.raise_for_status()
            return ipc.open_stream(io.BytesIO(response.content)).read_all()

    @staticmethod
    def _process_chunks(next_link, chunk_writer):
        while True:
            item = next_link()
            if item is None:
                return
            chunk, link = item
            worker_start_time = time.time()
            table = DatabricksSource._download_chunk(link)
            worker_end_time = time.time()
            logger.debug(
                f"Chunk {chunk + 1} -> "
                f"download time: {worker_end_time - worker_start_time:.3f}s"
            )
            chunk_writer.submit(chunk, table)

    def _execute_single_query(
        self,
        ws_client: dbsdk.WorkspaceClient,
        statement: str,
        working_dir: str,
        index: int,
    ) -> str:
        import threading

        import pyarrow as pa
        import pyarrow.parquet as pq

        from databricks.sdk.service.sql import Disposition, Format, StatementState

        uuid_string = uuid.uuid4().hex[:16]
        intermediate_file_name = f"intermediate_{index}_{uuid_string}.parquet"
        intermediate_file = os.path.join(working_dir, intermediate_file_name)

        logger.debug(f"Submitting statement to warehouse '{self.warehouse_id}'")
        query_start_time = time.time()
        response = ws_client.statement_execution.execute_statement(
            warehouse_id=self.warehouse_id,
            statement=statement,
            disposition=Disposition.EXTERNAL_LINKS,
            format=Format.ARROW_STREAM,
            wait_timeout=ASYNC_EXECUTION_WAIT_TIMEOUT,
        )

        poll_interval = self._support_poll_interval_seconds
        while response.status.state in (StatementState.PENDING, StatementState.RUNNING):
            logger.debug(
                f"Query still running (state={response.status.state}); "
                f"polling again in {poll_interval}s..."
            )
            time.sleep(poll_interval)
            response = ws_client.statement_execution.get_statement(
                response.statement_id
            )
        query_end_time = time.time()
        logger.info(
            f"Query {index} execution took {query_end_time - query_start_time:.3f}s"
        )

        if response.status.state != StatementState.SUCCEEDED:
            raise RuntimeError(
                f"Databricks query failed with state "
                f"'{response.status.state}': {response.status.error}"
            )

        logger.debug(f"Query succeeded; downloading results to '{intermediate_file}'")

        next_link = self._chunk_links_iterator(ws_client, response)

        class ChunkWriter:
            def __init__(self, path: str):
                self._path = path
                self._lock = threading.Lock()
                self._pending: dict[int, pa.Table] = {}
                self._next = 0
                self._writer: pq.ParquetWriter | None = None
                self.written = 0

            def submit(self, chunk: int, table: pa.Table) -> None:
                with self._lock:
                    self._pending[chunk] = table
                    self.drain()

            def drain(self):
                while self._next in self._pending:
                    logger.info(f"Draining pending chunk {self._next}")
                    table = self._pending.pop(self._next)
                    if self._writer is None:
                        self._writer = pq.ParquetWriter(
                            self._path,
                            table.schema,
                            compression=PARQUET_COMPRESSION,
                        )
                    self._writer.write_table(table)
                    self._next += 1
                    self.written += 1

            def close(self):
                with self._lock:
                    self.drain()
                    if self._writer:
                        self._writer.close()
                        self._writer = None

        chunk_writer = ChunkWriter(intermediate_file)

        download_start_time = time.time()
        try:
            if self._download_threads <= 1:
                self._process_chunks(next_link, chunk_writer)
            else:
                with ThreadPoolExecutor(max_workers=self._download_threads) as executor:
                    futures = [
                        executor.submit(self._process_chunks, next_link, chunk_writer)
                        for _ in range(self._download_threads)
                    ]
                    for future in futures:
                        future.result()
        finally:
            chunk_writer.close()
        download_end_time = time.time()
        total_tables = chunk_writer.written
        logger.info(
            f"Query {index}: downloaded and wrote {total_tables} chunks with "
            f"{self._download_threads} threads "
            f"in {download_end_time - download_start_time:.3f}s"
        )

        logger.info(
            f"Query {index}: {total_tables} chunks - "
            f"total elapsed {download_end_time - query_start_time:.3f}s"
        )
        logger.info(f"Query result saved to '{intermediate_file_name}'")
        return intermediate_file

    def __repr__(self):
        return (
            f"{self.__class__.__name__}"
            f"("
            f"host_url='{self._host}', "
            f"queries={self.queries}"
            f")"
        )


class DatabricksDestination(DestinationPlugin):
    """Legacy: Write to Databricks. Use DatabricksDest instead.
    :categories: destination

    Databricks based data outputs.
    """

    def __init__(
        self,
        host_url: str,
        token: str | Secret,
        tables: list[str] | str,
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
        Initializes the DatabricksDestination with the configuration desired to store
        the data.

        Args:
            host_url: Databricks URL

            token: Databricks Personal Account Token (PAT). The user owning the PAT
                requires ‘USE CATALOG’, ‘USE SCHEMA’, ‘READ VOLUME’ & ‘WRITE VOLUME’
                privileges.

            tables: Tables to be created in Databricks. Each table name provided must
                either be fully qualified in the form of ‘catalog.schema.table_name’ or
                have the information provided through the ‘catalog’ or the ‘schema’
                parameters. For example, if a table of the form ‘schema.table_name’ is
                provided, the ‘catalog’ parameter must also be provided. If a table of
                the form ‘table_name’ is provided, both ‘catalog’ and ‘schema’ must be
                provided.

            volume: Name of the Databricks volume. The connector uses an existing
                Databricks managed volume. The files written to the volume are deleted
                after connector finishes writing the data to the Databricks table.

            catalog: Catalog name (optional, can be null if the table name has the
                catalog name.)

            schema: Schema name (optional, can be null if the table name has the schema
                name.)

            warehouse: Warehouse name (use either this or warehouse_id) that will be
                used for uploading data to Databricks tables. The user has to have
                privileges to use the warehouse.

            warehouse_id: Warehouse Id. Use `null` if warehouse name is provided.

            if_table_exists: This is an optional property to define the strategy to
                follow when the table already exists. ‘replace’ will create overwrite
                the existing table, and ‘append’ will append to the existing data in the
                table. Defaults to ‘append’.

            schema_strategy: The is to mention the strategy to follow when appending to
                a table with an existing schema. ‘update’ will update the schema with
                the possible new columns that might exist in the TableFrame. ‘strict’
                will not modify the schema, and will fail if there is any difference.

        """
        try:
            # noinspection PyShadowingNames
            import databricks.sdk as dbsdk  # noqa: F401

            # noinspection PyShadowingNames
            import databricks.sql as dbsql  # noqa: F401
            from databricks.sdk.core import Config, pat_auth  # noqa: F401
        except ImportError as e:
            raise ImportError(
                "The 'tabsdata-databricks' package is missing some dependencies. "
                "You can get them by installing 'tabsdata['databricks']'"
                f" (cause: {e})"
            ) from e
        self.host_url = host_url
        self.token = token
        self.volume = volume
        self.tables = tables
        self.catalog = catalog
        self.schema = schema
        self.warehouse = warehouse
        self.warehouse_id = warehouse_id
        if warehouse and warehouse_id:
            raise ValueError(
                "You cannot provide both 'warehouse' and 'warehouse_id'. "
                "Please provide exactly one of them."
            )
        elif not warehouse and not warehouse_id:
            raise ValueError(
                "You must provide either 'warehouse' or 'warehouse_id'. "
                "Please provide exactly one of them."
            )
        self.if_table_exists = if_table_exists
        self.schema_strategy = schema_strategy

        qualified_tables = []
        for table in self.tables:
            fully_qualified_table = self._fully_qualify_table(table)
            if fully_qualified_table.count(".") != 2:
                raise ValueError(
                    "Each table name provided must either be fully qualified (in the "
                    "form of 'catalog.schema.table_name'), "
                    "or have the information provided through the 'catalog' or the "
                    "'schema' parameters. For example, if a table of the form "
                    "'schema.table_name' is provided, the 'catalog' parameter must "
                    "also be provided. If a table of the form "
                    "'table_name' is provided, both 'catalog' and 'schema' must be "
                    "provided."
                )
            qualified_tables.append(fully_qualified_table)
        self.tables = qualified_tables

        self.kwargs = kwargs
        # We start with the support options, only used when debugging a major issue
        self._support_append_create_table = self.kwargs.get(
            "support_append_create_table", {}
        )
        self._support_append_copy_options = self.kwargs.get(
            "support_append_copy_options", ""
        )
        self._support_append_copy_into = self.kwargs.get("support_append_copy_into", {})
        self._support_replace_create_table = self.kwargs.get(
            "support_replace_create_table", {}
        )
        self._support_replace_copy_options = self.kwargs.get(
            "support_replace_copy_options", ""
        )
        self._support_replace_copy_into = self.kwargs.get(
            "support_replace_copy_into", {}
        )
        self._support_databricks_logging_level = self.kwargs.get(
            "support_databricks_logging_level", logging.ERROR
        )

    @property
    def token(self) -> Secret:
        return self._token

    @token.setter
    def token(self, token: str | Secret):
        if isinstance(token, Secret):
            self._token = token
        elif isinstance(token, str):
            self._token = DirectSecret(token)
        else:
            raise TypeError(
                f"The token must be a string or a Secret, got {type(token)} instead."
            )

    @property
    def host_url(self) -> str:
        return self._host

    @host_url.setter
    def host_url(self, host: str):
        if not isinstance(host, str):
            raise TypeError(f"The host must be a string, got {type(host)} instead.")
        self._host = host

    @property
    def if_table_exists(self) -> IfTableExistStrategySpec:
        """
        str: The strategy to follow when the table already exists.
        """
        return self._if_table_exists

    @if_table_exists.setter
    def if_table_exists(self, if_table_exists: IfTableExistStrategySpec):
        """
        Sets the strategy to follow when the table already exists.

        Args:
            if_table_exists ({'append', 'replace'}): The strategy to
                follow when the table already exists.
                - ‘replace’ will create a new table, overwriting an existing
                one.
                - ‘append’ will append to an existing table.
        """
        valid_values = [
            IfTableExistsStrategy.APPEND.value,
            IfTableExistsStrategy.REPLACE.value,
        ]
        if if_table_exists not in valid_values:
            raise ValueError(
                "The 'if_table_exists' parameter in a DatabricksDestination must be "
                f"one of the following values '{valid_values}', got "
                f"'{if_table_exists}' instead."
            )
        self._if_table_exists = if_table_exists

    @property
    def schema_strategy(self) -> SchemaStrategySpec:
        """
        str: The strategy to follow when appending to a table with an existing schema.
        """
        return self._schema_strategy

    @schema_strategy.setter
    def schema_strategy(self, schema_strategy: SchemaStrategySpec):
        """
        Sets the strategy to follow when appending to a table with an existing schema.

        Args:
            schema_strategy ({'update', 'strict'}): The strategy to
                follow for the schema when the table already exists.
                - ‘update’ will update the schema with the possible new columns that
                    might exist in the TableFrame.
                - ‘strict’ will not modify the schema, and will fail if there is any
                    difference.
        """
        valid_values = [
            SchemaStrategy.UPDATE.value,
            SchemaStrategy.STRICT.value,
        ]
        if schema_strategy not in valid_values:
            raise ValueError(
                "The 'schema_strategy' parameter in a DatabricksDestination must be "
                f"one of the following values '{valid_values}', got "
                f"'{schema_strategy}' instead."
            )
        self._schema_strategy = schema_strategy

    @property
    def tables(self) -> List[str]:
        return self._tables

    @tables.setter
    def tables(self, tables):
        if isinstance(tables, list):
            if not all(isinstance(table, str) for table in tables):
                raise TypeError(
                    "All elements in the 'tables' list must be strings, "
                    f"got '{tables}' instead."
                )
            self._tables = tables
        elif isinstance(tables, str):
            self._tables = [tables]
        else:
            raise TypeError(
                "The 'tables' parameter must be a string or a list of "
                f"strings, got '{tables}' instead."
            )

    def stream(
        self,
        working_dir: str,
        *results: List[pl.LazyFrame | None] | pl.LazyFrame | None,
    ):
        if len(results) != len(self.tables):
            raise ValueError(
                f"The number of results ({len(results)}) does not match the number of "
                f"tables provided ({len(self.tables)}. Please make "
                "sure that the number of results matches the number of tables."
            )
        logging.getLogger("databricks").setLevel(self._support_databricks_logging_level)
        # Chunk the results
        files = self.chunk(working_dir, *results)
        self.write(files)

    def chunk(
        self, working_dir: str, *results: pl.LazyFrame | None
    ) -> List[None | str]:
        list_of_files = []
        logger.info("Chunking the results")
        for index, result in enumerate(results):
            logger.debug(f"Chunking result in position {index}")
            if result is None:
                logger.warning(f"Result in position '{index}' is None.")
                list_of_files.append(None)
            else:
                uuid_string = uuid.uuid4().hex[:16]  # Using only 16 characters to avoid
                # issues with file name length in Windows
                file_name = f"intermediate_{index}_{uuid_string}.parquet".replace(
                    "-", "_"
                )
                intermediate_destination_file = os.path.join(working_dir, file_name)
                logger.debug(f"Sinking the data to '{intermediate_destination_file}'")
                result.sink_parquet(intermediate_destination_file)
                logger.debug(
                    f"File '{intermediate_destination_file}' generated successfully"
                )
                list_of_files.append(intermediate_destination_file)
            logger.debug(f"Chunked result in position {index} successfully")
        logger.info("All results chunked successfully")
        return list_of_files

    def write(self, files):
        """
        This method is used to write the files to the databricks. It is called
        from the stream method, and it is not intended to be called directly.
        """
        # We need to ensure that the connection parameters are fully evaluated
        ws_client, sql_connection = self._get_connections()
        for file_path, table in zip(files, self.tables):
            if file_path is None:
                logger.warning(f"Received None for table '{table}'. No data loaded.")
            else:
                logger.info(f"Starting upload of results to table '{table}'")
                table = self._fully_qualify_table(table)
                logger.debug(f"Storing file '{file_path}' in table '{table}'")
                try:
                    self._upload_single_file_to_table(
                        ws_client, sql_connection, file_path, table
                    )
                    logger.info(f"Uploaded results to table '{table}' successfully.")
                except Exception:
                    logger.error(
                        f"Failed to upload results from file '{file_path}' to table"
                        f" '{table}'"
                    )
                    sql_connection.close()
                    raise
        sql_connection.close()

    def _upload_single_file_to_table(
        self,
        ws_client: dbsdk.WorkspaceClient,
        sql_conn: dbsql.Connection,
        file_path: str,
        table: str,
    ):
        try:
            table_4sdk = _table_fqn_4sdk(table)
            table_catalog, table_schema, _ = table_4sdk.split(".")
        except ValueError:
            raise ValueError(
                "The table name must be fully qualified in the form "
                "'catalog.schema.table_name'. Tried to fully qualify the table, "
                f"but got '{table}' instead"
            )
        file_path_in_volume = self._upload_file_to_volume(
            ws_client, file_path, self.volume, table_catalog, table_schema
        )
        self._perform_upload_to_table(ws_client, sql_conn, file_path_in_volume, table)

    def _upload_file_to_volume(
        self,
        ws_client: dbsdk.WorkspaceClient,
        file_path: str,
        volume: str,
        catalog: str,
        schema: str,
    ) -> str:
        file_name = os.path.basename(file_path)
        logger.debug(
            f"Putting file '{file_path}' in volume '{volume}' with catalog "
            f"'{catalog}' and schema '{schema}'"
        )
        try:
            volume_path = f"/Volumes/{catalog}/{schema}/{volume}"
            file_path_in_volume = f"{volume_path}/{file_name}"
            logger.debug(f"Putting file in path: '{file_path_in_volume}'")
            with open(file_path, "rb") as f:
                ws_client.files.upload(file_path_in_volume, f)
            logger.debug("File put successfully")
            return file_path_in_volume
        except Exception:
            logger.error(f"Failed to put file '{file_path}' in volume '{self.volume}'")
            raise

    def _perform_upload_to_table(
        self,
        ws_client: dbsdk.WorkspaceClient,
        sql_conn: dbsql.Connection,
        file_path_in_volume: str,
        table: str,
    ):
        logger.debug(f"Copying file '{file_path_in_volume}' to table '{table}'")
        table_4sql = _table_fqn_4sql(table)
        try:
            if self.if_table_exists == "replace":
                logger.debug(
                    "'if_table_exists' is set to 'replace', so the current "
                    f"data in table '{table_4sql}' will be truncated."
                )

                # Create the table or replace it, using the parquet file to
                # infer the schema.
                instruction = f"""
                   CREATE OR REPLACE TABLE {table_4sql}
                   USING DELTA
                     AS SELECT * FROM read_files(
                        '{file_path_in_volume}', format => 'parquet'
                     ) LIMIT 0;
                   """
                logger.debug(f"Executing the instruction '{instruction}'")
                result = sql_conn.cursor().execute(
                    instruction, **self._support_replace_create_table
                )
                logger.debug(f"Instruction executed successfully: '{result}'")

                # Copy the data from the parquet file into the table
                #
                # IMPORTANT: databricks will not COPY INTO an existing table the same
                # file name twice (unless COPY_OPTIONS has force=true).
                instruction = f"""
                                   COPY INTO {table_4sql} FROM '{file_path_in_volume}'
                                     FILEFORMAT = PARQUET
                                    {self._support_replace_copy_options}
                                   """
                logger.debug(f"Executing the instruction '{instruction}'")
                result = sql_conn.cursor().execute(
                    instruction, **self._support_replace_copy_into
                )
                logger.debug(f"Instruction executed successfully: '{result}'")
            elif self.if_table_exists == "append":
                logger.debug(
                    "'if_table_exists' is set to 'append', so the data will be "
                    f"appended to table '{table_4sql}' if it already exists."
                )
                if self.schema_strategy == "strict":
                    # for schema_strategy 'strict' use
                    # COPY_OPTIONS('mergeSchema' = 'false')
                    logger.debug(
                        "Schema strategy is 'strict', so the schema will not be "
                        "modified and the operation will fail if there are any "
                        "differences."
                    )
                    merge_options = "'mergeSchema' = 'false'"
                else:
                    logger.debug(
                        "Schema strategy is 'update', so the schema will be updated "
                        "with any new columns that might exist in the file."
                    )
                    # for schema_strategy 'update' use
                    # COPY_OPTIONS('mergeSchema' = 'true')
                    merge_options = "'mergeSchema' = 'true'"

                # Create the table if it does not exist, using the parquet file to
                # infer the schema.
                instruction = f"""
                   CREATE TABLE IF NOT EXISTS {table_4sql}
                   USING DELTA
                     AS SELECT * FROM read_files(
                         '{file_path_in_volume}', format => 'parquet'
                     ) LIMIT 0;
                   """
                logger.debug(f"Executing the instruction '{instruction}'")
                result = sql_conn.cursor().execute(
                    instruction, **self._support_append_create_table
                )
                logger.debug(f"Instruction executed successfully: '{result}'")

                # Copy the data from the parquet file into the table
                #
                # IMPORTANT: databricks will not COPY INTO an existing table the same
                # file name twice (unless COPY_OPTIONS has force=true).
                instruction = f"""
                   COPY INTO {table_4sql} FROM '{file_path_in_volume}'
                     FILEFORMAT = PARQUET
                     COPY_OPTIONS ({merge_options}{self._support_append_copy_options});
                   """
                logger.debug(f"Executing the instruction '{instruction}'")
                result = sql_conn.cursor().execute(
                    instruction, **self._support_append_copy_into
                )
                logger.debug(f"Instruction executed successfully: '{result}'")
            else:
                raise ValueError(
                    f"Invalid value for 'if_table_exists': {self.if_table_exists}. "
                    "Expected 'replace' or 'append'."
                )
            logger.debug(f"File copied successfully to table '{table_4sql}'")
        except Exception:
            logger.error(
                f"Failed to copy file '{file_path_in_volume}' to table '{table_4sql}'"
            )
            raise
        finally:
            # Delete file from volume
            logger.debug(f"Deleting file '{file_path_in_volume}' from volume.")
            # noinspection PyBroadException
            try:
                ws_client.files.delete(file_path_in_volume)
                logger.debug(f"File '{file_path_in_volume}' deleted successfully.")
            except Exception:
                logger.warning(
                    f"Failed to delete file '{file_path_in_volume}' from volume."
                )

    def _fully_qualify_table(self, table: str) -> str:
        """
        Fully qualifies the table with the catalog and schema if they are provided.
        """

        logger.debug(f"Fully qualifying table: '{table}'")
        catalog = self.catalog
        schema = self.schema
        dots = table.count(".")
        if dots == 2:
            # If the table already has two dots, assume it's fully qualified
            logger.debug(f"Table '{table}' is already fully qualified.")
            return table
        elif dots == 1:
            # If the table has one dot, assume it is of the form schema.table
            logger.debug(f"Table '{table}' has one dot, trying to add catalog.")
            if catalog:
                # If catalog is provided, prepend it to the table
                logger.debug(f"Prepending catalog '{catalog}' to table '{table}'.")
                return f"{catalog}.{table}"
            else:
                # If no catalog is provided, return the table as is
                logger.debug(f"No catalog provided, returning table '{table}'.")
                return table
        elif dots == 0:
            logger.debug(f"Table '{table}' has no dots, checking catalog and schema.")
            # If the table has no dots, assume it's just the table name
            # If both catalog and schema are provided, fully qualify the table
            if catalog and schema:
                logger.debug(
                    f"Both catalog '{catalog}' and schema '{schema}' provided, "
                    f"fully qualifying table '{table}'."
                )
                return f"{catalog}.{schema}.{table}"
            elif schema:
                # If only schema is provided, qualify with schema
                logger.debug(
                    f"Only schema '{schema}' provided, qualifying table '{table}'"
                    " with it."
                )
                return f"{schema}.{table}"
            else:
                # If no catalog or schema is provided, return the table as is
                logger.debug(
                    f"No catalog or schema provided, returning table '{table}'."
                )
                return table
        else:
            raise ValueError(
                f"Invalid table name '{table}': "
                f"expected at most 2 dots (catalog.schema.table_name);"
                f" got {dots}."
            )

    def _get_connections(self) -> tuple[dbsdk.WorkspaceClient, dbsql.Connection]:
        # Note: we do the imports here instead of at the top to speed up the import
        # of the tabsdata package, and therefore the td CLI response time.

        # noinspection PyShadowingNames
        import databricks.sdk as dbsdk

        # noinspection PyShadowingNames
        import databricks.sql as dbsql
        from databricks.sdk.core import Config, pat_auth

        # noinspection DuplicatedCode
        ws_client = dbsdk.WorkspaceClient(
            host=self.host_url,
            token=self.token.secret_value,
        )
        logger.debug(f"Workspace client created: {ws_client}")
        if self.warehouse_id is None:
            for warehouse in ws_client.warehouses.list():
                if warehouse.name == self.warehouse:
                    self.warehouse_id = warehouse.id
                    break

        def credentials_provider():
            config = Config(
                host=self.host_url,
                token=self.token.secret_value,
            )
            return pat_auth(config)

        sql_connection = dbsql.connect(
            server_hostname=self.host_url,  # Note: it looks like the host for the
            # workspace client requires https, while this one does not. Make sure
            # this works properly, and decide which one the user will provide.
            http_path=f"/sql/1.0/warehouses/{self.warehouse_id}",
            credentials_provider=credentials_provider,
        )
        logger.debug(f"SQL connection created: {sql_connection}")
        return ws_client, sql_connection
