#
# Copyright 2025 Tabs Data Inc.
#

from __future__ import annotations

import logging
import os
import uuid
from typing import TYPE_CHECKING

import polars as pl

from tabsdata._credentials import UserPasswordCredentials, build_credentials
from tabsdata._io.inputs.sql_inputs import (
    _replace_initial_values,
    _validate_initial_values_type,
)
from tabsdata._io.outputs.shared_enums import (
    IfTableExistsStrategy,
    IfTableExistStrategySpec,
)
from tabsdata._io.plugin import DestinationPlugin, SourcePlugin
from tabsdata._secret import Secret
from tabsdata._tabsserver.function.offset_utils import OffsetReturn
from tabsdata.exceptions import ErrorCode, SourceConfigurationError

if TYPE_CHECKING:
    import sqlalchemy

logger = logging.getLogger(__name__)
logger.setLevel(logging.DEBUG)


class MSSQLSource(SourcePlugin):
    """
    Source plugin for Microsoft SQL Server.
    """

    def __init__(
        self,
        connection_string: str,
        query: str | list[str],
        chunk_size: int = 50000,
        credentials: dict | UserPasswordCredentials | None = None,
        server: str | Secret = None,
        database: str | Secret = None,
        driver: str | Secret = None,
        initial_values: dict | None = None,
        **kwargs,
    ):
        """
        Initialize the MSSQLSource with a connection string and a query.

        Args:
            connection_string (str): The connection string to connect to the database.
            query (str | list[str]): The SQL query or queries to execute.
            chunk_size (int): The number of rows to fetch in each chunk.
                Defaults to 50000.
        """

        try:
            import pyodbc  # noqa: F401
        except ImportError:
            raise ImportError(
                "The 'tabsdata_mssql' package is missing some dependencies. You "
                "can get them by installing 'tabsdata['mssql']'"
            )

        self.connection_string = connection_string
        self.query = query
        self.chunk_size = chunk_size
        self.credentials = credentials
        self.server = server
        self.database = database
        self.driver = driver
        self.initial_values = initial_values
        self.kwargs = kwargs
        self._support_read_sql_query = self.kwargs.get("support_read_sql_query", {})
        self._support_to_parquet = self.kwargs.get("support_to_parquet", {})
        self._support_extra_connection_string_secrets = self.kwargs.get(
            "support_extra_connection_string_secrets", []
        )
        self._support_pyodbc_connect = self.kwargs.get("support_pyodbc_connect", {})

    @property
    def query(self) -> list[str]:
        """
        Get the SQL query or queries to execute.

        Returns:
            str | list[str]: The SQL query or queries.
        """
        return self._query

    @query.setter
    def query(self, value: str | list[str]):
        """
        Set the SQL query or queries to execute.

        Args:
            value (str | list[str]): The SQL query or queries.
        """
        if isinstance(value, str):
            self._query = [value]
        elif isinstance(value, list) and all(isinstance(q, str) for q in value):
            self._query = value
        else:
            raise TypeError(
                "The 'query' parameter must be a string or a list of "
                f"strings, got a parameter of type '{type(value)}' "
                "instead."
            )

    @property
    def connection_string(self) -> str:
        """
        Get the connection string for the database.

        Returns:
            str: The connection string.
        """
        return _obtain_connection_string(self)

    @connection_string.setter
    def connection_string(self, value: str):
        """
        Set the connection string for the database.

        Args:
            value (str): The connection string.
        """
        if not isinstance(value, str):
            raise TypeError(
                "The 'connection_string' parameter must be a string, got "
                f"a parameter of type '{type(value)}' instead."
            )
        self._connection_string = value

    @property
    def credentials(self) -> UserPasswordCredentials | None:
        """
        UserPasswordCredentials | None: The credentials required to access
            Microsoft SQL Server. If no credentials were provided, it will return None.
        """
        return self._credentials

    @credentials.setter
    def credentials(self, credentials: dict | UserPasswordCredentials | None):
        """
        Sets the credentials to access Microsoft SQL Server.

        Args:
            credentials (dict | UserPasswordCredentials | None): The credentials
                required to access Microsoft SQL Server. Can be a
                UserPasswordCredentials object, a dictionary or None
        """
        if not credentials:
            self._credentials = None
        else:
            credentials = build_credentials(credentials)
            if not (isinstance(credentials, UserPasswordCredentials)):
                raise SourceConfigurationError(ErrorCode.SOCE42, type(credentials))
            self._credentials = credentials

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

    def chunk(self, working_dir: str):
        """
        Execute the query and yield chunks of data.

        Args:
            working_dir (str): The working directory for temporary files.

        Yields:
            pd.DataFrame: A chunk of data from the query result.
        """
        logger.debug("Connecting to Microsoft SQL Server")
        from sqlalchemy import create_engine
        from sqlalchemy.engine import URL

        engine = None
        try:
            connection_url = URL.create(
                "mssql+pyodbc", query={"odbc_connect": self.connection_string}
            )
            engine = create_engine(connection_url)
            resulting_files = []
            logger.debug(f"Executing queries '{self.query}'")
            for query in self.query:
                result = self._execute_single_query(engine, query, working_dir)
                resulting_files.append(result)
        finally:
            if engine is not None:
                engine.dispose()
                logger.debug("Connection to Microsoft SQL Server closed")
        logger.debug(
            f"Queries executed successfully, results saved to '{resulting_files}' in "
            f"directory '{working_dir}'"
        )
        return resulting_files

    def _execute_single_query(self, engine, query: str, working_dir: str) -> str:
        """
        Execute a single SQL query and return the result as a parquet file.

        Args:
            query (str): The SQL query to execute.

        Returns:
            str: The path to the resulting parquet file.
        """
        import pandas as pd

        logger.debug(f"Executing query '{query}'")
        if initial_values := self.initial_values:
            query = _replace_initial_values(query, initial_values)
        first_chunk = True
        uuid_string = uuid.uuid4().hex[:16]
        intermediate_file_name = f"intermediate_{uuid_string}.parquet"
        intermediate_file = os.path.join(working_dir, intermediate_file_name)
        for chunk in pd.read_sql_query(
            query, engine, chunksize=self.chunk_size, **self._support_read_sql_query
        ):
            chunk.to_parquet(
                intermediate_file,
                engine="fastparquet",
                index=False,
                append=(not first_chunk),
                **self._support_to_parquet,
            )
            first_chunk = False
        logger.debug(
            f"Query executed successfully, result saved to {intermediate_file_name}"
        )
        return intermediate_file

    def __repr__(self):
        """
        Return a string representation of the MSSQLSource instance.

        Returns:
            str: A string representation of the instance.
        """
        return (
            f"{self.__class__.__name__}(connection_string='{self._connection_string}', "
            f"query={self.query})"
        )


class MSSQLDestination(DestinationPlugin):
    """
    Destination plugin for Microsoft SQL Server.
    """

    def __init__(
        self,
        connection_string: str,
        destination_table: str | list[str],
        credentials: dict | UserPasswordCredentials | None = None,
        server: str | Secret = None,
        database: str | Secret = None,
        driver: str | Secret = None,
        if_table_exists: IfTableExistStrategySpec = "append",
        chunk_size: int = 50000,
        **kwargs,
    ):
        """
        Initialize the MSSQLDestination with a connection string and a table name.

        Args:
            connection_string (str): The connection string to connect to the database.
            destination_table (str | List[str]): The table(s) to create.
                If multiple tables are provided, they must be provided as a list.
            if_table_exists ({'append', 'replace'}): The strategy to
                follow when the table already exists.
                - ‘replace’ will create a new database table, overwriting an
                    existing one.
                - ‘append’ will append to an existing table.
        """

        try:
            import fastparquet  # noqa: F401
            import pyodbc  # noqa: F401
        except ImportError:
            raise ImportError(
                "The 'tabsdata_mssql' package is missing some dependencies. You "
                "can get them by installing 'tabsdata['mssql']'"
            )

        self.connection_string = connection_string
        self.destination_table = destination_table
        self.credentials = credentials
        self.server = server
        self.database = database
        self.driver = driver
        self.if_table_exists = if_table_exists
        self.kwargs = kwargs
        self.chunk_size = chunk_size
        self._support_extra_connection_string_secrets = self.kwargs.get(
            "support_extra_connection_string_secrets", []
        )
        self._support_pyodbc_connect = self.kwargs.get("support_pyodbc_connect", {})

    @property
    def destination_table(self) -> list[str]:
        """
        Get the destination table(s) where the data will be stored.

        Returns:
            list[str]: The destination table(s).
        """
        return self._destination_table

    @destination_table.setter
    def destination_table(self, value: str | list[str]):
        """
        Set the destination table(s) where the data will be stored.

        Args:
            value (str | list[str]): The destination table(s).
        """
        if isinstance(value, str):
            self._destination_table = [value]
        elif isinstance(value, list) and all(isinstance(q, str) for q in value):
            self._destination_table = value
        else:
            raise TypeError(
                "The 'destination_table' parameter must be a string or a list of "
                f"strings, got a parameter '{value}' of type '{type(value)}' "
                "instead."
            )

    @property
    def connection_string(self) -> str:
        """
        Get the connection string for the database.

        Returns:
            str: The connection string.
        """
        return _obtain_connection_string(self)

    @connection_string.setter
    def connection_string(self, value: str):
        """
        Set the connection string for the database.

        Args:
            value (str): The connection string.
        """
        if not isinstance(value, str):
            raise TypeError(
                "The 'connection_string' parameter must be a string, got "
                f"a parameter of type '{type(value)}' instead."
            )
        self._connection_string = value

    @property
    def credentials(self) -> UserPasswordCredentials | None:
        """
        UserPasswordCredentials | None: The credentials required to access
            Microsoft SQL Server. If no credentials were provided, it will return None.
        """
        return self._credentials

    @credentials.setter
    def credentials(self, credentials: dict | UserPasswordCredentials | None):
        """
        Sets the credentials to access Microsoft SQL Server.

        Args:
            credentials (dict | UserPasswordCredentials | None): The credentials
                required to access Microsoft SQL Server. Can be a
                UserPasswordCredentials object, a dictionary or None
        """
        if not credentials:
            self._credentials = None
        else:
            credentials = build_credentials(credentials)
            if not (isinstance(credentials, UserPasswordCredentials)):
                raise SourceConfigurationError(ErrorCode.DECE49, type(credentials))
            self._credentials = credentials

    @property
    def if_table_exists(self) -> IfTableExistStrategySpec:
        """
        Returns the value of the if_table_exists property.
        This property determines what to do if the table already exists.
        """
        return self._if_table_exists

    @if_table_exists.setter
    def if_table_exists(self, value: IfTableExistStrategySpec):
        valid_values = [
            IfTableExistsStrategy.APPEND.value,
            IfTableExistsStrategy.REPLACE.value,
        ]
        if not isinstance(value, str):
            raise TypeError(
                "The 'if_table_exists' parameter must be a string, got a parameter of "
                f"type '{type(value)}' instead."
            )
        if value not in valid_values:
            raise ValueError(
                f"The 'if_table_exists' parameter must one of '{valid_values}', "
                f"got '{value}' instead."
            )
        self._if_table_exists = value

    def stream(
        self,
        working_dir: str,
        *results: list[pl.LazyFrame | None] | pl.LazyFrame | None,
    ):
        if len(results) != len(self.destination_table):
            raise ValueError(
                f"The number of results ({len(results)}) does not match the number of "
                f"tables provided ('{self.destination_table}', for a total "
                f"of {len(self.destination_table)} tables). Please make "
                "sure that the number of results matches the number of tables."
            )
        # Chunk the results
        logger.info("Chunking the results.")
        files = self.chunk(working_dir, *results)
        logger.info("Writing the results to Microsoft SQL Server.")
        self.write(files)
        logger.info("All results written successfully.")

    def chunk(
        self, working_dir: str, *results: pl.LazyFrame | None
    ) -> list[None | str]:
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
                logger.debug(f"Sinking the data to {intermediate_destination_file}")
                result.sink_parquet(intermediate_destination_file)
                logger.debug(
                    f"File {intermediate_destination_file} generated successfully"
                )
                list_of_files.append(intermediate_destination_file)
            logger.debug(f"Chunked result in position {index} successfully")
        logger.info("All results chunked successfully")
        return list_of_files

    def write(self, files):
        """
        This method is used to write the files to the database. It is called
        from the stream method, and it is not intended to be called directly.
        """

        logger.debug("Writting to Microsoft SQL Server")
        from sqlalchemy import create_engine
        from sqlalchemy.engine import URL

        engine = None
        try:
            connection_url = URL.create(
                "mssql+pyodbc", query={"odbc_connect": self.connection_string}
            )
            engine = create_engine(connection_url)
            from sqlalchemy.orm import sessionmaker

            Session = sessionmaker(bind=engine)
            session = Session()
            with session.begin():
                for file_path, table in zip(files, self.destination_table):
                    if file_path is None:
                        logger.warning(
                            f"Received None for table '{table}'. No data loaded."
                        )
                    else:
                        logger.info(f"Storing file '{file_path}' in table '{table}'")
                        try:
                            self._upload_single_file_to_table(session, file_path, table)
                            logger.info(
                                f"Uploaded results to table '{table}' successfully."
                            )
                        except Exception:
                            logger.error(
                                f"Failed to upload results from file '{file_path}' to"
                                f" table '{table}'"
                            )
                            raise
                logger.debug(
                    "All files uploaded successfully to their respective tables."
                )
        finally:
            if engine is not None:
                engine.dispose()
                logger.debug("Connection to Microsoft SQL Server closed")
        logger.debug("All files written successfully to Microsoft SQL Server")

    def _upload_single_file_to_table(
        self, session: sqlalchemy.orm.Session, file_path: str, table: str
    ):

        import pyarrow as pa
        import pyarrow.parquet as pq

        logger.debug(f"Using strategy in case table exists '{self.if_table_exists}'")
        chunk_size = self.chunk_size
        parquet_file = pq.ParquetFile(file_path)
        if_table_exists = self.if_table_exists
        for batch in parquet_file.iter_batches(batch_size=chunk_size):
            chunk_table = pa.Table.from_batches(batches=[batch])
            df = pl.from_arrow(chunk_table)
            logger.debug(f"Writing batch of shape {df.shape} to table {table}")
            # Note: the warning below is due to the fact that if_table_exists must be
            # one of the following: "fail", "replace", "append". This is enforced by the
            # Output class, so we can safely ignore this warning.
            df.write_database(
                table_name=table,
                connection=session,
                if_table_exists=if_table_exists,
            )
            if_table_exists = "append"
        logger.info(f"Result stored in Microsoft SQL server table '{table}'")

    def __repr__(self):
        """
        Return a string representation of the MSSQLSource instance.

        Returns:
            str: A string representation of the instance.
        """
        return (
            f"{self.__class__.__name__}(connection_string='{self._connection_string}', "
            f"destination_table={self.destination_table})"
        )


def _obtain_connection_string(
    mssql_plugin: MSSQLDestination | MSSQLSource,
) -> str:
    """
    Obtain the connection string for the database.

    Args:
        mssql_plugin (MSSQLDestination | MSSQLSource): The instance of the class.

    Returns:
        str: The connection string.
    """
    aux_str = mssql_plugin._connection_string
    if not aux_str.endswith(";"):
        aux_str += ";"
    if isinstance(mssql_plugin.credentials, UserPasswordCredentials):
        username = mssql_plugin.credentials.user
        password = mssql_plugin.credentials.password
        aux_str = _add_field_to_string("UID", username, aux_str)
        aux_str = _add_field_to_string("PWD", password, aux_str)
    aux_str = _add_field_to_string("SERVER", mssql_plugin.server, aux_str)
    aux_str = _add_field_to_string("Database", mssql_plugin.database, aux_str)
    aux_str = _add_field_to_string("DRIVER", mssql_plugin.driver, aux_str)
    for field_name, value in mssql_plugin._support_extra_connection_string_secrets:
        aux_str = _add_field_to_string(field_name, value, aux_str)
    return aux_str


def _add_field_to_string(
    field_name: str, value: str | Secret, current_string: str
) -> str:
    """
    Add a field to the connection string.

    Args:
        field_name (str): The name of the field to add.
        value (str | Secret): The value of the field.
    """
    if not value:
        return current_string
    if isinstance(value, Secret):
        value = value.secret_value
    current_string += f"{field_name}={value};"
    return current_string
