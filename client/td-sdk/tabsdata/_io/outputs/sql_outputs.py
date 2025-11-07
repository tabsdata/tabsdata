#
# Copyright 2025 Tabs Data Inc.
#

from __future__ import annotations

import logging
import os
import uuid
from typing import TYPE_CHECKING, List
from urllib.parse import urlparse

import polars as pl

from tabsdata._credentials import (
    UserPasswordCredentials,
    build_credentials,
)
from tabsdata._io.constants import (
    MARIADB_SCHEME,
    MYSQL_SCHEME,
    ORACLE_SCHEME,
    POSTGRES_SCHEMES,
)
from tabsdata._io.outputs.shared_enums import (
    IfTableExistsStrategy,
    IfTableExistStrategySpec,
)
from tabsdata._io.plugin import DestinationPlugin
from tabsdata._tabsserver.function.sql_utils import add_mariadb_collation
from tabsdata._utils.sql_utils import add_driver_to_uri, obtain_uri
from tabsdata.exceptions import (
    DestinationConfigurationError,
    ErrorCode,
)

if TYPE_CHECKING:
    import sqlalchemy

logger = logging.getLogger(__name__)
logging.getLogger("sqlalchemy").setLevel(logging.ERROR)
logging.getLogger("urllib3").setLevel(logging.ERROR)


class MariaDBDestination(DestinationPlugin):
    """
    Class for managing the configuration of MariaDB-based data outputs.

    Attributes:
        credentials (UserPasswordCredentials): The credentials required to access the
            MariaDB database.
        destination_table (str | List[str]): The table(s) to create. If multiple tables
            are provided, they must be provided as a list.
        if_table_exists ({'append', 'replace'}): The strategy to
            follow when the table already exists.
            - ‘replace’ will create a new database table, overwriting an existing one.
            - ‘append’ will append to an existing table.
        uri (str): The URI of the database where the data is going to be stored.
    """

    def __init__(
        self,
        uri: str,
        destination_table: List[str] | str,
        credentials: UserPasswordCredentials = None,
        if_table_exists: IfTableExistStrategySpec = "append",
    ):
        """
        Initializes the MariaDBDestination with the given URI and destination table,
        and optionally connection credentials.

        Args:
            uri (str): The URI of the database where the data is going to be stored.
            destination_table (List[str] | str): The tables to create. If multiple
                tables are provided, they must be provided as a list.
            credentials (UserPasswordCredentials, optional): The credentials
                required to access the MariaDB database. Must be a
                UserPasswordCredentials object.
            if_table_exists ({'append', 'replace'}, optional): The strategy to
                follow when the table already exists. Defaults to 'append'.
                - ‘replace’ will create a new database table, overwriting an existing
                one.
                - ‘append’ will append to an existing table.

        Raises:
            OutputConfigurationError
        """
        self.credentials = credentials
        self.uri = uri
        self.destination_table = destination_table
        self.if_table_exists = if_table_exists

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
                - ‘replace’ will create a new database table, overwriting an existing
                one.
                - ‘append’ will append to an existing table.
        """
        valid_values = [
            IfTableExistsStrategy.APPEND.value,
            IfTableExistsStrategy.REPLACE.value,
        ]
        if if_table_exists not in valid_values:
            raise DestinationConfigurationError(
                ErrorCode.DECE26, valid_values, if_table_exists
            )
        self._if_table_exists = if_table_exists

    @property
    def uri(self) -> str:
        """
        str: The URI of the database where the data is going to be stored.
        """
        return self._uri

    @uri.setter
    def uri(self, uri: str):
        """
        Sets the URI of the database where the data is going to be stored.

        Args:
            uri (str): The URI of the database where the data is going to be stored.
        """
        self._uri = uri
        self._parsed_uri = urlparse(uri)
        if not self._parsed_uri.scheme.lower().startswith(MARIADB_SCHEME):
            raise DestinationConfigurationError(
                ErrorCode.DECE2, self._parsed_uri.scheme, MARIADB_SCHEME, self.uri
            )

    @property
    def destination_table(self) -> str | List[str]:
        """
        str | List[str]: The table(s) to create. If multiple tables are provided,
            they must be provided as a list.
        """
        return self._destination_table

    @destination_table.setter
    def destination_table(self, destination_table: List[str] | str):
        """
        Sets the table(s) to create.

        Args:
            destination_table (List[str] | str): The table(s) to create. If multiple
                tables are provided, they must be provided as a list.
        """
        if isinstance(destination_table, (list, str)):
            self._destination_table = destination_table
        else:
            raise DestinationConfigurationError(
                ErrorCode.DECE22, type(destination_table)
            )

    @property
    def credentials(self) -> UserPasswordCredentials:
        """
        UserPasswordCredentials: The credentials required to access the MariaDB
            database.
        """
        return self._credentials

    @credentials.setter
    def credentials(self, credentials: UserPasswordCredentials | None):
        """
        Sets the credentials to access the MariaDB database.

        Args:
            credentials (UserPasswordCredentials | None): The credentials
                required to access the MariaDB database. Can be a
                UserPasswordCredentials object or None.
        """
        if not credentials:
            self._credentials = None
        else:
            credentials = build_credentials(credentials)
            if not (isinstance(credentials, UserPasswordCredentials)):
                raise DestinationConfigurationError(ErrorCode.DECE23, type(credentials))
            self._credentials = credentials

    def __repr__(self) -> str:
        return f"{self.__class__.__name__}({self.uri})"

    def chunk(
        self, working_dir: str, *results: pl.LazyFrame | None
    ) -> list[str | None]:
        """
        Store the results in the SQL destination.

        Args:
            working_dir (str): The working directory where the results will be stored.
            results (list[pl.LazyFrame | None]): The results to store in the SQL
                destination.
        """
        logger.debug(f"Beginning chunking process for SQL destination {self}")
        logger.debug(f"Results to store: {results}")
        intermediate_files = _chunk_to_intermediate_files(
            self.destination_table, results, working_dir
        )
        logger.debug(f"Intermediate files created: {intermediate_files}")
        return intermediate_files

    def write(self, files: list[str | None]):
        logger.debug(f"Writing results to SQL destination {self}")
        logger.debug(f"Files to write: {files}")
        _store_results_in_sql(self, files)
        logger.debug("Results written to SQL destination successfully")


class MySQLDestination(DestinationPlugin):
    """
    Class for managing the configuration of MySQL-based data outputs.

    Attributes:
        credentials (UserPasswordCredentials): The credentials required to access the
            MySQL database.
        destination_table (str | List[str]): The table(s) to create. If multiple tables
            are provided, they must be provided as a list.
        if_table_exists ({'append', 'replace'}): The strategy to
            follow when the table already exists.
            - ‘replace’ will create a new database table, overwriting an existing one.
            - ‘append’ will append to an existing table.
        uri (str): The URI of the database where the data is going to be stored.
    """

    def __init__(
        self,
        uri: str,
        destination_table: List[str] | str,
        credentials: UserPasswordCredentials = None,
        if_table_exists: IfTableExistStrategySpec = "append",
    ):
        """
        Initializes the MySQLDestination with the given URI and destination table,
        and optionally connection credentials.

        Args:
            uri (str): The URI of the database where the data is going to be stored.
            destination_table (List[str] | str): The tables to create. If multiple
                tables are provided, they must be provided as a list.
            credentials (UserPasswordCredentials, optional): The credentials
                required to access the MySQL database. Must be a
                UserPasswordCredentials object.

        Raises:
            OutputConfigurationError
        """
        self.credentials = credentials
        self.uri = uri
        self.destination_table = destination_table
        self.if_table_exists = if_table_exists

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
                - ‘replace’ will create a new database table, overwriting an existing
                one.
                - ‘append’ will append to an existing table.
        """
        valid_values = [
            IfTableExistsStrategy.APPEND.value,
            IfTableExistsStrategy.REPLACE.value,
        ]
        if if_table_exists not in valid_values:
            raise DestinationConfigurationError(
                ErrorCode.DECE27, valid_values, if_table_exists
            )
        self._if_table_exists = if_table_exists

    @property
    def uri(self) -> str:
        """
        str: The URI of the database where the data is going to be stored.
        """
        return self._uri

    @uri.setter
    def uri(self, uri: str):
        """
        Sets the URI of the database where the data is going to be stored.

        Args:
            uri (str): The URI of the database where the data is going to be stored.
        """
        self._uri = uri
        self._parsed_uri = urlparse(uri)
        if not self._parsed_uri.scheme.lower().startswith(MYSQL_SCHEME):
            raise DestinationConfigurationError(
                ErrorCode.DECE2, self._parsed_uri.scheme, MYSQL_SCHEME, self.uri
            )

    @property
    def destination_table(self) -> str | List[str]:
        """
        str | List[str]: The table(s) to create. If multiple tables are provided,
            they must be provided as a list.
        """
        return self._destination_table

    @destination_table.setter
    def destination_table(self, destination_table: List[str] | str):
        """
        Sets the table(s) to create.

        Args:
            destination_table (List[str] | str): The table(s) to create. If multiple
                tables are provided, they must be provided as a list.
        """
        if isinstance(destination_table, (list, str)):
            self._destination_table = destination_table
        else:
            raise DestinationConfigurationError(
                ErrorCode.DECE8, type(destination_table)
            )

    @property
    def credentials(self) -> UserPasswordCredentials:
        """
        UserPasswordCredentials: The credentials required to access the MySQLDatabase.
        """
        return self._credentials

    @credentials.setter
    def credentials(self, credentials: UserPasswordCredentials | None):
        """
        Sets the credentials to access the MySQLDatabase.

        Args:
            credentials (UserPasswordCredentials | None): The credentials
                required to access the MySQLDatabase. Can be a
                UserPasswordCredentials object or None.
        """
        if not credentials:
            self._credentials = None
        else:
            credentials = build_credentials(credentials)
            if not (isinstance(credentials, UserPasswordCredentials)):
                raise DestinationConfigurationError(ErrorCode.DECE9, type(credentials))
            self._credentials = credentials

    def __repr__(self) -> str:
        return f"{self.__class__.__name__}({self.uri})"

    def chunk(
        self, working_dir: str, *results: pl.LazyFrame | None
    ) -> list[str | None]:
        """
        Store the results in the SQL destination.

        Args:
            working_dir (str): The working directory where the results will be stored.
            results (list[pl.LazyFrame | None]): The results to store in the SQL
                destination.
        """
        logger.debug(f"Beginning chunking process for SQL destination {self}")
        logger.debug(f"Results to store: {results}")
        intermediate_files = _chunk_to_intermediate_files(
            self.destination_table, results, working_dir
        )
        logger.debug(f"Intermediate files created: {intermediate_files}")
        return intermediate_files

    def write(self, files: list[str | None]):
        logger.debug(f"Writing results to SQL destination {self}")
        logger.debug(f"Files to write: {files}")
        _store_results_in_sql(self, files)
        logger.debug("Results written to SQL destination successfully")


class OracleDestination(DestinationPlugin):
    """
    Class for managing the configuration of Oracle-based data outputs.

    Attributes:
        credentials (UserPasswordCredentials): The credentials required to access the
            Oracle database.
        destination_table (str | List[str]): The table(s) to create. If multiple tables
            are provided, they must be provided as a list.
        if_table_exists ({'append', 'replace'}): The strategy to
            follow when the table already exists.
            - ‘replace’ will create a new database table, overwriting an existing one.
            - ‘append’ will append to an existing table.
        uri (str): The URI of the database where the data is going to be stored.
    """

    def __init__(
        self,
        uri: str,
        destination_table: List[str] | str,
        credentials: UserPasswordCredentials = None,
        if_table_exists: IfTableExistStrategySpec = "append",
    ):
        """
        Initializes the OracleDestination with the given URI and destination table,
        and optionally connection credentials.

        Args:
            uri (str): The URI of the database where the data is going to be stored.
            destination_table (List[str] | str): The tables to create. If multiple
                tables are provided, they must be provided as a list.
            credentials (UserPasswordCredentials, optional): The credentials
                required to access the Oracle database. Must be a
                UserPasswordCredentials object.

        Raises:
            OutputConfigurationError
        """
        self.credentials = credentials
        self.uri = uri
        self.destination_table = destination_table
        self.if_table_exists = if_table_exists

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
                - ‘replace’ will create a new database table, overwriting an existing
                one.
                - ‘append’ will append to an existing table.
        """
        valid_values = [
            IfTableExistsStrategy.APPEND.value,
            IfTableExistsStrategy.REPLACE.value,
        ]
        if if_table_exists not in valid_values:
            raise DestinationConfigurationError(
                ErrorCode.DECE28, valid_values, if_table_exists
            )
        self._if_table_exists = if_table_exists

    @property
    def uri(self) -> str:
        """
        str: The URI of the database where the data is going to be stored.
        """
        return self._uri

    @uri.setter
    def uri(self, uri: str):
        """
        Sets the URI of the database where the data is going to be stored.

        Args:
            uri (str): The URI of the database where the data is going to be stored.
        """
        self._uri = uri
        self._parsed_uri = urlparse(uri)
        if not self._parsed_uri.scheme.lower().startswith(ORACLE_SCHEME):
            raise DestinationConfigurationError(
                ErrorCode.DECE2, self._parsed_uri.scheme, ORACLE_SCHEME, self.uri
            )

    @property
    def destination_table(self) -> str | List[str]:
        """
        str | List[str]: The table(s) to create. If multiple tables are provided,
            they must be provided as a list.
        """
        return self._destination_table

    @destination_table.setter
    def destination_table(self, destination_table: List[str] | str):
        """
        Sets the table(s) to create.

        Args:
            destination_table (List[str] | str): The table(s) to create. If multiple
                tables are provided, they must be provided as a list.
        """
        if isinstance(destination_table, (list, str)):
            self._destination_table = destination_table
        else:
            raise DestinationConfigurationError(
                ErrorCode.DECE24, type(destination_table)
            )

    @property
    def credentials(self) -> UserPasswordCredentials:
        """
        UserPasswordCredentials: The credentials required to access the Oracle
            database.
        """
        return self._credentials

    @credentials.setter
    def credentials(self, credentials: UserPasswordCredentials | None):
        """
        Sets the credentials to access the Oracle database.

        Args:
            credentials (UserPasswordCredentials | None): The credentials
                required to access the Oracle database. Can be a
                UserPasswordCredentials object or None.
        """
        if not credentials:
            self._credentials = None
        else:
            credentials = build_credentials(credentials)
            if not (isinstance(credentials, UserPasswordCredentials)):
                raise DestinationConfigurationError(ErrorCode.DECE25, type(credentials))
            self._credentials = credentials

    def __repr__(self) -> str:
        return f"{self.__class__.__name__}({self.uri})"

    def chunk(
        self, working_dir: str, *results: pl.LazyFrame | None
    ) -> list[str | None]:
        """
        Store the results in the SQL destination.

        Args:
            working_dir (str): The working directory where the results will be stored.
            results (list[pl.LazyFrame | None]): The results to store in the SQL
                destination.
        """
        logger.debug(f"Beginning chunking process for SQL destination {self}")
        logger.debug(f"Results to store: {results}")
        intermediate_files = _chunk_to_intermediate_files(
            self.destination_table, results, working_dir
        )
        logger.debug(f"Intermediate files created: {intermediate_files}")
        return intermediate_files

    def write(self, files: list[str | None]):
        logger.debug(f"Writing results to SQL destination {self}")
        logger.debug(f"Files to write: {files}")
        _store_results_in_sql(self, files)
        logger.debug("Results written to SQL destination successfully")


class PostgresDestination(DestinationPlugin):
    """
    Class for managing the configuration of Postgres-based data outputs.

    Attributes:
        credentials (UserPasswordCredentials): The credentials required to access the
            Postgres database.
        destination_table (str | List[str]): The table(s) to create. If multiple tables
            are provided, they must be provided as a list.
        if_table_exists ({'append', 'replace'}): The strategy to
            follow when the table already exists.
            - ‘replace’ will create a new database table, overwriting an existing one.
            - ‘append’ will append to an existing table.
        uri (str): The URI of the database where the data is going to be stored.
    """

    def __init__(
        self,
        uri: str,
        destination_table: List[str] | str,
        credentials: UserPasswordCredentials = None,
        if_table_exists: IfTableExistStrategySpec = "append",
    ):
        """
        Initializes the PostgresDestination with the given URI and destination table,
        and optionally connection credentials.

        Args:
            uri (str): The URI of the database where the data is going to be stored.
            destination_table (List[str] | str): The tables to create. If multiple
                tables are provided, they must be provided as a list.
            credentials (UserPasswordCredentials, optional): The credentials
                required to access the Postgres database. Must be a
                UserPasswordCredentials object.

        Raises:
            OutputConfigurationError
        """
        self.credentials = credentials
        self.uri = uri
        self.destination_table = destination_table
        self.if_table_exists = if_table_exists

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
                - ‘replace’ will create a new database table, overwriting an existing
                one.
                - ‘append’ will append to an existing table.
        """
        valid_values = [
            IfTableExistsStrategy.APPEND.value,
            IfTableExistsStrategy.REPLACE.value,
        ]
        if if_table_exists not in valid_values:
            raise DestinationConfigurationError(
                ErrorCode.DECE29, valid_values, if_table_exists
            )
        self._if_table_exists = if_table_exists

    @property
    def uri(self) -> str:
        """
        str: The URI of the database where the data is going to be stored.
        """
        return self._uri

    @uri.setter
    def uri(self, uri: str):
        """
        Sets the URI of the database where the data is going to be stored.

        Args:
            uri (str): The URI of the database where the data is going to be stored.
        """
        self._uri = uri
        self._parsed_uri = urlparse(uri)
        if not any(
            [
                self._parsed_uri.scheme.lower().startswith(scheme)
                for scheme in POSTGRES_SCHEMES
            ]
        ):
            raise DestinationConfigurationError(
                ErrorCode.DECE2, self._parsed_uri.scheme, POSTGRES_SCHEMES, self.uri
            )

    @property
    def destination_table(self) -> str | List[str]:
        """
        str | List[str]: The table(s) to create. If multiple tables are provided,
            they must be provided as a list.
        """
        return self._destination_table

    @destination_table.setter
    def destination_table(self, destination_table: List[str] | str):
        """
        Sets the table(s) to create.

        Args:
            destination_table (List[str] | str): The table(s) to create. If multiple
                tables are provided, they must be provided as a list.
        """
        if isinstance(destination_table, (list, str)):
            self._destination_table = destination_table
        else:
            raise DestinationConfigurationError(
                ErrorCode.DECE20, type(destination_table)
            )

    @property
    def credentials(self) -> UserPasswordCredentials:
        """
        UserPasswordCredentials: The credentials required to access the
            Postgres database.
        """
        return self._credentials

    @credentials.setter
    def credentials(self, credentials: UserPasswordCredentials | None):
        """
        Sets the credentials to access the PostgresDatabase.

        Args:
            credentials (UserPasswordCredentials | None): The credentials
                required to access the PostgresDatabase. Can be a
                UserPasswordCredentials object or None.
        """
        if not credentials:
            self._credentials = None
        else:
            credentials = build_credentials(credentials)
            if not (isinstance(credentials, UserPasswordCredentials)):
                raise DestinationConfigurationError(ErrorCode.DECE21, type(credentials))
            self._credentials = credentials

    def __repr__(self) -> str:
        return f"{self.__class__.__name__}({self.uri})"

    def chunk(
        self, working_dir: str, *results: pl.LazyFrame | None
    ) -> list[str | None]:
        """
        Store the results in the SQL destination.

        Args:
            working_dir (str): The working directory where the results will be stored.
            results (list[pl.LazyFrame | None]): The results to store in the SQL
                destination.
        """
        logger.debug(f"Beginning chunking process for SQL destination {self}")
        logger.debug(f"Results to store: {results}")
        intermediate_files = _chunk_to_intermediate_files(
            self.destination_table, results, working_dir
        )
        logger.debug(f"Intermediate files created: {intermediate_files}")
        return intermediate_files

    def write(self, files: list[str | None]):
        logger.debug(f"Writing results to SQL destination {self}")
        logger.debug(f"Files to write: {files}")
        _store_results_in_sql(self, files)
        logger.debug("Results written to SQL destination successfully")


def _chunk_to_intermediate_files(
    destination_table_configuration: str | list[str],
    results: tuple[pl.LazyFrame | None],
    working_dir: str,
) -> list[str]:
    if isinstance(destination_table_configuration, str):
        destination_table_configuration = [destination_table_configuration]
    elif isinstance(destination_table_configuration, list):
        pass
    else:
        logger.error(
            "destination_table must be a string or a list of strings, "
            f"got {type(destination_table_configuration)} instead"
        )
        raise TypeError(
            "destination_table must be a string or a list of strings, "
            f"got {type(destination_table_configuration)} instead"
        )

    if len(results) != len(destination_table_configuration):
        logger.error(
            "The number of destination tables does not match the number of results."
        )
        logger.error(f"Destination tables: '{destination_table_configuration}'")
        logger.error(f"Number or results: {len(results)}")
        raise TypeError(
            "The number of destination tables does not match the number of results."
        )
    intermediate_files = []
    for destination_table, result in zip(destination_table_configuration, results):
        if result is None:
            logger.info("Result is None. No data stored in intermediate file.")
            intermediate_file_path = None
        elif isinstance(result, pl.LazyFrame):
            intermediate_file = (
                f"intermediate_{destination_table}_{uuid.uuid4()}.parquet"
            )
            intermediate_file_path = os.path.join(working_dir, intermediate_file)
            logger.debug(f"Writing intermediate file '{intermediate_file_path}'")
            result.sink_parquet(
                intermediate_file_path,
                maintain_order=True,
            )
            logger.debug("Intermediate file written successfully")
        else:
            logger.error(f"Incorrect result type: '{type(result)}'. No data stored.")
            raise TypeError(f"Incorrect result type: '{type(result)}'. No data stored.")
        intermediate_files.append(intermediate_file_path)
    return intermediate_files


def _store_results_in_sql(
    destination: (
        MariaDBDestination | MySQLDestination | OracleDestination | PostgresDestination
    ),
    intermediate_files: list[str | None],
):

    from sqlalchemy import create_engine

    logger.info(f"Storing results in SQL destination '{destination}'")
    if isinstance(
        destination,
        (MariaDBDestination, MySQLDestination, OracleDestination, PostgresDestination),
    ):
        uri = obtain_uri(destination, log=True, add_credentials=True)
        uri = add_driver_to_uri(uri, log=True)
        if isinstance(destination, MariaDBDestination):
            uri = add_mariadb_collation(uri)
        destination_table_configuration = destination.destination_table
        destination_if_table_exists = destination.if_table_exists
        engine = create_engine(uri)
        try:
            _create_session_and_store(
                engine,
                intermediate_files,
                destination_table_configuration,
                destination_if_table_exists,
            )
            logger.info("Results stored in SQL destination")
        except Exception:
            logger.error("Error storing results in SQL destination")
            raise
        finally:
            engine.dispose()
    else:
        logger.error(f"Storing results in destination '{destination}' not supported.")
        raise TypeError(
            f"Storing results in destination '{destination}' not supported."
        )


def _create_session_and_store(
    engine: sqlalchemy.engine.base.Engine,
    intermediate_files: list[str | None],
    destination_table_configuration: str | List[str],
    destination_if_table_exists: str,
):

    from sqlalchemy.orm import sessionmaker

    Session = sessionmaker(bind=engine)
    session = Session()
    with session.begin():
        if isinstance(destination_table_configuration, str):
            destination_table_configuration = [destination_table_configuration]

        for intermediate_file, destination_table in zip(
            intermediate_files, destination_table_configuration
        ):
            _store_result_in_sql_table(
                intermediate_file,
                session,
                destination_table,
                destination_if_table_exists,
            )


def _store_result_in_sql_table(
    intermediate_file: str | None,
    session: sqlalchemy.orm.Session,
    destination_table: str,
    if_table_exists: str,
):

    import pyarrow as pa
    import pyarrow.parquet as pq

    logger.info(f"Storing result in SQL table: {destination_table}")
    logger.debug(f"Intermediate file: {intermediate_file}")
    if intermediate_file is None:
        logger.info("Intermediate file is None. No data stored.")
        return
    elif isinstance(intermediate_file, str):
        pass
    else:
        logger.error(
            f"Incorrect intermediate file type: '{type(intermediate_file)}'. "
            "No data stored."
        )
        raise TypeError(
            f"Incorrect intermediate file type: '{type(intermediate_file)}'"
            ". No data stored."
        )
    logger.debug(f"Using strategy in case table exists: {if_table_exists}")
    chunk_size = 10000
    parquet_file = pq.ParquetFile(intermediate_file)
    for batch in parquet_file.iter_batches(batch_size=chunk_size):
        chunk_table = pa.Table.from_batches(batches=[batch])
        df = pl.from_arrow(chunk_table)
        logger.debug(f"Writing batch of shape {df.shape} to table {destination_table}")
        # Note: the warning below is due to the fact that if_table_exists must be one of
        # the following: "fail", "replace", "append". This is enforced by the
        # Output class, so we can safely ignore this warning.
        df.write_database(
            table_name=destination_table,
            connection=session,
            if_table_exists=if_table_exists,
        )
        if_table_exists = "append"
    logger.info(f"Result stored in SQL table: {destination_table}")


DRIVER_TYPE_AND_RECOMMENDATION_FOR_OUTPUT = {
    MySQLDestination: ("MySQL", "mysql-connector-python"),
    OracleDestination: ("Oracle", "oracledb"),
    PostgresDestination: ("Postgres", "psycopg2-binary"),
    MariaDBDestination: ("MariaDB", "mysql-connector-python"),
}


def verify_output_sql_drivers(output: DestinationPlugin):
    if isinstance(
        output,
        (MySQLDestination, OracleDestination, PostgresDestination, MariaDBDestination),
    ):

        from sqlalchemy import create_engine

        uri = obtain_uri(output, log=False, add_credentials=False)
        uri = add_driver_to_uri(uri, log=False)
        try:
            engine = create_engine(uri)
            engine.dispose()
        except Exception as e:
            driver_type, recommended_driver = DRIVER_TYPE_AND_RECOMMENDATION_FOR_OUTPUT[
                type(output)
            ]
            logger.warning("-" * 50)
            logger.warning(
                "The local Python environment does not have a suitable "
                f"{driver_type} driver installed. The function will likely "
                "fail to execute when running in the Tabsdata server."
            )
            logger.warning("")
            logger.warning("It is recommended to either:")
            logger.warning(
                f"  Install a {driver_type} driver in your local "
                "environment, for example: 'pip install "
                f"{recommended_driver}'; and then update the function by running "
                "'td fn update'."
            )
            logger.warning(
                "  Or create a custom requirements.yaml file for the "
                f"function and add a {driver_type} driver to it; and then "
                "update the function by running 'td fn update'."
            )
            logger.warning("")
            logger.warning(f"Original error: {e}")
            logger.warning("-" * 50)
    else:
        return
