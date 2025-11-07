#
# Copyright 2025 Tabs Data Inc.
#

import logging
import os
from datetime import datetime, timezone
from typing import List
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
from tabsdata._io.plugin import SourcePlugin
from tabsdata._tabsserver.function.offset_utils import OffsetReturn
from tabsdata._utils.sql_utils import obtain_uri
from tabsdata.exceptions import (
    ErrorCode,
    SourceConfigurationError,
)

logger = logging.getLogger(__name__)


def _validate_initial_values_type(initial_values: dict):
    """
    Validates the initial values provided for the SQL queries.

    Args:
        initial_values (dict): The initial values for the parameters in the SQL queries.

    Raises:
        InputConfigurationError: If the initial values are not valid.
    """
    for key, value in initial_values.items():
        if not isinstance(key, str):
            raise SourceConfigurationError(ErrorCode.SOCE40, type(key))


class MariaDBSource(SourcePlugin):
    """
    Class for managing the configuration of MariaDB-based data inputs.

    Attributes:
        credentials (UserPasswordCredentials): The credentials required to access the
            MariaDB database.
        initial_values (dict): The initial values for the parameters in the SQL queries.
        query (str | List[str]): The SQL query(s) to execute. If multiple queries are
            provided, they must be provided as a dictionary, with the parameter name in
            the registered function as the key and the SQL query as the value.
        uri (str): The URI of the database where the data is located.

    """

    def __init__(
        self,
        uri: str,
        query: str | List[str],
        credentials: UserPasswordCredentials | None = None,
        initial_values: dict | None = None,
    ):
        """
        Initializes the MariaDBSource with the given URI and query, and optionally
            connection credentials and initial values for the parameters in the SQL
            queries.

        Args:
            uri (str): The URI of the database where the data is located
            query (str | List[str]): The SQL query(s) to execute. If multiple queries
                are provided, they must be provided as a list, and they will be
                mapped to the function inputs in the same order as they are defined.
            credentials (UserPasswordCredentials, optional): The credentials
                required to access the MariaDB database. Must be a
                UserPasswordCredentials object.
            initial_values (dict, optional): The initial values for the parameters in
                the SQL queries.

        Raises:
            InputConfigurationError
        """
        self.credentials = credentials
        self.uri = uri
        self.query = query
        self.initial_values = initial_values

    @property
    def uri(self) -> str:
        """
        str: The URI of the database where the data is located.
        """
        return self._uri

    @uri.setter
    def uri(self, uri: str):
        """
        Sets the URI of the database where the data is located.

        Args:
            uri (str): The URI of the database where the data is located.
        """
        self._uri = uri
        self._parsed_uri = urlparse(uri)
        if self._parsed_uri.scheme.lower() != MARIADB_SCHEME:
            raise SourceConfigurationError(
                ErrorCode.SOCE2, self._parsed_uri.scheme, MARIADB_SCHEME, self.uri
            )

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
    def query(self) -> str | List[str]:
        """
        str | List[str]: The SQL query(s) to execute.
        """
        return self._query

    @query.setter
    def query(self, query: str | List[str]):
        """
        Sets the SQL query(s) to execute

        Args:
            query (str | List[str]): The SQL query(s) to execute. If multiple queries
                are provided, they must be provided as a list, and they will be
                mapped to the function inputs in the same order as they are defined.
        """
        if isinstance(query, str):
            self._query = query
        elif isinstance(query, list):
            self._query = query
            if not all(isinstance(single_query, str) for single_query in self._query):
                raise SourceConfigurationError(ErrorCode.SOCE35, type(query))
        else:
            raise SourceConfigurationError(ErrorCode.SOCE35, type(query))

    @property
    def credentials(self) -> UserPasswordCredentials | None:
        """
        UserPasswordCredentials | None: The credentials required to access
            MariaDB. If no credentials were provided, it will return None.
        """
        return self._credentials

    @credentials.setter
    def credentials(self, credentials: UserPasswordCredentials | None):
        """
        Sets the credentials to access MariaDB.

        Args:
            credentials (UserPasswordCredentials | None): The credentials
                required to access MariaDB. Can be a UserPasswordCredentials
                object or None
        """
        if not credentials:
            self._credentials = None
        else:
            credentials = build_credentials(credentials)
            if not (isinstance(credentials, UserPasswordCredentials)):
                raise SourceConfigurationError(ErrorCode.SOCE36, type(credentials))
            self._credentials = credentials

    @property
    def _offset_return(self) -> str:
        """
        Indicates whether the offset is returned by modifying the
        'initial_values' attribute of the plugin, or if it is part
        of the function return"""
        return OffsetReturn.FUNCTION.value

    def chunk(self, working_dir: str) -> list[str]:
        logger.debug(f"Triggering {self}")
        local_sources = _execute_sql_importer(self, working_dir)
        logger.debug(f"Obtained local sources: '{local_sources}'")
        return local_sources

    def __repr__(self) -> str:
        """
        Returns a string representation of the input.

        Returns:
            str: A string representation of the input.
        """
        return f"{self.__class__.__name__}(uri={self.uri}, query={self.query})"


class MySQLSource(SourcePlugin):
    """
    Class for managing the configuration of MySQL-based data inputs.

    Attributes:
        credentials (UserPasswordCredentials): The credentials required to access the
            MySQL database.
        initial_values (dict): The initial values for the parameters in the SQL queries.
        query (str | List[str]): The SQL query(s) to execute. If multiple queries are
            provided, they must be provided as a dictionary, with the parameter name in
            the registered function as the key and the SQL query as the value.
        uri (str): The URI of the database where the data is located.

    """

    def __init__(
        self,
        uri: str,
        query: str | List[str],
        credentials: UserPasswordCredentials | None = None,
        initial_values: dict | None = None,
    ):
        """
        Initializes the MySQLSource with the given URI and query, and optionally
            connection credentials and initial values for the parameters in the SQL
            queries.

        Args:
            uri (str): The URI of the database where the data is located
            query (str | List[str]): The SQL query(s) to execute. If multiple queries
                are provided, they must be provided as a list, and they will be
                mapped to the function inputs in the same order as they are defined.
            credentials (UserPasswordCredentials, optional): The credentials
                required to access the MySQL database. Must be a
                UserPasswordCredentials object.
            initial_values (dict, optional): The initial values for the parameters in
                the SQL queries.

        Raises:
            InputConfigurationError
        """
        self.credentials = credentials
        self.uri = uri
        self.query = query
        self.initial_values = initial_values

    @property
    def uri(self) -> str:
        """
        str: The URI of the database where the data is located.
        """
        return self._uri

    @uri.setter
    def uri(self, uri: str):
        """
        Sets the URI of the database where the data is located.

        Args:
            uri (str): The URI of the database where the data is located.
        """
        self._uri = uri
        self._parsed_uri = urlparse(uri)
        if self._parsed_uri.scheme.lower() != MYSQL_SCHEME:
            raise SourceConfigurationError(
                ErrorCode.SOCE2, self._parsed_uri.scheme, MYSQL_SCHEME, self.uri
            )

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
            raise SourceConfigurationError(ErrorCode.SOCE12, type(initial_values))
        else:
            _validate_initial_values_type(initial_values)
            self._initial_values = initial_values

    @property
    def query(self) -> str | List[str]:
        """
        str | List[str]: The SQL query(s) to execute.
        """
        return self._query

    @query.setter
    def query(self, query: str | List[str]):
        """
        Sets the SQL query(s) to execute

        Args:
            query (str | List[str]): The SQL query(s) to execute. If multiple queries
                are provided, they must be provided as a list, and they will be
                mapped to the function inputs in the same order as they are defined.
        """
        if isinstance(query, str):
            self._query = query
        elif isinstance(query, list):
            self._query = query
            if not all(isinstance(single_query, str) for single_query in self._query):
                raise SourceConfigurationError(ErrorCode.SOCE19, type(query))
        else:
            raise SourceConfigurationError(ErrorCode.SOCE19, type(query))

    @property
    def credentials(self) -> UserPasswordCredentials | None:
        """
        UserPasswordCredentials | None: The credentials required to access the
            MySQLDatabase. If no credentials were provided, it will return None.
        """
        return self._credentials

    @credentials.setter
    def credentials(self, credentials: UserPasswordCredentials | None):
        """
        Sets the credentials to access the MySQLDatabase.

        Args:
            credentials (UserPasswordCredentials | None): The credentials
                required to access the MySQLDatabase. Can be a UserPasswordCredentials
                object or None
        """
        if not credentials:
            self._credentials = None
        else:
            credentials = build_credentials(credentials)
            if not (isinstance(credentials, UserPasswordCredentials)):
                raise SourceConfigurationError(ErrorCode.SOCE22, type(credentials))
            self._credentials = credentials

    @property
    def _offset_return(self) -> str:
        """
        Indicates whether the offset is returned by modifying the
        'initial_values' attribute of the plugin, or if it is part
        of the function return"""
        return OffsetReturn.FUNCTION.value

    def chunk(self, working_dir: str) -> list[str]:
        logger.debug(f"Triggering {self}")
        local_sources = _execute_sql_importer(self, working_dir)
        logger.debug(f"Obtained local sources: '{local_sources}'")
        return local_sources

    def __repr__(self) -> str:
        """
        Returns a string representation of the input.

        Returns:
            str: A string representation of the input.
        """
        return f"{self.__class__.__name__}(uri={self.uri}, query={self.query})"


class OracleSource(SourcePlugin):
    """
    Class for managing the configuration of Oracle-based data inputs.

    Attributes:
        credentials (UserPasswordCredentials): The credentials required to access the
            Oracle database.
        initial_values (dict): The initial values for the parameters in the SQL queries.
        query (str | List[str]): The SQL query(s) to execute. If multiple queries are
            provided, they must be provided as a dictionary, with the parameter name in
            the registered function as the key and the SQL query as the value.
        uri (str): The URI of the database where the data is located.

    """

    def __init__(
        self,
        uri: str,
        query: str | List[str],
        credentials: UserPasswordCredentials | None = None,
        initial_values: dict | None = None,
    ):
        """
        Initializes the OracleSource with the given URI and query, and optionally
            connection credentials and initial values for the parameters in the SQL
            queries.

        Args:
            uri (str): The URI of the database where the data is located
            query (str | List[str]): The SQL query(s) to execute. If multiple queries
                are provided, they must be provided as a list, and they will be
                mapped to the function inputs in the same order as they are defined.
            credentials (UserPasswordCredentials, optional): The credentials
                required to access the Oracle database. Must be a
                UserPasswordCredentials object.
            initial_values (dict, optional): The initial values for the parameters in
                the SQL queries.

        Raises:
            InputConfigurationError
        """
        self.credentials = credentials
        self.uri = uri
        self.query = query
        self.initial_values = initial_values

    @property
    def uri(self) -> str:
        """
        str: The URI of the database where the data is located.
        """
        return self._uri

    @uri.setter
    def uri(self, uri: str):
        """
        Sets the URI of the database where the data is located.

        Args:
            uri (str): The URI of the database where the data is located.
        """
        self._uri = uri
        self._parsed_uri = urlparse(uri)
        if self._parsed_uri.scheme.lower() != ORACLE_SCHEME:
            raise SourceConfigurationError(
                ErrorCode.SOCE2, self._parsed_uri.scheme, ORACLE_SCHEME, self.uri
            )

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
            raise SourceConfigurationError(ErrorCode.SOCE37, type(initial_values))
        else:
            _validate_initial_values_type(initial_values)
            self._initial_values = initial_values

    @property
    def query(self) -> str | List[str]:
        """
        str | List[str]: The SQL query(s) to execute.
        """
        return self._query

    @query.setter
    def query(self, query: str | List[str]):
        """
        Sets the SQL query(s) to execute

        Args:
            query (str | List[str]): The SQL query(s) to execute. If multiple queries
                are provided, they must be provided as a list, and they will be
                mapped to the function inputs in the same order as they are defined.
        """
        if isinstance(query, str):
            self._query = query
        elif isinstance(query, list):
            self._query = query
            if not all(isinstance(single_query, str) for single_query in self._query):
                raise SourceConfigurationError(ErrorCode.SOCE38, type(query))
        else:
            raise SourceConfigurationError(ErrorCode.SOCE38, type(query))

    @property
    def credentials(self) -> UserPasswordCredentials | None:
        """
        UserPasswordCredentials | None: The credentials required to access
            Oracle. If no credentials were provided, it will return None.
        """
        return self._credentials

    @credentials.setter
    def credentials(self, credentials: UserPasswordCredentials | None):
        """
        Sets the credentials to access Oracle.

        Args:
            credentials (UserPasswordCredentials | None): The credentials
                required to access Oracle. Can be a UserPasswordCredentials
                object or None
        """
        if not credentials:
            self._credentials = None
        else:
            credentials = build_credentials(credentials)
            if not (isinstance(credentials, UserPasswordCredentials)):
                raise SourceConfigurationError(ErrorCode.SOCE39, type(credentials))
            self._credentials = credentials

    @property
    def _offset_return(self) -> str:
        """
        Indicates whether the offset is returned by modifying the
        'initial_values' attribute of the plugin, or if it is part
        of the function return"""
        return OffsetReturn.FUNCTION.value

    def chunk(self, working_dir: str) -> list[str]:
        logger.debug(f"Triggering {self}")
        local_sources = _execute_sql_importer(self, working_dir)
        logger.debug(f"Obtained local sources: '{local_sources}'")
        return local_sources

    def __repr__(self) -> str:
        """
        Returns a string representation of the input.

        Returns:
            str: A string representation of the input.
        """
        return f"{self.__class__.__name__}(uri={self.uri}, query={self.query})"


class PostgresSource(SourcePlugin):
    """
    Class for managing the configuration of Postgres-based data inputs.

    Attributes:
        credentials (UserPasswordCredentials): The credentials required to access the
            Postgres database.
        initial_values (dict): The initial values for the parameters in the SQL queries.
        query (str | List[str]): The SQL query(s) to execute. If multiple queries are
            provided, they must be provided as a dictionary, with the parameter name in
            the registered function as the key and the SQL query as the value.
        uri (str): The URI of the database where the data is located.
    """

    def __init__(
        self,
        uri: str,
        query: str | List[str],
        credentials: UserPasswordCredentials | None = None,
        initial_values: dict | None = None,
    ):
        """
        Initializes the PostgresSource with the given URI and query, and optionally
            connection credentials and initial values for the parameters in the SQL
            queries.

        Args:
            uri (str): The URI of the database where the data is located
            query (str | List[str]): The SQL query(s) to execute. If multiple queries
                are provided, they must be provided as a list, and they will be
                mapped to the function inputs in the same order as they are defined.
            credentials (UserPasswordCredentials, optional): The credentials
                required to access the Postgres database. Must be a
                UserPasswordCredentials object.
            initial_values (dict, optional): The initial values for the parameters in
                the SQL queries.

        Raises:
            InputConfigurationError
        """
        self.credentials = credentials
        self.uri = uri
        self.query = query
        self.initial_values = initial_values

    @property
    def uri(self) -> str:
        """
        str: The URI of the database where the data is located.
        """
        return self._uri

    @uri.setter
    def uri(self, uri: str):
        """
        Sets the URI of the database where the data is located.

        Args:
            uri (str): The URI of the database where the data is located.
        """
        self._uri = uri
        self._parsed_uri = urlparse(uri)
        if self._parsed_uri.scheme.lower() not in POSTGRES_SCHEMES:
            raise SourceConfigurationError(
                ErrorCode.SOCE2, self._parsed_uri.scheme, POSTGRES_SCHEMES, self.uri
            )

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
            raise SourceConfigurationError(ErrorCode.SOCE31, type(initial_values))
        else:
            # Check if the initial values are valid
            _validate_initial_values_type(initial_values)
            self._initial_values = initial_values

    @property
    def query(self) -> str | List[str]:
        """
        str | List[str]: The SQL query(s) to execute.
        """
        return self._query

    @query.setter
    def query(self, query: str | List[str]):
        """
        Sets the SQL query(s) to execute

        Args:
            query (str | List[str]): The SQL query(s) to execute. If multiple queries
                are provided, they must be provided as a list, and they will be
                mapped to the function inputs in the same order as they are defined.
        """
        if isinstance(query, str):
            self._query = query
        elif isinstance(query, list):
            self._query = query
            if not all(isinstance(single_query, str) for single_query in self._query):
                raise SourceConfigurationError(ErrorCode.SOCE32, type(query))
        else:
            raise SourceConfigurationError(ErrorCode.SOCE32, type(query))

    @property
    def credentials(self) -> UserPasswordCredentials | None:
        """
        UserPasswordCredentials | None: The credentials required to access the
            PostgresDatabase. If no credentials were provided, it will return None.
        """
        return self._credentials

    @credentials.setter
    def credentials(self, credentials: UserPasswordCredentials | None):
        """
        Sets the credentials to access the PostgresDatabase.

        Args:
            credentials (UserPasswordCredentials | None): The credentials
                required to access the Postgres database. Can be a
                UserPasswordCredentials object or None
        """
        if not credentials:
            self._credentials = None
        else:
            credentials = build_credentials(credentials)
            if not (isinstance(credentials, UserPasswordCredentials)):
                raise SourceConfigurationError(ErrorCode.SOCE33, type(credentials))
            self._credentials = credentials

    @property
    def _offset_return(self) -> str:
        """
        Indicates whether the offset is returned by modifying the
        'initial_values' attribute of the plugin, or if it is part
        of the function return"""
        return OffsetReturn.FUNCTION.value

    def chunk(self, working_dir: str) -> list[str]:
        logger.debug(f"Triggering {self}")
        local_sources = _execute_sql_importer(self, working_dir)
        logger.debug(f"Obtained local sources: '{local_sources}'")
        return local_sources

    def __repr__(self) -> str:
        """
        Returns a string representation of the input.

        Returns:
            str: A string representation of the input.
        """
        return f"{self.__class__.__name__}(uri={self.uri}, query={self.query})"


def _execute_sql_importer(
    source: MariaDBSource | MySQLSource | OracleSource | PostgresSource,
    destination: str,
) -> list:
    if isinstance(source.query, str):
        source_list = [_execute_sql_query(source, destination, source.query)]
    elif isinstance(source.query, list):
        source_list = []
        for query in source.query:
            source_list.append(_execute_sql_query(source, destination, query))
    else:
        logger.error(
            f"Invalid source data, expected 'str' or 'list' but got: {source.query}"
        )
        raise TypeError(
            f"Invalid source data, expected 'str' or 'list' but got: {source.query}"
        )
    return source_list


def _execute_sql_query(
    source: MariaDBSource | MySQLSource | OracleSource | PostgresSource,
    destination: str,
    query: str,
) -> str | None:
    logger.info(f"Importing SQL query: {query}")
    if initial_values := source.initial_values:
        query = _replace_initial_values(query, initial_values)
    if isinstance(source, MySQLSource):
        logger.info("Importing SQL query from MySQL")
        uri = obtain_uri(source, log=True, add_credentials=True)
        loaded_frame = pl.read_database_uri(query=query, uri=uri)
    elif isinstance(source, PostgresSource):
        logger.info("Importing SQL query from Postgres")
        uri = obtain_uri(source, log=True, add_credentials=True)
        loaded_frame = pl.read_database_uri(query=query, uri=uri)
    elif isinstance(source, MariaDBSource):
        logger.info("Importing SQL query from MariaDB")
        uri = obtain_uri(source, log=True, add_credentials=True)
        loaded_frame = pl.read_database_uri(query=query, uri=uri)
    elif isinstance(source, OracleSource):
        logger.info("Importing SQL query from Oracle")
        uri = obtain_uri(source, log=True, add_credentials=True)
        loaded_frame = pl.read_database_uri(query=query, uri=uri)
    else:
        logger.error(f"Invalid SQL source type: {type(source)}. No data imported.")
        raise TypeError(f"Invalid SQL source type: {type(source)}. No data imported.")
    if loaded_frame.is_empty():
        logger.warning(f"No data obtained from query: '{query}'")
        return None
    else:
        destination_file_name = f"{datetime.now(tz=timezone.utc).timestamp()}.parquet"
        destination_file = os.path.join(destination, destination_file_name)
        loaded_frame.write_parquet(destination_file)
        logger.info(f"Imported SQL query to: {destination_file}")
        return destination_file_name


def _replace_initial_values(query: str, initial_values: dict) -> str:
    """
    Replace the placeholders in the query with the initial values
    """
    logger.debug(f"Replacing initial values {initial_values} in query: {query}")
    for key, value in initial_values.items():
        query = query.replace(f":{key}", str(value))
    logger.debug(f"Query after replacing initial values: {query}")
    return query
