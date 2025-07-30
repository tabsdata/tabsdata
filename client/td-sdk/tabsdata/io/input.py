#
# Copyright 2025 Tabs Data Inc.
#

import logging
from abc import ABC, abstractmethod
from enum import Enum
from typing import List
from urllib.parse import urlparse

from tabsdata.credentials import (
    UserPasswordCredentials,
    build_credentials,
)
from tabsdata.exceptions import (
    ErrorCode,
    InputConfigurationError,
)
from tabsdata.io.constants import (
    MARIADB_SCHEME,
    MYSQL_SCHEME,
    ORACLE_SCHEME,
    POSTGRES_SCHEMES,
)
from tabsdata.tableuri import build_table_uri_object

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
            raise InputConfigurationError(ErrorCode.ICE40, type(key))


class InputIdentifiers(Enum):
    """
    Enum for the identifiers of the different types of data inputs.
    """

    LOCALFILE = "localfile-input"
    MARIADB = "mariadb-input"
    MYSQL = "mysql-input"
    ORACLE = "oracle-input"
    POSTGRES = "postgres-input"
    TABLE = "table-input"


class Input(ABC):
    """
    Abstract base class for managing data input configurations.
    """

    @abstractmethod
    def to_dict(self) -> dict:
        """
        Convert the Input object to a dictionary with all
            the relevant information.

        Returns:
            dict: A dictionary with the relevant information of the Input
                object.
        """

    def __eq__(self, other: object) -> bool:
        if not isinstance(other, Input):
            return False
        return self.to_dict() == other.to_dict()

    def __repr__(self) -> str:
        """
        Returns a string representation of the Input.

        Returns:
            str: A string representation of the Input.
        """
        return f"{self.__class__.__name__}({self.to_dict()[self.IDENTIFIER]})"


class MariaDBSource(Input):
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

    Methods:
        to_dict(): Converts the MariaDBSource object to a dictionary.
    """

    IDENTIFIER = InputIdentifiers.MARIADB.value

    CREDENTIALS_KEY = "credentials"
    INITIAL_VALUES_KEY = "initial_values"
    QUERY_KEY = "query"
    URI_KEY = "uri"

    def __init__(
        self,
        uri: str,
        query: str | List[str],
        credentials: dict | UserPasswordCredentials | None = None,
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
            credentials (dict | UserPasswordCredentials, optional): The credentials
                required to access the MariaDB database. Can be a dictionary or a
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
        if self._parsed_uri.scheme != MARIADB_SCHEME:
            raise InputConfigurationError(
                ErrorCode.ICE2, self._parsed_uri.scheme, MARIADB_SCHEME, self.uri
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
            raise InputConfigurationError(ErrorCode.ICE34, type(initial_values))
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
                raise InputConfigurationError(ErrorCode.ICE35, type(query))
        else:
            raise InputConfigurationError(ErrorCode.ICE35, type(query))

    @property
    def credentials(self) -> UserPasswordCredentials | None:
        """
        UserPasswordCredentials | None: The credentials required to access
            MariaDB. If no credentials were provided, it will return None.
        """
        return self._credentials

    @credentials.setter
    def credentials(self, credentials: dict | UserPasswordCredentials | None):
        """
        Sets the credentials to access MariaDB.

        Args:
            credentials (dict | UserPasswordCredentials | None): The credentials
                required to access MariaDB. Can be a UserPasswordCredentials
                object, a dictionary or None
        """
        if not credentials:
            self._credentials = None
        else:
            credentials = build_credentials(credentials)
            if not (isinstance(credentials, UserPasswordCredentials)):
                raise InputConfigurationError(ErrorCode.ICE36, type(credentials))
            self._credentials = credentials

    def to_dict(self) -> dict:
        """
        Converts the MariaDBSource object to a dictionary with all the relevant
            information.

        Returns:
            dict: A dictionary with the relevant information of the MariaDBSource
                object.
        """
        return {
            self.IDENTIFIER: {
                self.INITIAL_VALUES_KEY: self.initial_values,
                self.QUERY_KEY: self.query,
                self.URI_KEY: self.uri,
                self.CREDENTIALS_KEY: (
                    self.credentials.to_dict() if self.credentials else None
                ),
            }
        }


class MySQLSource(Input):
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

    Methods:
        to_dict(): Converts the MySQLSource object to a dictionary.
    """

    IDENTIFIER = InputIdentifiers.MYSQL.value

    CREDENTIALS_KEY = "credentials"
    INITIAL_VALUES_KEY = "initial_values"
    QUERY_KEY = "query"
    URI_KEY = "uri"

    def __init__(
        self,
        uri: str,
        query: str | List[str],
        credentials: dict | UserPasswordCredentials | None = None,
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
            credentials (dict | UserPasswordCredentials, optional): The credentials
                required to access the MySQL database. Can be a dictionary or a
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
        if self._parsed_uri.scheme != MYSQL_SCHEME:
            raise InputConfigurationError(
                ErrorCode.ICE2, self._parsed_uri.scheme, MYSQL_SCHEME, self.uri
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
            raise InputConfigurationError(ErrorCode.ICE12, type(initial_values))
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
                raise InputConfigurationError(ErrorCode.ICE19, type(query))
        else:
            raise InputConfigurationError(ErrorCode.ICE19, type(query))

    @property
    def credentials(self) -> UserPasswordCredentials | None:
        """
        UserPasswordCredentials | None: The credentials required to access the
            MySQLDatabase. If no credentials were provided, it will return None.
        """
        return self._credentials

    @credentials.setter
    def credentials(self, credentials: dict | UserPasswordCredentials | None):
        """
        Sets the credentials to access the MySQLDatabase.

        Args:
            credentials (dict | UserPasswordCredentials | None): The credentials
                required to access the MySQLDatabase. Can be a UserPasswordCredentials
                object, a dictionary or None
        """
        if not credentials:
            self._credentials = None
        else:
            credentials = build_credentials(credentials)
            if not (isinstance(credentials, UserPasswordCredentials)):
                raise InputConfigurationError(ErrorCode.ICE22, type(credentials))
            self._credentials = credentials

    def to_dict(self) -> dict:
        """
        Converts the MySQLSource object to a dictionary with all the relevant
            information.

        Returns:
            dict: A dictionary with the relevant information of the MySQLSource
                object.
        """
        return {
            self.IDENTIFIER: {
                self.INITIAL_VALUES_KEY: self.initial_values,
                self.QUERY_KEY: self.query,
                self.URI_KEY: self.uri,
                self.CREDENTIALS_KEY: (
                    self.credentials.to_dict() if self.credentials else None
                ),
            }
        }


class OracleSource(Input):
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

    Methods:
        to_dict(): Converts the OracleSource object to a dictionary.
    """

    IDENTIFIER = InputIdentifiers.ORACLE.value

    CREDENTIALS_KEY = "credentials"
    INITIAL_VALUES_KEY = "initial_values"
    QUERY_KEY = "query"
    URI_KEY = "uri"

    def __init__(
        self,
        uri: str,
        query: str | List[str],
        credentials: dict | UserPasswordCredentials | None = None,
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
            credentials (dict | UserPasswordCredentials, optional): The credentials
                required to access the Oracle database. Can be a dictionary or a
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
        if self._parsed_uri.scheme != ORACLE_SCHEME:
            raise InputConfigurationError(
                ErrorCode.ICE2, self._parsed_uri.scheme, ORACLE_SCHEME, self.uri
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
            raise InputConfigurationError(ErrorCode.ICE37, type(initial_values))
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
                raise InputConfigurationError(ErrorCode.ICE38, type(query))
        else:
            raise InputConfigurationError(ErrorCode.ICE38, type(query))

    @property
    def credentials(self) -> UserPasswordCredentials | None:
        """
        UserPasswordCredentials | None: The credentials required to access
            Oracle. If no credentials were provided, it will return None.
        """
        return self._credentials

    @credentials.setter
    def credentials(self, credentials: dict | UserPasswordCredentials | None):
        """
        Sets the credentials to access Oracle.

        Args:
            credentials (dict | UserPasswordCredentials | None): The credentials
                required to access Oracle. Can be a UserPasswordCredentials
                object, a dictionary or None
        """
        if not credentials:
            self._credentials = None
        else:
            credentials = build_credentials(credentials)
            if not (isinstance(credentials, UserPasswordCredentials)):
                raise InputConfigurationError(ErrorCode.ICE39, type(credentials))
            self._credentials = credentials

    def to_dict(self) -> dict:
        """
        Converts the OracleSource object to a dictionary with all the relevant
            information.

        Returns:
            dict: A dictionary with the relevant information of the OracleSource
                object.
        """
        return {
            self.IDENTIFIER: {
                self.INITIAL_VALUES_KEY: self.initial_values,
                self.QUERY_KEY: self.query,
                self.URI_KEY: self.uri,
                self.CREDENTIALS_KEY: (
                    self.credentials.to_dict() if self.credentials else None
                ),
            }
        }


class PostgresSource(Input):
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

    Methods:
        to_dict(): Converts the PostgresSource object to a dictionary.
    """

    IDENTIFIER = InputIdentifiers.POSTGRES.value

    CREDENTIALS_KEY = "credentials"
    INITIAL_VALUES_KEY = "initial_values"
    QUERY_KEY = "query"
    URI_KEY = "uri"

    def __init__(
        self,
        uri: str,
        query: str | List[str],
        credentials: dict | UserPasswordCredentials | None = None,
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
            credentials (dict | UserPasswordCredentials, optional): The credentials
                required to access the Postgres database. Can be a dictionary or a
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
        if self._parsed_uri.scheme not in POSTGRES_SCHEMES:
            raise InputConfigurationError(
                ErrorCode.ICE2, self._parsed_uri.scheme, POSTGRES_SCHEMES, self.uri
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
            raise InputConfigurationError(ErrorCode.ICE31, type(initial_values))
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
                raise InputConfigurationError(ErrorCode.ICE32, type(query))
        else:
            raise InputConfigurationError(ErrorCode.ICE32, type(query))

    @property
    def credentials(self) -> UserPasswordCredentials | None:
        """
        UserPasswordCredentials | None: The credentials required to access the
            PostgresDatabase. If no credentials were provided, it will return None.
        """
        return self._credentials

    @credentials.setter
    def credentials(self, credentials: dict | UserPasswordCredentials | None):
        """
        Sets the credentials to access the PostgresDatabase.

        Args:
            credentials (dict | UserPasswordCredentials | None): The credentials
                required to access the Postgres database. Can be a
                UserPasswordCredentials object, a dictionary or None
        """
        if not credentials:
            self._credentials = None
        else:
            credentials = build_credentials(credentials)
            if not (isinstance(credentials, UserPasswordCredentials)):
                raise InputConfigurationError(ErrorCode.ICE33, type(credentials))
            self._credentials = credentials

    def to_dict(self) -> dict:
        """
        Converts the PostgresSource object to a dictionary with all the relevant
            information.

        Returns:
            dict: A dictionary with the relevant information of the PostgresSource
                object.
        """
        return {
            self.IDENTIFIER: {
                self.INITIAL_VALUES_KEY: self.initial_values,
                self.QUERY_KEY: self.query,
                self.URI_KEY: self.uri,
                self.CREDENTIALS_KEY: (
                    self.credentials.to_dict() if self.credentials else None
                ),
            }
        }


class TableInput(Input):
    """
    Class for managing the configuration of table-based data inputs.

    Attributes:
        table (str | List[str]): The table(s) to load.

    Methods:
        to_dict(): Converts the TableInput object to a dictionary.
    """

    IDENTIFIER = InputIdentifiers.TABLE.value

    TABLE_KEY = "table"

    def __init__(self, table: str | List[str]):
        """
        Initializes the TableInput with the given tables. If multiple tables are
            provided, they must be provided as a list.

        Args:
            table (str | List[str]): The table(s) to load.
                If multiple tables are provided, they must be provided as a list.
        """
        self.table = table

    @property
    def table(self) -> str | List[str]:
        """
        str | List[str]: The table(s) to load.
        """
        return self._table

    @table.setter
    def table(self, table: str | List[str]):
        """
        Sets the table(s) to load.

        Args:
            table (str | List[str]): The table(s) to load.
                If multiple tables are provided, they must be provided as a list
        """
        self._table = table
        if isinstance(table, list):
            assert [build_table_uri_object(single_uri) for single_uri in table]
            self._table = table
            self._table_list = self._table
        else:
            assert build_table_uri_object(table)
            self._table = table
            self._table_list = [self._table]
        self._verify_valid_table_list()

    def _verify_valid_table_list(self):
        """
        Verifies that the tables in the list are valid.
        """
        for table in self._table_list:
            uri = build_table_uri_object(table)
            if not uri.table:
                raise InputConfigurationError(ErrorCode.ICE25, table)

    def to_dict(self) -> dict:
        """
        Converts the TableInput object to a dictionary with all the relevant
            information.

        Returns:
            dict: A dictionary with the relevant information of the Output
                object.
        """
        return {self.IDENTIFIER: {self.TABLE_KEY: self._table_list}}


def build_input(input: dict | Input | None) -> Input | None:
    """
    Builds an Input object.

    Args:
        input (dict | Input | None): A dictionary with the input information or an
            Input object.

    Returns:
        Input: A Input object built from the input.

    Raises:
        InputConfigurationError
    """
    if not input:
        return None
    elif isinstance(input, Input):
        return input
    elif isinstance(input, dict):
        return build_input_from_dict(input)
    else:
        raise InputConfigurationError(ErrorCode.ICE11, type(input))


def build_input_from_dict(input: dict) -> Input:
    valid_identifiers = [element.value for element in InputIdentifiers]
    # The input dictionary must have exactly one key, which must be one of the
    # valid identifiers
    if len(input) != 1 or next(iter(input)) not in valid_identifiers:
        raise InputConfigurationError(
            ErrorCode.ICE7, valid_identifiers, list(input.keys())
        )
    # Since we have only one key, we select the identifier and the configuration
    identifier, configuration = next(iter(input.items()))
    # The configuration must be a dictionary
    if not isinstance(configuration, dict):
        raise InputConfigurationError(ErrorCode.ICE8, identifier, type(configuration))
    existing_inputs = [
        MySQLSource,
        TableInput,
        PostgresSource,
        MariaDBSource,
        OracleSource,
    ]
    for input_class in existing_inputs:
        if identifier == input_class.IDENTIFIER:
            return input_class(**configuration)
