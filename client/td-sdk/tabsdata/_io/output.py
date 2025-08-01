#
# Copyright 2025 Tabs Data Inc.
#

import logging
from abc import ABC, abstractmethod
from enum import Enum
from typing import List, Literal
from urllib.parse import urlparse

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
from tabsdata._io.outputs.shared_enums import IfTableExistsStrategy
from tabsdata.exceptions import (
    ErrorCode,
    OutputConfigurationError,
)

logger = logging.getLogger(__name__)


class OutputIdentifiers(Enum):
    """
    Enum for the identifiers of the different types of data outputs.
    """

    MARIADB = "mariadb-output"
    MYSQL = "mysql-output"
    ORACLE = "oracle-output"
    POSTGRES = "postgres-output"
    TABLE = "table-output"


class Output(ABC):
    """
    Abstract base class for managing data output configurations.
    """

    @abstractmethod
    def _to_dict(self) -> dict:
        """
        Convert the Output object to a dictionary with all
            the relevant information.

        Returns:
            dict: A dictionary with the relevant information of the Output
                object.
        """

    def __eq__(self, other: object) -> bool:
        if not isinstance(other, Output):
            return False
        return self._to_dict() == other._to_dict()

    def __repr__(self) -> str:
        """
        Returns a string representation of the Output.

        Returns:
            str: A string representation of the Output.
        """
        return f"{self.__class__.__name__}({self._to_dict()[self.IDENTIFIER]})"


class MariaDBDestination(Output):
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

    Methods:
        to_dict(): Converts the MariaDBDestination object to a dictionary
    """

    IDENTIFIER = OutputIdentifiers.MARIADB.value

    CREDENTIALS_KEY = "credentials"
    DESTINATION_TABLE_KEY = "destination_table"
    IF_TABLE_EXISTS_KEY = "if_table_exists"
    URI_KEY = "uri"

    def __init__(
        self,
        uri: str,
        destination_table: List[str] | str,
        credentials: dict | UserPasswordCredentials = None,
        if_table_exists: Literal["append", "replace"] = "append",
    ):
        """
        Initializes the MariaDBDestination with the given URI and destination table,
        and optionally connection credentials.

        Args:
            uri (str): The URI of the database where the data is going to be stored.
            destination_table (List[str] | str): The tables to create. If multiple
                tables are provided, they must be provided as a list.
            credentials (dict | UserPasswordCredentials, optional): The credentials
                required to access the MariaDB database. Can be a dictionary or a
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
    def if_table_exists(self) -> Literal["append", "replace"]:
        """
        str: The strategy to follow when the table already exists.
        """
        return self._if_table_exists

    @if_table_exists.setter
    def if_table_exists(self, if_table_exists: Literal["append", "replace"]):
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
            raise OutputConfigurationError(
                ErrorCode.OCE26, valid_values, if_table_exists
            )
        self._if_table_exists = if_table_exists

    def _to_dict(self) -> dict:
        """
        Converts the MariaDBDestination object to a dictionary with all the relevant
            information.

        Returns:
            dict: A dictionary with the relevant information of the MariaDBDestination
                object.
        """
        return {
            self.IDENTIFIER: {
                self.URI_KEY: self.uri,
                self.DESTINATION_TABLE_KEY: self.destination_table,
                self.CREDENTIALS_KEY: (
                    self.credentials._to_dict() if self.credentials else None
                ),
                self.IF_TABLE_EXISTS_KEY: self.if_table_exists,
            }
        }

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
        if not self._parsed_uri.scheme.startswith(MARIADB_SCHEME):
            raise OutputConfigurationError(
                ErrorCode.OCE2, self._parsed_uri.scheme, MARIADB_SCHEME, self.uri
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
            raise OutputConfigurationError(ErrorCode.OCE22, type(destination_table))

    @property
    def credentials(self) -> UserPasswordCredentials:
        """
        UserPasswordCredentials: The credentials required to access the MariaDB
            database.
        """
        return self._credentials

    @credentials.setter
    def credentials(self, credentials: dict | UserPasswordCredentials | None):
        """
        Sets the credentials to access the MariaDB database.

        Args:
            credentials (dict | UserPasswordCredentials | None): The credentials
                required to access the MariaDB database. Can be a
                UserPasswordCredentials object, a dictionary or None if no
                credentials are needed.
        """
        if not credentials:
            self._credentials = None
        else:
            credentials = build_credentials(credentials)
            if not (isinstance(credentials, UserPasswordCredentials)):
                raise OutputConfigurationError(ErrorCode.OCE23, type(credentials))
            self._credentials = credentials


class MySQLDestination(Output):
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

    Methods:
        to_dict(): Converts the MySQLDestination object to a dictionary
    """

    IDENTIFIER = OutputIdentifiers.MYSQL.value

    CREDENTIALS_KEY = "credentials"
    DESTINATION_TABLE_KEY = "destination_table"
    IF_TABLE_EXISTS_KEY = "if_table_exists"
    URI_KEY = "uri"

    def __init__(
        self,
        uri: str,
        destination_table: List[str] | str,
        credentials: dict | UserPasswordCredentials = None,
        if_table_exists: Literal["append", "replace"] = "append",
    ):
        """
        Initializes the MySQLDestination with the given URI and destination table,
        and optionally connection credentials.

        Args:
            uri (str): The URI of the database where the data is going to be stored.
            destination_table (List[str] | str): The tables to create. If multiple
                tables are provided, they must be provided as a list.
            credentials (dict | UserPasswordCredentials, optional): The credentials
                required to access the MySQL database. Can be a dictionary or a
                UserPasswordCredentials object.

        Raises:
            OutputConfigurationError
        """
        self.credentials = credentials
        self.uri = uri
        self.destination_table = destination_table
        self.if_table_exists = if_table_exists

    @property
    def if_table_exists(self) -> Literal["append", "replace"]:
        """
        str: The strategy to follow when the table already exists.
        """
        return self._if_table_exists

    @if_table_exists.setter
    def if_table_exists(self, if_table_exists: Literal["append", "replace"]):
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
            raise OutputConfigurationError(
                ErrorCode.OCE27, valid_values, if_table_exists
            )
        self._if_table_exists = if_table_exists

    def _to_dict(self) -> dict:
        """
        Converts the MySQLDestination object to a dictionary with all the relevant
            information.

        Returns:
            dict: A dictionary with the relevant information of the MySQLDestination
                object.
        """
        return {
            self.IDENTIFIER: {
                self.URI_KEY: self.uri,
                self.DESTINATION_TABLE_KEY: self.destination_table,
                self.CREDENTIALS_KEY: (
                    self.credentials._to_dict() if self.credentials else None
                ),
                self.IF_TABLE_EXISTS_KEY: self.if_table_exists,
            }
        }

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
        if not self._parsed_uri.scheme.startswith(MYSQL_SCHEME):
            raise OutputConfigurationError(
                ErrorCode.OCE2, self._parsed_uri.scheme, MYSQL_SCHEME, self.uri
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
            raise OutputConfigurationError(ErrorCode.OCE8, type(destination_table))

    @property
    def credentials(self) -> UserPasswordCredentials:
        """
        UserPasswordCredentials: The credentials required to access the MySQLDatabase.
        """
        return self._credentials

    @credentials.setter
    def credentials(self, credentials: dict | UserPasswordCredentials | None):
        """
        Sets the credentials to access the MySQLDatabase.

        Args:
            credentials (dict | UserPasswordCredentials | None): The credentials
                required to access the MySQLDatabase. Can be a
                UserPasswordCredentials object, a dictionary or None if no
                credentials are needed.
        """
        if not credentials:
            self._credentials = None
        else:
            credentials = build_credentials(credentials)
            if not (isinstance(credentials, UserPasswordCredentials)):
                raise OutputConfigurationError(ErrorCode.OCE9, type(credentials))
            self._credentials = credentials


class OracleDestination(Output):
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

    Methods:
        to_dict(): Converts the OracleDestination object to a dictionary
    """

    IDENTIFIER = OutputIdentifiers.ORACLE.value

    CREDENTIALS_KEY = "credentials"
    DESTINATION_TABLE_KEY = "destination_table"
    IF_TABLE_EXISTS_KEY = "if_table_exists"
    URI_KEY = "uri"

    def __init__(
        self,
        uri: str,
        destination_table: List[str] | str,
        credentials: dict | UserPasswordCredentials = None,
        if_table_exists: Literal["append", "replace"] = "append",
    ):
        """
        Initializes the OracleDestination with the given URI and destination table,
        and optionally connection credentials.

        Args:
            uri (str): The URI of the database where the data is going to be stored.
            destination_table (List[str] | str): The tables to create. If multiple
                tables are provided, they must be provided as a list.
            credentials (dict | UserPasswordCredentials, optional): The credentials
                required to access the Oracle database. Can be a dictionary or a
                UserPasswordCredentials object.

        Raises:
            OutputConfigurationError
        """
        self.credentials = credentials
        self.uri = uri
        self.destination_table = destination_table
        self.if_table_exists = if_table_exists

    @property
    def if_table_exists(self) -> Literal["append", "replace"]:
        """
        str: The strategy to follow when the table already exists.
        """
        return self._if_table_exists

    @if_table_exists.setter
    def if_table_exists(self, if_table_exists: Literal["append", "replace"]):
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
            raise OutputConfigurationError(
                ErrorCode.OCE28, valid_values, if_table_exists
            )
        self._if_table_exists = if_table_exists

    def _to_dict(self) -> dict:
        """
        Converts the OracleDestination object to a dictionary with all the relevant
            information.

        Returns:
            dict: A dictionary with the relevant information of the OracleDestination
                object.
        """
        return {
            self.IDENTIFIER: {
                self.URI_KEY: self.uri,
                self.DESTINATION_TABLE_KEY: self.destination_table,
                self.CREDENTIALS_KEY: (
                    self.credentials._to_dict() if self.credentials else None
                ),
                self.IF_TABLE_EXISTS_KEY: self.if_table_exists,
            }
        }

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
        if not self._parsed_uri.scheme.startswith(ORACLE_SCHEME):
            raise OutputConfigurationError(
                ErrorCode.OCE2, self._parsed_uri.scheme, ORACLE_SCHEME, self.uri
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
            raise OutputConfigurationError(ErrorCode.OCE24, type(destination_table))

    @property
    def credentials(self) -> UserPasswordCredentials:
        """
        UserPasswordCredentials: The credentials required to access the Oracle
            database.
        """
        return self._credentials

    @credentials.setter
    def credentials(self, credentials: dict | UserPasswordCredentials | None):
        """
        Sets the credentials to access the Oracle database.

        Args:
            credentials (dict | UserPasswordCredentials | None): The credentials
                required to access the Oracle database. Can be a
                UserPasswordCredentials object, a dictionary or None if no
                credentials are needed.
        """
        if not credentials:
            self._credentials = None
        else:
            credentials = build_credentials(credentials)
            if not (isinstance(credentials, UserPasswordCredentials)):
                raise OutputConfigurationError(ErrorCode.OCE25, type(credentials))
            self._credentials = credentials


class PostgresDestination(Output):
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

    Methods:
        to_dict(): Converts the PostgresDestination object to a dictionary
    """

    IDENTIFIER = OutputIdentifiers.POSTGRES.value

    CREDENTIALS_KEY = "credentials"
    DESTINATION_TABLE_KEY = "destination_table"
    IF_TABLE_EXISTS_KEY = "if_table_exists"
    URI_KEY = "uri"

    def __init__(
        self,
        uri: str,
        destination_table: List[str] | str,
        credentials: dict | UserPasswordCredentials = None,
        if_table_exists: Literal["append", "replace"] = "append",
    ):
        """
        Initializes the PostgresDestination with the given URI and destination table,
        and optionally connection credentials.

        Args:
            uri (str): The URI of the database where the data is going to be stored.
            destination_table (List[str] | str): The tables to create. If multiple
                tables are provided, they must be provided as a list.
            credentials (dict | UserPasswordCredentials, optional): The credentials
                required to access the Postgres database. Can be a dictionary or a
                UserPasswordCredentials object.

        Raises:
            OutputConfigurationError
        """
        self.credentials = credentials
        self.uri = uri
        self.destination_table = destination_table
        self.if_table_exists = if_table_exists

    @property
    def if_table_exists(self) -> Literal["append", "replace"]:
        """
        str: The strategy to follow when the table already exists.
        """
        return self._if_table_exists

    @if_table_exists.setter
    def if_table_exists(self, if_table_exists: Literal["append", "replace"]):
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
            raise OutputConfigurationError(
                ErrorCode.OCE29, valid_values, if_table_exists
            )
        self._if_table_exists = if_table_exists

    def _to_dict(self) -> dict:
        """
        Converts the PostgresDestination object to a dictionary with all the relevant
            information.

        Returns:
            dict: A dictionary with the relevant information of the PostgresDestination
                object.
        """
        return {
            self.IDENTIFIER: {
                self.URI_KEY: self.uri,
                self.DESTINATION_TABLE_KEY: self.destination_table,
                self.CREDENTIALS_KEY: (
                    self.credentials._to_dict() if self.credentials else None
                ),
                self.IF_TABLE_EXISTS_KEY: self.if_table_exists,
            }
        }

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
            [self._parsed_uri.scheme.startswith(scheme) for scheme in POSTGRES_SCHEMES]
        ):
            raise OutputConfigurationError(
                ErrorCode.OCE2, self._parsed_uri.scheme, POSTGRES_SCHEMES, self.uri
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
            raise OutputConfigurationError(ErrorCode.OCE20, type(destination_table))

    @property
    def credentials(self) -> UserPasswordCredentials:
        """
        UserPasswordCredentials: The credentials required to access the
            Postgres database.
        """
        return self._credentials

    @credentials.setter
    def credentials(self, credentials: dict | UserPasswordCredentials | None):
        """
        Sets the credentials to access the PostgresDatabase.

        Args:
            credentials (dict | UserPasswordCredentials | None): The credentials
                required to access the PostgresDatabase. Can be a
                UserPasswordCredentials object, a dictionary or None if no
                credentials are needed.
        """
        if not credentials:
            self._credentials = None
        else:
            credentials = build_credentials(credentials)
            if not (isinstance(credentials, UserPasswordCredentials)):
                raise OutputConfigurationError(ErrorCode.OCE21, type(credentials))
            self._credentials = credentials


class TableOutput(Output):
    """
    Class for managing the configuration of table-based data outputs.

    Attributes:
        table (str | List[str]): The table(s) to create. If multiple tables are
            provided, they must be provided as a list.

    Methods:
        to_dict(): Converts the TableOutput object to a dictionary
    """

    IDENTIFIER = OutputIdentifiers.TABLE.value

    TABLE_KEY = "table"

    def __init__(self, table: str | List[str]):
        """
        Initializes the TableOutput with the given table(s) to create.

        Args:
            table (str | List[str]): The table(s) to create. If multiple tables are
                provided, they must be provided as a list.
        """
        self.table = table

    @property
    def table(self) -> str | List[str]:
        """
        str | List[str]: The table(s) to create. If multiple tables are provided,
            they must be provided as a list.
        """
        return self._table

    @table.setter
    def table(self, table: str | List[str]):
        """
        Sets the table(s) to create.

        Args:
            table (str | List[str]): The table(s) to create. If multiple tables are
                provided, they must be provided as a list.
        """
        self._table = table
        self._table_list = table if isinstance(table, list) else [table]
        for single_table in self._table_list:
            if not isinstance(single_table, str):
                raise OutputConfigurationError(
                    ErrorCode.OCE10, single_table, type(single_table)
                )

    def _to_dict(self) -> dict:
        """
        Converts the TableOutput object to a dictionary with all the relevant
        information.
        """
        return {self.IDENTIFIER: {self.TABLE_KEY: self._table_list}}


def build_output(
    output: dict | Output | None,
) -> Output | MySQLDestination | TableOutput | None:
    """
    Builds an Output object.

    Args:
        output (dict | Output | None): A dictionary with the output information,
            or an Output object.

    Returns:
        Output: A Output object built from the output. That can be an Output object,
            or None if nothing was provided.

    Raises:
        OutputConfigurationError
    """
    if not output:
        return None
    elif isinstance(output, Output):
        return output
    elif isinstance(output, dict):
        return build_output_from_dict(output)
    else:
        raise OutputConfigurationError(ErrorCode.OCE7, type(output))


def build_output_from_dict(
    output: dict,
) -> Output | MySQLDestination | TableOutput | None:
    # The output dictionary must have exactly one key, which must be one of the
    # valid identifiers
    valid_identifiers = [element.value for element in OutputIdentifiers]
    if len(output) != 1 or next(iter(output)) not in valid_identifiers:
        raise OutputConfigurationError(
            ErrorCode.OCE3, valid_identifiers, list(output.keys())
        )
    # Since we have only one key, we select the identifier and the configuration
    identifier, configuration = next(iter(output.items()))
    # The configuration must be a dictionary
    if not isinstance(configuration, dict):
        raise OutputConfigurationError(ErrorCode.OCE4, identifier, type(configuration))
    existing_outputs = [
        MySQLDestination,
        TableOutput,
        PostgresDestination,
        MariaDBDestination,
        OracleDestination,
    ]
    for output_class in existing_outputs:
        if identifier == output_class.IDENTIFIER:
            return output_class(**configuration)
