#
# Copyright 2025 Tabs Data Inc.
#

from urllib.parse import urlparse

from tabsdata._credentials import UserPasswordCredentials, build_credentials
from tabsdata._io.connections.connections import Conn
from tabsdata._io.constants import MYSQL_SCHEME
from tabsdata.exceptions import (
    ConnectionConfigurationError,
    ErrorCode,
)


class MySQLConn(Conn):
    """
    Represents a connection configuration to MySQL.
    """

    def __init__(
        self,
        uri: str,
        credentials: UserPasswordCredentials | None = None,
        enforce_connection_params: bool = True,
        cx_src_configs_mysql: dict = None,
        cx_dst_configs_mysql: dict = None,
    ):
        """
        Initialize a MySQLConn instance.

        Args:
            uri (str): The URI of the MySQL database.
            credentials (UserPasswordCredentials | None): Credentials for authenticating
                with the database.
            enforce_connection_params (bool, optional): Whether to enforce connection
                parameters for the MySQL connection. Applicable only for a connection to
                a MySQL Destination. Defaults to True.
            cx_src_configs_mysql (dict, optional): Source-specific configuration
                parameters for the MySQL connection. Defaults to None.
            cx_dst_configs_mysql (dict, optional): Destination-specific configuration
                parameters for the MySQL connection. Defaults to None.

        Raises:
            ConnectionConfigurationError: If any connection configurations are invalid.
        """
        self.uri = uri
        self.credentials = credentials
        self.enforce_connection_params = enforce_connection_params
        self.cx_src_configs_sql = cx_src_configs_mysql or {}
        self.cx_dst_configs_sql = cx_dst_configs_mysql or {}

    @property
    def uri(self) -> str:
        """
        str: The URI of the database where the data is located.
        """
        return self._uri

    @uri.setter
    def uri(self, uri: str):
        """
        Set the URI of the database where the data is located.
        Args:
            uri (str): The URI of the database.
        """
        if not isinstance(uri, str):
            raise ConnectionConfigurationError(
                ErrorCode.CXCE1, self.__class__.__name__, type(uri)
            )
        self._uri = uri
        self._parsed_uri = urlparse(uri)

        if self._parsed_uri.scheme.lower() != MYSQL_SCHEME:
            raise ConnectionConfigurationError(
                ErrorCode.CXCE2, self._parsed_uri.scheme, MYSQL_SCHEME, self._uri
            )

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
        Set the credentials required to access the MySQLDatabase.
        Args:
            credentials (UserPasswordCredentials | None): The credentials required to
                access the MySQLDatabase.
        """
        if not credentials:
            self._credentials = None
        else:
            credentials = build_credentials(credentials)
            if not (isinstance(credentials, UserPasswordCredentials)):
                raise ConnectionConfigurationError(
                    ErrorCode.CXCE3, self.__class__.__name__, type(credentials)
                )
            self._credentials = credentials

    @property
    def enforce_connection_params(self) -> bool:
        """
        bool: Whether to enforce connection parameters for the MySQL connection.
        """
        return self._enforce_connection_params

    @enforce_connection_params.setter
    def enforce_connection_params(self, enforce_connection_params: bool):
        """
        Set whether to enforce connection parameters for the MySQL connection.
        Args:
            enforce_connection_params (bool): Whether to enforce connection parameters
                for the MySQL connection.
        """
        if not isinstance(enforce_connection_params, bool):
            raise ConnectionConfigurationError(
                ErrorCode.CXCE4,
                self.__class__.__name__,
                type(enforce_connection_params),
            )
        self._enforce_connection_params = enforce_connection_params

    @property
    def cx_src_configs_sql(self) -> dict:
        """
        dict: The source configuration parameters for the MySQL connection.
        """
        return self._cx_src_configs_sql

    @cx_src_configs_sql.setter
    def cx_src_configs_sql(self, cx_src_configs_sql: dict | None):
        """
        Set the source configuration parameters for the MySQL connection.
        Args:
            cx_src_configs_sql (dict | None): The source configuration parameters for
                the MySQL connection.
        """
        if cx_src_configs_sql is None:
            cx_src_configs_sql = {}
        elif not isinstance(cx_src_configs_sql, dict):
            raise ConnectionConfigurationError(
                ErrorCode.CXCE5, self.__class__.__name__, type(cx_src_configs_sql)
            )
        self._cx_src_configs_sql = cx_src_configs_sql

    @property
    def cx_dst_configs_sql(self) -> dict:
        """
        dict: The destination configuration parameters for the MySQL connection.
        """
        return self._cx_dst_configs_sql

    @cx_dst_configs_sql.setter
    def cx_dst_configs_sql(self, cx_dst_configs_sql: dict | None):
        """
        Set the destination configuration parameters for the MySQL connection.
        Args:
            cx_dst_configs_sql (dict | None): The destination configuration parameters
            for the MySQL connection.
        """
        if cx_dst_configs_sql is None:
            cx_dst_configs_sql = {}
        elif not isinstance(cx_dst_configs_sql, dict):
            raise ConnectionConfigurationError(
                ErrorCode.CXCE6, self.__class__.__name__, type(cx_dst_configs_sql)
            )
        self._cx_dst_configs_sql = cx_dst_configs_sql

    def __repr__(self):
        return (
            f"{self.__class__.__name__}(uri='{self.uri}', "
            f"credentials={self.credentials}, "
            f"enforce_connection_params={self.enforce_connection_params}, "
            f"cx_src_configs_mysql={self.cx_src_configs_sql}, "
            f"cx_dst_configs_mysql={self.cx_dst_configs_sql})"
        )

    def __eq__(self, other):
        if not isinstance(other, MySQLConn):
            return False
        return (
            self.uri == other.uri
            and self.credentials == other.credentials
            and self.enforce_connection_params == other.enforce_connection_params
            and self.cx_src_configs_sql == other.cx_src_configs_sql
            and self.cx_dst_configs_sql == other.cx_dst_configs_sql
        )
