#
# Copyright 2025 Tabs Data Inc.
#

from urllib.parse import urlparse

from tabsdata._credentials import UserPasswordCredentials, build_credentials
from tabsdata._io.connections.connections import Conn
from tabsdata._io.constants import MARIADB_SCHEME
from tabsdata._tabsserver.function.sql_utils import add_mariadb_collation
from tabsdata.exceptions import (
    ConnectionConfigurationError,
    ErrorCode,
)


class MariaDBConn(Conn):
    """
    Represents a connection configuration to MariaDB.
    """

    def __init__(
        self,
        uri: str,
        credentials: UserPasswordCredentials | None = None,
        enforce_connection_params: bool = True,
        cx_src_configs_mariadb: dict = None,
        cx_dst_configs_mariadb: dict = None,
    ):
        """
        Initialize a MariaDBConn instance.

        Args:
            uri (str): The URI of the MariaDB database.
            credentials (UserPasswordCredentials | None): Credentials for authenticating
                with the database.
            enforce_connection_params (bool, optional): Whether to enforce connection
                parameters for the MariaDB connection. Applicable only for a connection
                to a MariaDB Destination. Defaults to True.
            cx_src_configs_mariadb (dict, optional): Source-specific configuration
                parameters for the MariaDB connection. Defaults to None.
            cx_dst_configs_mariadb (dict, optional): Destination-specific configuration
                parameters for the MariaDB connection. Defaults to None.

        Raises:
            ConnectionConfigurationError: If any connection configurations are invalid.
        """
        self.uri = uri
        self.credentials = credentials
        self.enforce_connection_params = enforce_connection_params
        self.cx_src_configs_mariadb = cx_src_configs_mariadb or {}
        self.cx_dst_configs_mariadb = cx_dst_configs_mariadb or {}

    @property
    def uri(self) -> str:
        """
        str: The URI of the database where the data is located.
        """
        return self._uri

    @uri.setter
    def uri(self, uri: str):
        """
        Set the URI of the database.
        Args:
            uri (str): The URI of the database.
        """
        if not isinstance(uri, str):
            raise ConnectionConfigurationError(
                ErrorCode.CXCE1, self.__class__.__name__, type(uri)
            )
        parsed_uri = urlparse(uri)
        if parsed_uri.scheme.lower() != MARIADB_SCHEME:
            raise ConnectionConfigurationError(
                ErrorCode.CXCE2, parsed_uri.scheme, MARIADB_SCHEME, uri
            )
        self._parsed_uri = parsed_uri
        self._uri = uri
        self._uri_with_collation = add_mariadb_collation(uri)

    @property
    def credentials(self) -> UserPasswordCredentials | None:
        """
        UserPasswordCredentials | None: The credentials required to access the
            MariaDB database. If no credentials were provided, it will return None.
        """
        return self._credentials

    @credentials.setter
    def credentials(self, credentials: UserPasswordCredentials | None):
        """
        Set the credentials required to access the MariaDB database.
        Args:
            credentials (UserPasswordCredentials | None): The credentials required to
                access the MariaDB database.
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
        bool: Whether to enforce connection parameters for the MariaDB connection.
        """
        return self._enforce_connection_params

    @enforce_connection_params.setter
    def enforce_connection_params(self, enforce_connection_params: bool):
        """
        Set whether to enforce connection parameters for the MariaDB connection.
        Args:
            enforce_connection_params (bool): Whether to enforce connection parameters
                for the MariaDB connection.
        """
        if not isinstance(enforce_connection_params, bool):
            raise ConnectionConfigurationError(
                ErrorCode.CXCE4,
                self.__class__.__name__,
                type(enforce_connection_params),
            )
        self._enforce_connection_params = enforce_connection_params

    @property
    def cx_src_configs_mariadb(self) -> dict:
        """
        dict: Source-specific configuration parameters for the MariaDB connection.
        """
        return self._cx_src_configs_mariadb

    @cx_src_configs_mariadb.setter
    def cx_src_configs_mariadb(self, cx_src_configs_mariadb: dict | None):
        """
        Set the source-specific configuration parameters for the MariaDB connection.
        Args:
            cx_src_configs_mariadb (dict | None): Source-specific configuration
            parameters for the MariaDB connection.
        """
        if cx_src_configs_mariadb is None:
            cx_src_configs_mariadb = {}
        elif not isinstance(cx_src_configs_mariadb, dict):
            raise ConnectionConfigurationError(
                ErrorCode.CXCE5,
                self.__class__.__name__,
                type(cx_src_configs_mariadb),
            )
        self._cx_src_configs_mariadb = cx_src_configs_mariadb

    @property
    def cx_dst_configs_mariadb(self) -> dict:
        """
        dict: Destination-specific configuration parameters for the MariaDB connection.
        """
        return self._cx_dst_configs_mariadb

    @cx_dst_configs_mariadb.setter
    def cx_dst_configs_mariadb(self, cx_dst_configs_mariadb: dict | None):
        """
        Set the destination-specific configuration parameters for the MariaDB
        connection.
        Args:
            cx_dst_configs_mariadb (dict | None): Destination-specific configuration
            parameters for the MariaDB connection.
        """
        if cx_dst_configs_mariadb is None:
            cx_dst_configs_mariadb = {}
        elif not isinstance(cx_dst_configs_mariadb, dict):
            raise ConnectionConfigurationError(
                ErrorCode.CXCE6,
                self.__class__.__name__,
                type(cx_dst_configs_mariadb),
            )
        self._cx_dst_configs_mariadb = cx_dst_configs_mariadb

    def __eq__(self, other):
        if not isinstance(other, MariaDBConn):
            return False
        return (
            self.uri == other.uri
            and self.credentials == other.credentials
            and self.enforce_connection_params == other.enforce_connection_params
            and self.cx_src_configs_mariadb == other.cx_src_configs_mariadb
            and self.cx_dst_configs_mariadb == other.cx_dst_configs_mariadb
        )
