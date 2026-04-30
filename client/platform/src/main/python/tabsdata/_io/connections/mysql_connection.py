#
# Copyright 2025 Tabs Data Inc.
#

from typing import Optional
from urllib.parse import urlparse

from tabsdata._credentials import UserPasswordCredentials, build_credentials
from tabsdata._io.connections.connections import Conn
from tabsdata._io.constants import MYSQL_SCHEME
from tabsdata._utils.validation import _td_validate_call
from tabsdata.exceptions import (
    ConnectionConfigurationError,
    ErrorCode,
)


class MySQLConn(Conn):
    """MySQL connection config.

    Required: uri (str).
    Optional: credentials (UserPasswordCredentials).
    Represents a connection configuration to MySQL.
    """

    @_td_validate_call(ErrorCode.DECE101)
    def __init__(
        self,
        uri: str,
        credentials: Optional[UserPasswordCredentials] = None,
        enforce_connection_params: bool = True,
        cx_src_configs_mysql: Optional[dict] = None,
        cx_dst_configs_mysql: Optional[dict] = None,
    ):
        """
        Initialize a MySQLConn instance.

        Args:
            uri: The URI of the MySQL database.
            credentials: Credentials for authenticating with the database.
            enforce_connection_params: Whether to enforce connection parameters for the
                MySQL connection. Applicable only for a connection to a MySQL
                Destination. Defaults to True.
            cx_src_configs_mysql: Source-specific configuration parameters for the MySQL
                connection. Defaults to None.
            cx_dst_configs_mysql: Destination-specific configuration parameters for the
                MySQL connection. Defaults to None.
        """
        self._uri = self._validate_uri(uri)
        self._credentials = self._build_credentials(credentials)
        self._enforce_connection_params = enforce_connection_params
        self._cx_src_configs_mysql = cx_src_configs_mysql or {}
        self._cx_dst_configs_mysql = cx_dst_configs_mysql or {}

    @property
    def uri(self) -> str:
        """
        The URI of the database where the data is located.
        """
        return self._uri

    def _validate_uri(self, uri: str):
        """
        Validate that the provided URI is a valid MySQL URI.
        Args:
            uri: The URI of the database.
        """
        parsed_uri = urlparse(uri)
        parsed_uri_scheme = parsed_uri.scheme.lower().split("+")[0]
        if parsed_uri_scheme != MYSQL_SCHEME:
            raise ConnectionConfigurationError(
                ErrorCode.CXCE2, parsed_uri_scheme, MYSQL_SCHEME, uri
            )
        self._parsed_uri = parsed_uri
        return uri

    @property
    def credentials(self) -> Optional[UserPasswordCredentials]:
        """
        The credentials required to access the MySQL database. If no credentials were
        provided, it will return None.
        """
        return self._credentials

    def _build_credentials(
        self, credentials: Optional[UserPasswordCredentials]
    ) -> Optional[UserPasswordCredentials]:
        """
        Build the credentials required to access the MySQLDatabase.
        Args:
            credentials: The credentials required to access the MySQLDatabase.
        """
        if credentials:
            credentials = build_credentials(credentials)
        return credentials

    @property
    def enforce_connection_params(self) -> bool:
        """
        Whether to enforce connection parameters for the MySQL connection.
        """
        return self._enforce_connection_params

    @property
    def cx_src_configs_mysql(self) -> dict:
        """
        The source configuration parameters for the MySQL connection.
        """
        return self._cx_src_configs_mysql

    @property
    def cx_dst_configs_mysql(self) -> dict:
        """
        The destination configuration parameters for the MySQL connection.
        """
        return self._cx_dst_configs_mysql

    @classmethod
    def _load_from_dict(cls, configs: dict) -> "MySQLConn":
        credentials = configs.get("credentials")
        if isinstance(credentials, dict):
            configs = dict(configs)
            configs["credentials"] = UserPasswordCredentials(**credentials)
        return cls(**configs)

    def __repr__(self):
        return (
            f"{self.__class__.__name__}(uri='{self.uri}', "
            f"enforce_connection_params={self.enforce_connection_params}, "
            f"cx_src_configs_mysql={self.cx_src_configs_mysql}, "
            f"cx_dst_configs_mysql={self.cx_dst_configs_mysql})"
        )

    def __eq__(self, other):
        if not isinstance(other, MySQLConn):
            return False
        return (
            self.uri == other.uri
            and self.credentials == other.credentials
            and self.enforce_connection_params == other.enforce_connection_params
            and self.cx_src_configs_mysql == other.cx_src_configs_mysql
            and self.cx_dst_configs_mysql == other.cx_dst_configs_mysql
        )
