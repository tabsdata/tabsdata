#
# Copyright 2025 Tabs Data Inc.
#
from typing import Optional
from urllib.parse import urlparse

from tabsdata._credentials import UserPasswordCredentials, build_credentials
from tabsdata._io.connections.connections import Conn
from tabsdata._io.constants import MARIADB_SCHEME
from tabsdata._tabsserver.function.sql_utils import add_mariadb_collation
from tabsdata._utils.validation import _td_validate_call
from tabsdata.exceptions import (
    ConnectionConfigurationError,
    ErrorCode,
)


class MariaDBConn(Conn):
    """
    Represents a connection configuration to MariaDB.
    """

    @_td_validate_call(ErrorCode.DECE100)
    def __init__(
        self,
        uri: str,
        credentials: Optional[UserPasswordCredentials] = None,
        enforce_connection_params: bool = True,
        cx_src_configs_mariadb: Optional[dict] = None,
        cx_dst_configs_mariadb: Optional[dict] = None,
    ):
        """
        Initialize a MariaDBConn instance.

        Args:
            uri: The URI of the MariaDB database.
            credentials: Credentials for authenticating with the database.
            enforce_connection_params: Whether to enforce connection parameters for the
                MariaDB connection. Applicable only for a connection to a MariaDB
                Destination. Defaults to True.
            cx_src_configs_mariadb: Source-specific configuration parameters for the
                MariaDB connection. Defaults to None.
            cx_dst_configs_mariadb: Destination-specific configuration parameters for
                the MariaDB connection. Defaults to None.
        """
        self._uri = self._validate_uri(uri)
        self._credentials = self._build_credentials(credentials)
        self._enforce_connection_params = enforce_connection_params
        self._cx_src_configs_mariadb = cx_src_configs_mariadb or {}
        self._cx_dst_configs_mariadb = cx_dst_configs_mariadb or {}

    @property
    def uri(self) -> str:
        """
        The URI of the database where the data is located.
        """
        return self._uri

    def _validate_uri(self, uri: str):
        """
        Validate that the provided URI is a valid MariaDB URI.
        Args:
            uri: The URI of the database.
        """
        parsed_uri = urlparse(uri)
        parsed_uri_scheme = parsed_uri.scheme.lower().split("+")[0]
        if parsed_uri_scheme != MARIADB_SCHEME:
            raise ConnectionConfigurationError(
                ErrorCode.CXCE2, parsed_uri_scheme, MARIADB_SCHEME, uri
            )
        self._parsed_uri = parsed_uri
        self._uri_with_collation = add_mariadb_collation(uri)
        return uri

    @property
    def credentials(self) -> Optional[UserPasswordCredentials]:
        """
        The credentials required to access the MariaDB database. If no credentials were
        provided, it will return None.
        """
        return self._credentials

    def _build_credentials(self, credentials: Optional[UserPasswordCredentials]):
        """
        Build the credentials required to access the MariaDB database.
        Args:
            credentials: The credentials required to access the MariaDB database.
        """
        if credentials:
            credentials = build_credentials(credentials)
        return credentials

    @property
    def enforce_connection_params(self) -> bool:
        """
        Whether to enforce connection parameters for the MariaDB connection.
        """
        return self._enforce_connection_params

    @property
    def cx_src_configs_mariadb(self) -> dict:
        """
        Source-specific configuration parameters for the MariaDB connection.
        """
        return self._cx_src_configs_mariadb

    @property
    def cx_dst_configs_mariadb(self) -> dict:
        """
        Destination-specific configuration parameters for the MariaDB connection.
        """
        return self._cx_dst_configs_mariadb

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

    def __repr__(self):
        return (
            f"{self.__class__.__name__}(uri='{self.uri}', "
            f"enforce_connection_params={self.enforce_connection_params}, "
            f"cx_src_configs_mariadb={self.cx_src_configs_mariadb}, "
            f"cx_dst_configs_mariadb={self.cx_dst_configs_mariadb})"
        )
