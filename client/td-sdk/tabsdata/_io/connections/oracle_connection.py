# Copyright 2025 Tabs Data Inc.
#

from urllib.parse import urlparse

from tabsdata._credentials import UserPasswordCredentials, build_credentials
from tabsdata._io.connections.connections import Conn
from tabsdata._io.constants import ORACLE_SCHEME
from tabsdata.exceptions import (
    ConnectionConfigurationError,
    ErrorCode,
)


class OracleConn(Conn):
    """
    Represents a connection configuration to Oracle.
    """

    def __init__(
        self,
        uri: str,
        credentials: UserPasswordCredentials | None = None,
        enforce_connection_params: bool = True,
        cx_src_configs_oracle: dict = None,
        cx_dst_configs_oracle: dict = None,
    ):
        """
        Initialize an OracleConn instance.

        Args:
            uri (str): The URI of the Oracle database.
            credentials (UserPasswordCredentials | None): Credentials for authenticating
                with the database.
            enforce_connection_params (bool, optional): Whether to enforce connection
                parameters for the Oracle connection. Defaults to True.
            cx_src_configs_oracle (dict, optional): Source-specific configuration
                parameters for the Oracle connection. Defaults to None.
            cx_dst_configs_oracle (dict, optional): Destination-specific configuration
                parameters for the Oracle connection. Defaults to None.

        Raises:
            ConnectionConfigurationError: If any connection configurations are invalid.
        """
        self.uri = uri
        self.credentials = credentials
        self.enforce_connection_params = enforce_connection_params
        self.cx_src_configs_oracle = cx_src_configs_oracle or {}
        self.cx_dst_configs_oracle = cx_dst_configs_oracle or {}

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
        if parsed_uri.scheme.lower() != ORACLE_SCHEME:
            raise ConnectionConfigurationError(
                ErrorCode.CXCE2, parsed_uri.scheme, ORACLE_SCHEME, uri
            )
        self._parsed_uri = parsed_uri
        self._uri = uri

    @property
    def credentials(self) -> UserPasswordCredentials | None:
        """
        UserPasswordCredentials | None: The credentials required to access the
            Oracle database. If no credentials were provided, it will return None.
        """
        return self._credentials

    @credentials.setter
    def credentials(self, credentials: UserPasswordCredentials | None):
        """
        Set the credentials required to access the Oracle database.
        Args:
            credentials (UserPasswordCredentials | None): The credentials required to
                access the Oracle database.
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
        bool: Whether to enforce connection parameters for the Oracle connection.
        """
        return self._enforce_connection_params

    @enforce_connection_params.setter
    def enforce_connection_params(self, enforce_connection_params: bool):
        """
        Set whether to enforce connection parameters for the Oracle connection.
        Args:
            enforce_connection_params (bool): Whether to enforce connection parameters
                for the Oracle connection.
        """
        if not isinstance(enforce_connection_params, bool):
            raise ConnectionConfigurationError(
                ErrorCode.CXCE4,
                self.__class__.__name__,
                type(enforce_connection_params),
            )
        self._enforce_connection_params = enforce_connection_params

    @property
    def cx_src_configs_oracle(self) -> dict:
        """
        dict: The source configuration parameters for the Oracle connection.
        """
        return self._cx_src_configs_oracle

    @cx_src_configs_oracle.setter
    def cx_src_configs_oracle(self, cx_src_configs_oracle: dict | None):
        """
        Set the source configuration parameters for the Oracle connection.
        Args:
            cx_src_configs_oracle (dict | None): The source configuration parameters
            for the Oracle connection.
        """
        if cx_src_configs_oracle is None:
            cx_src_configs_oracle = {}
        elif not isinstance(cx_src_configs_oracle, dict):
            raise ConnectionConfigurationError(
                ErrorCode.CXCE5,
                self.__class__.__name__,
                type(cx_src_configs_oracle),
            )
        self._cx_src_configs_oracle = cx_src_configs_oracle

    @property
    def cx_dst_configs_oracle(self) -> dict:
        """
        dict: The destination configuration parameters for the Oracle connection.
        """
        return self._cx_dst_configs_oracle

    @cx_dst_configs_oracle.setter
    def cx_dst_configs_oracle(self, cx_dst_configs_oracle: dict | None):
        """
        Set the destination configuration parameters for the Oracle connection.
        Args:
            cx_dst_configs_oracle (dict | None): The destination configuration
            parameters for the Oracle connection.
        """
        if cx_dst_configs_oracle is None:
            cx_dst_configs_oracle = {}
        elif not isinstance(cx_dst_configs_oracle, dict):
            raise ConnectionConfigurationError(
                ErrorCode.CXCE6,
                self.__class__.__name__,
                type(cx_dst_configs_oracle),
            )
        self._cx_dst_configs_oracle = cx_dst_configs_oracle

    def __eq__(self, other):
        if not isinstance(other, OracleConn):
            return False
        return (
            self.uri == other.uri
            and self.credentials == other.credentials
            and self.enforce_connection_params == other.enforce_connection_params
            and self.cx_src_configs_oracle == other.cx_src_configs_oracle
            and self.cx_dst_configs_oracle == other.cx_dst_configs_oracle
        )
