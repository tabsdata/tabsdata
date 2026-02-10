#
# Copyright 2025 Tabs Data Inc.
#

from __future__ import annotations

import logging
from typing import Optional
from urllib.parse import quote_plus, urlparse

from tabsdata._credentials import UserPasswordCredentials, build_credentials
from tabsdata._io.connections.connections import Conn
from tabsdata._io.constants import MSSQL_SCHEME, ODBC_CONNECT_PARAM
from tabsdata._utils.validation import _td_validate_call
from tabsdata.exceptions import ConnectionConfigurationError, ErrorCode

logger = logging.getLogger(__name__)
logger.setLevel(logging.DEBUG)


class MSSQLConn(Conn):
    """
    Represents a connection configuration to Microsoft SQL Server (MSSQL).
    """

    @_td_validate_call(ErrorCode.DECE104)
    def __init__(
        self,
        connection_string_or_uri: str,
        credentials: Optional[UserPasswordCredentials] = None,
        enforce_connection_params: bool = True,
        cx_src_configs_mssql: Optional[dict] = None,
        cx_dst_configs_mssql: Optional[dict] = None,
    ):
        """
        Initialize a MSSQLConn instance.

        Args:
            connection_string_or_uri: The connection string or URI for the MSSQL
                database.
            enforce_connection_params: Whether to enforce connection parameters for the
                MSSQL connection. Applicable only for a connection to a MSSQL
                Destination. Defaults to True.
            cx_src_configs_mssql: Source-specific configuration parameters for the MSSQL
                connection. Defaults to None.
            cx_dst_configs_mssql: Destination-specific configuration parameters for the
                MSSQL connection. Defaults to None.
        """
        self._connection_string_or_uri = self._validate_connection_string(
            connection_string_or_uri
        )
        self._credentials = self._build_credentials(credentials)
        self._enforce_connection_params = enforce_connection_params
        self._cx_src_configs_mssql = cx_src_configs_mssql or {}
        self._cx_dst_configs_mssql = cx_dst_configs_mssql or {}

    @property
    def connection_string_or_uri(self) -> str:
        """
        The connection string or URI for the MSSQL database.
        """
        return self._connection_string_or_uri

    @property
    def uri(self) -> str:
        """
        The URI for the MSSQL database.
        """
        return self._obtain_uri()

    @property
    def credentials(self) -> Optional[UserPasswordCredentials]:
        """
        The credentials used to authenticate with the database.
        """
        return self._credentials

    def _build_credentials(self, credentials: Optional[UserPasswordCredentials]):
        """
        Build the credentials required to access the MSSQL database.
        Args:
            credentials: The credentials required to access the MSSQL database.
        """
        if credentials:
            credentials = build_credentials(credentials)
        return credentials

    @property
    def enforce_connection_params(self) -> bool:
        """
        Whether to enforce connection parameters for the MSSQL connection.
        Applicable only for a connection to a MSSQL Destination.
        """
        return self._enforce_connection_params

    @property
    def cx_src_configs_mssql(self) -> dict:
        """
        The source configuration parameters for the MSSQL connection.
        """
        return self._cx_src_configs_mssql

    @property
    def cx_dst_configs_mssql(self) -> dict:
        """
        The destination configuration parameters for the MSSQL connection.
        """
        return self._cx_dst_configs_mssql

    def _validate_connection_string(self, connection_string: str):
        parsed_connection_string = urlparse(connection_string)
        parsed_uri_scheme = parsed_connection_string.scheme.lower().split("+")[0]
        if (
            parsed_uri_scheme != MSSQL_SCHEME
            and not parsed_connection_string.path.lower().startswith("driver=")
        ):
            raise ConnectionConfigurationError(
                ErrorCode.CXCE8, self.__class__.__name__, connection_string
            )
        return connection_string

    def _obtain_uri(self) -> str:
        if self.connection_string_or_uri.startswith(MSSQL_SCHEME):
            return self.connection_string_or_uri
        else:
            connection_string = self.connection_string_or_uri
            if not connection_string.endswith(";"):
                connection_string += ";"
            return (
                f"{MSSQL_SCHEME}:///?{ODBC_CONNECT_PARAM}="
                f"{quote_plus(connection_string)}"
            )

    def __eq__(self, other) -> bool:
        if not isinstance(other, MSSQLConn):
            return False
        return (
            self.connection_string_or_uri == other.connection_string_or_uri
            and self.credentials == other.credentials
            and self.enforce_connection_params == other.enforce_connection_params
            and self.cx_src_configs_mssql == other.cx_src_configs_mssql
            and self.cx_dst_configs_mssql == other.cx_dst_configs_mssql
        )
