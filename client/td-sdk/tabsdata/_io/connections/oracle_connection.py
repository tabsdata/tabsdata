# Copyright 2025 Tabs Data Inc.
#

from typing import Optional
from urllib.parse import urlparse

from tabsdata._credentials import UserPasswordCredentials, build_credentials
from tabsdata._io.connections.connections import Conn
from tabsdata._io.constants import ORACLE_SCHEME
from tabsdata.dataquality._utils import _td_validate_call
from tabsdata.exceptions import (
    ConnectionConfigurationError,
    ErrorCode,
)


class OracleConn(Conn):
    """
    Represents a connection configuration to Oracle.
    """

    @_td_validate_call(ErrorCode.PVCX3)
    def __init__(
        self,
        uri: str,
        credentials: Optional[UserPasswordCredentials] = None,
        enforce_connection_params: bool = True,
        cx_src_configs_oracle: Optional[dict] = None,
        cx_dst_configs_oracle: Optional[dict] = None,
    ):
        """
        Initialize an OracleConn instance.

        Args:
            uri: The URI of the Oracle database.
            credentials: Credentials for authenticating with the database.
            enforce_connection_params: Whether to enforce connection parameters for the
                Oracle connection. Defaults to True.
            cx_src_configs_oracle: Source-specific configuration parameters for the
            Oracle connection. Defaults to None.
            cx_dst_configs_oracle: Destination-specific configuration parameters for the
                Oracle connection. Defaults to None.

        Raises:
            ConnectionConfigurationError: If any connection configurations are invalid.
        """
        self._uri = self._validate_uri(uri)
        self._credentials = self._build_credentials(credentials)
        self._enforce_connection_params = enforce_connection_params
        self._cx_src_configs_oracle = cx_src_configs_oracle or {}
        self._cx_dst_configs_oracle = cx_dst_configs_oracle or {}

    @property
    def uri(self) -> str:
        """
        The URI of the database where the data is located.
        """
        return self._uri

    def _validate_uri(self, uri: str):
        """
        Validate that the provided URI is a valid Oracle URI.
        Args:
            uri: The URI of the database.
        """
        parsed_uri = urlparse(uri)
        parsed_uri_scheme = parsed_uri.scheme.lower().split("+")[0]
        if parsed_uri_scheme != ORACLE_SCHEME:
            raise ConnectionConfigurationError(
                ErrorCode.CXCE2, parsed_uri_scheme, ORACLE_SCHEME, uri
            )
        self._parsed_uri = parsed_uri
        return uri

    @property
    def credentials(self) -> Optional[UserPasswordCredentials]:
        """
        The credentials required to access the Oracle database. If no credentials were
        provided, it will return None.
        """
        return self._credentials

    def _build_credentials(self, credentials: Optional[UserPasswordCredentials]):
        """
        Build the credentials required to access the Oracle database.
        Args:
            credentials: The credentials required to access the Oracle database.
        """
        if credentials:
            credentials = build_credentials(credentials)
        return credentials

    @property
    def enforce_connection_params(self) -> bool:
        """
        Whether to enforce connection parameters for the Oracle connection.
        """
        return self._enforce_connection_params

    @property
    def cx_src_configs_oracle(self) -> dict:
        """
        Source-specific configuration parameters for the Oracle connection.
        """
        return self._cx_src_configs_oracle

    @property
    def cx_dst_configs_oracle(self) -> dict:
        """
        Destination-specific configuration parameters for the Oracle connection.
        """
        return self._cx_dst_configs_oracle

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
