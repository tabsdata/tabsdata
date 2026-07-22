#
# Copyright 2025 Tabsdata Inc.
#

from typing import Optional
from urllib.parse import urlparse

from tabsdata._credentials import UserPasswordCredentials, build_credentials
from tabsdata._io.connections.connections import Conn
from tabsdata._io.constants import POSTGRES_SCHEMES
from tabsdata._utils.validation import _td_validate_call
from tabsdata.exceptions import (
    ConnectionConfigurationError,
    ErrorCode,
)


class PostgresConn(Conn):
    """PostgreSQL connection config.

    Required: uri (str).
    Optional: credentials (UserPasswordCredentials).
    Represents a connection configuration to Postgres.
    """

    @_td_validate_call(ErrorCode.DECE103)
    def __init__(
        self,
        uri: str,
        credentials: Optional[UserPasswordCredentials] = None,
        enforce_connection_params: bool = True,
        cx_src_configs_postgres: Optional[dict] = None,
        cx_dst_configs_postgres: Optional[dict] = None,
    ):
        """
        Initialize a PostgresConn instance.

        Args:
            uri: The URI of the Postgres database.
            credentials: Credentials for authenticating with the database.
            enforce_connection_params: Whether to enforce connection parameters for the
                Postgres connection. Applicable only for a connection to a Postgres
                Destination. Defaults to True.
            cx_src_configs_postgres: Source-specific configuration parameters for the
                Postgres connection. Defaults to None.
            cx_dst_configs_postgres: Destination-specific configuration parameters for
                the Postgres connection. Defaults to None.

        Raises:
            ConnectionConfigurationError: If any connection configurations are invalid.
        """
        self._uri = self._validate_uri(uri)
        self._credentials = self._build_credentials(credentials)
        self._enforce_connection_params = enforce_connection_params
        self._cx_src_configs_postgres = cx_src_configs_postgres or {}
        self._cx_dst_configs_postgres = cx_dst_configs_postgres or {}

    @property
    def uri(self) -> str:
        """
        The URI of the database where the data is located.
        """
        return self._uri

    def _validate_uri(self, uri: str):
        """
        Validate that the provided URI is a valid Postgres URI.
        Args:
            uri: The URI of the database.
        """
        parsed_uri = urlparse(uri)
        parsed_uri_scheme = parsed_uri.scheme.lower().split("+")[0]
        if parsed_uri_scheme not in POSTGRES_SCHEMES:
            raise ConnectionConfigurationError(
                ErrorCode.CXCE2, parsed_uri_scheme, POSTGRES_SCHEMES, uri
            )
        self._parsed_uri = parsed_uri
        return uri

    @property
    def credentials(self) -> Optional[UserPasswordCredentials]:
        """
        The credentials required to access the Postgres database. If no credentials were
        provided, it will return None.
        """
        return self._credentials

    def _build_credentials(self, credentials: Optional[UserPasswordCredentials]):
        """
        Build the credentials required to access the Postgres database.
        Args:
            credentials: The credentials required to access the Postgres database.
        """
        if credentials:
            credentials = build_credentials(credentials)
        return credentials

    @property
    def enforce_connection_params(self) -> bool:
        """
        Whether to enforce connection parameters for the Postgres connection.
        """
        return self._enforce_connection_params

    @property
    def cx_src_configs_postgres(self) -> dict:
        """
        Source-specific configuration parameters for the Postgres connection.
        """
        return self._cx_src_configs_postgres

    @property
    def cx_dst_configs_postgres(self) -> dict:
        """
        Destination-specific configuration parameters for the Postgres connection.
        """
        return self._cx_dst_configs_postgres

    @classmethod
    def _load_from_dict(cls, configs: dict) -> "PostgresConn":
        credentials = configs.get("credentials")
        if isinstance(credentials, dict):
            configs = dict(configs)
            configs["credentials"] = UserPasswordCredentials(**credentials)
        return cls(**configs)

    def __eq__(self, other):
        if not isinstance(other, PostgresConn):
            return False
        return (
            self.uri == other.uri
            and self.credentials == other.credentials
            and self.enforce_connection_params == other.enforce_connection_params
            and self.cx_src_configs_postgres == other.cx_src_configs_postgres
            and self.cx_dst_configs_postgres == other.cx_dst_configs_postgres
        )

    def __repr__(self) -> str:
        return (
            f"{self.__class__.__name__}(uri='{self.uri}', "
            f"enforce_connection_params={self.enforce_connection_params}, "
            f"cx_src_configs_postgres={self.cx_src_configs_postgres}, "
            f"cx_dst_configs_postgres={self.cx_dst_configs_postgres})"
        )
