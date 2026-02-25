#
# Copyright 2026 Tabs Data Inc.
#

from typing import Optional
from urllib.parse import urlparse

from typing_extensions import deprecated

from tabsdata._credentials import UserPasswordCredentials, build_credentials
from tabsdata._io.connections.connections import Conn
from tabsdata._io.constants import DB2_SCHEME
from tabsdata._utils.annotations import unstable, unstable_category
from tabsdata._utils.validation import _td_validate_call
from tabsdata.exceptions import (
    ConnectionConfigurationError,
    ErrorCode,
)


@unstable()
@deprecated(
    """Class 'Db2Conn' is considered unstable and is subject to change in future
    releases.""",  # noqa: E231,E241,E501
    category=unstable_category(
        reason=(
            "Class 'Db2Conn' may undergo API and feature changes as it evolves"
            " toward full capability and stability."
        ),
        since="1.7.1",
        replacement="There is no current stable replacement for this class.",
    ),
)
class Db2Conn(Conn):
    """
    Represents a connection configuration to Db2.
    """

    @_td_validate_call(ErrorCode.DECE108)
    def __init__(
        self,
        uri: str,
        credentials: Optional[UserPasswordCredentials] = None,
        enforce_connection_params: bool = True,
        cx_src_configs_db2: Optional[dict] = None,
        cx_dst_configs_db2: Optional[dict] = None,
    ):
        """
        Initialize a Db2Conn instance.

        Args:
            uri: The URI of the Db2 database.
            credentials: Credentials for authenticating with the database.
            enforce_connection_params: Whether to enforce connection parameters for the
                Db2 connection. Applicable only for a connection to a Db2
                Destination. Defaults to True.
            cx_src_configs_db2: Source-specific configuration parameters for the
                Db2 connection. Defaults to None.
            cx_dst_configs_db2: Destination-specific configuration parameters for
                the Db2 connection. Defaults to None.

        Raises:
            ConnectionConfigurationError: If any connection configurations are invalid.
        """
        self._uri = self._validate_uri(uri)
        self._credentials = self._build_credentials(credentials)
        self._enforce_connection_params = enforce_connection_params
        self._cx_src_configs_db2 = cx_src_configs_db2 or {}
        self._cx_dst_configs_db2 = cx_dst_configs_db2 or {}

    @property
    def uri(self) -> str:
        """
        The URI of the database where the data is located.
        """
        return self._uri

    def _validate_uri(self, uri: str):
        """
        Validate that the provided URI is a valid Db2 URI.
        Args:
            uri: The URI of the database.
        """
        parsed_uri = urlparse(uri)
        parsed_uri_scheme = parsed_uri.scheme.lower().split("+")[0]
        if parsed_uri_scheme != DB2_SCHEME:
            raise ConnectionConfigurationError(
                ErrorCode.CXCE9, parsed_uri_scheme, DB2_SCHEME, uri
            )
        self._parsed_uri = parsed_uri
        return uri

    @property
    def credentials(self) -> Optional[UserPasswordCredentials]:
        """
        The credentials required to access the Db2 database. If no credentials were
        provided, it will return None.
        """
        return self._credentials

    def _build_credentials(self, credentials: Optional[UserPasswordCredentials]):
        """
        Build the credentials required to access the Db2 database.
        Args:
            credentials: The credentials required to access the Db2 database.
        """
        if credentials:
            credentials = build_credentials(credentials)
        return credentials

    @property
    def enforce_connection_params(self) -> bool:
        """
        Whether to enforce connection parameters for the Db2 connection.
        """
        return self._enforce_connection_params

    @property
    def cx_src_configs_db2(self) -> dict:
        """
        Source-specific configuration parameters for the Db2 connection.
        """
        return self._cx_src_configs_db2

    @property
    def cx_dst_configs_db2(self) -> dict:
        """
        Destination-specific configuration parameters for the Db2 connection.
        """
        return self._cx_dst_configs_db2

    def __eq__(self, other):
        if not isinstance(other, Db2Conn):
            return False
        return (
            self.uri == other.uri
            and self.credentials == other.credentials
            and self.enforce_connection_params == other.enforce_connection_params
            and self.cx_src_configs_db2 == other.cx_src_configs_db2
            and self.cx_dst_configs_db2 == other.cx_dst_configs_db2
        )

    def __repr__(self) -> str:
        return (
            f"{self.__class__.__name__}(uri='{self.uri}', "
            f"enforce_connection_params={self.enforce_connection_params}, "
            f"cx_src_configs_db2={self.cx_src_configs_db2}, "
            f"cx_dst_configs_db2={self.cx_dst_configs_db2})"
        )
