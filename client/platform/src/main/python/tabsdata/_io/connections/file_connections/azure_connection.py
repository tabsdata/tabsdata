#
# Copyright 2026 Tabsdata Inc.
#

import logging
from urllib.parse import urlparse

from tabsdata._credentials import AzureAccountKeyCredentials, AzureCredentials
from tabsdata._io.connections.connections import Conn
from tabsdata._io.constants import AZURE_SCHEME
from tabsdata._utils.validation import _td_validate_call
from tabsdata.exceptions import (
    ConnectionConfigurationError,
    ErrorCode,
)

logger = logging.getLogger(__name__)


class AzureConn(Conn):
    """Azure Blob Storage connection config.

    Required: bucket_uri (str), credentials.
    Represents a connection configuration to an Azure Blob Storage container.
    """

    @_td_validate_call(ErrorCode.DECE115)
    def __init__(
        self,
        container_uri: str,
        credentials: AzureCredentials,
        enforce_connection_params: bool = True,
        cx_src_configs_azure: dict | None = None,
        cx_dst_configs_azure: dict | None = None,
    ):
        """
        Initialize an AzureConn instance.

        Args:
            container_uri: The Azure container URI in the form 'az://container_name'.
                Must not contain a path (e.g. 'az://container/prefix' is invalid).
            credentials: Credentials for authenticating with Azure Blob Storage.
            enforce_connection_params: Whether to enforce connection parameters for the
                Azure connection. Defaults to True.
            cx_src_configs_azure: Optional dictionary of additional configuration
                parameters for Azure source connections.
            cx_dst_configs_azure: Optional dictionary of additional configuration
                parameters for Azure destination connections.
        """

        self.container_uri = container_uri
        self.credentials = credentials
        self.enforce_connection_params = enforce_connection_params
        self.cx_src_configs_azure = cx_src_configs_azure or {}
        self.cx_dst_configs_azure = cx_dst_configs_azure or {}

    @property
    def container_uri(self) -> str:
        """
        The Azure container URI (e.g. 'az://mycontainer').
        """
        return self._container_uri

    @property
    def container_name(self) -> str:
        """
        The bare Azure container name without the scheme (e.g. 'mycontainer').
        """
        return self._container_name

    @container_uri.setter
    @_td_validate_call(ErrorCode.DECE115)
    def container_uri(self, container_uri: str):
        """
        Set the Azure container URI.

        Args:
            container_uri: The Azure container URI in the form 'az://container_name'.
                Must not contain a path.
        """
        parsed = urlparse(container_uri)
        if parsed.scheme.lower() != AZURE_SCHEME:
            raise ConnectionConfigurationError(
                ErrorCode.CXCE9,
                parsed.scheme,
                AZURE_SCHEME,
                container_uri,
            )
        if parsed.path and parsed.path != "/":
            raise ConnectionConfigurationError(
                ErrorCode.CXCE10,
                self.__class__.__name__,
                f"{AZURE_SCHEME}://container_name",
                container_uri,
            )
        self._container_uri = f"{AZURE_SCHEME}://{parsed.netloc}"
        self._container_name = parsed.netloc

    @property
    def credentials(self) -> AzureCredentials:
        """
        The credentials for authenticating with Azure Blob Storage.
        """
        return self._credentials

    @credentials.setter
    @_td_validate_call(ErrorCode.DECE115)
    def credentials(self, credentials: AzureCredentials):
        """
        Set the credentials for authenticating with Azure Blob Storage.

        Args:
            credentials: The credentials to set.
        """
        self._credentials = credentials

    @property
    def cx_src_configs_azure(self) -> dict:
        """
        Additional configuration parameters for Azure source connections.
        """
        return self._cx_src_configs_azure

    @cx_src_configs_azure.setter
    @_td_validate_call(ErrorCode.DECE115)
    def cx_src_configs_azure(self, cx_src_configs_azure: dict | None):
        """
        Set additional configuration parameters for Azure source connections.

        Args:
            cx_src_configs_azure: A dictionary of additional configuration parameters
                for Azure source connections.
        """
        self._cx_src_configs_azure = cx_src_configs_azure or {}

    @property
    def cx_dst_configs_azure(self) -> dict:
        """
        Additional configuration parameters for Azure destination connections.
        """
        return self._cx_dst_configs_azure

    @cx_dst_configs_azure.setter
    @_td_validate_call(ErrorCode.DECE115)
    def cx_dst_configs_azure(self, cx_dst_configs_azure: dict | None):
        """
        Set additional configuration parameters for Azure destination connections.

        Args:
            cx_dst_configs_azure: A dictionary of additional configuration parameters
                for Azure destination connections.
        """
        self._cx_dst_configs_azure = cx_dst_configs_azure or {}

    @property
    def enforce_connection_params(self) -> bool:
        """
        Whether to enforce connection parameters for the Azure connection.
        """
        return self._enforce_connection_params

    @enforce_connection_params.setter
    @_td_validate_call(ErrorCode.DECE115)
    def enforce_connection_params(self, enforce_connection_params: bool):
        """
        Set whether to enforce connection parameters for the Azure connection.

        Args:
            enforce_connection_params: Whether to enforce connection parameters for the
                Azure connection.
        """
        self._enforce_connection_params = enforce_connection_params

    def __repr__(self) -> str:
        return (
            f"AzureConn(container_uri={self.container_uri}, "
            f"credentials={self.credentials}, "
            f"cx_src_configs_azure={self.cx_src_configs_azure}, "
            f"cx_dst_configs_azure={self.cx_dst_configs_azure}, "
            f"enforce_connection_params={self.enforce_connection_params})"
        )

    @classmethod
    def _load_from_dict(cls, configs: dict) -> "AzureConn":
        credentials = configs.get("credentials")
        if isinstance(credentials, dict):
            configs = dict(configs)
            configs["credentials"] = AzureAccountKeyCredentials(**credentials)
        return cls(**configs)

    def __eq__(self, other: object) -> bool:
        if not isinstance(other, AzureConn):
            return False
        return (
            self.container_uri == other.container_uri
            and self.credentials == other.credentials
            and self.cx_src_configs_azure == other.cx_src_configs_azure
            and self.cx_dst_configs_azure == other.cx_dst_configs_azure
            and self.enforce_connection_params == other.enforce_connection_params
        )
