#
# Copyright 2026 Tabs Data Inc.
#

from __future__ import annotations

import logging
from urllib.parse import urlparse

from tabsdata._credentials import GCPCredentials, GCPServiceAccountKeyCredentials
from tabsdata._io.connections.connections import Conn
from tabsdata._io.constants import GCS_SCHEME
from tabsdata._utils.validation import _td_validate_call
from tabsdata.exceptions import (
    ConnectionConfigurationError,
    ErrorCode,
)

logger = logging.getLogger(__name__)


class GCSConn(Conn):
    """GCS connection config.

    Required: bucket_uri (str), credentials.
    Represents a connection configuration to a GCS storage bucket.
    """

    @_td_validate_call(ErrorCode.DECE113)
    def __init__(
        self,
        bucket_uri: str,
        credentials: GCPCredentials,
        enforce_connection_params: bool = True,
        cx_src_configs_gcs: dict | None = None,
        cx_dst_configs_gcs: dict | None = None,
    ):
        """
        Initialize a GCSConn instance.

        Args:
            bucket_uri: The GCS bucket URI in the form 'gs://bucket_name'.
                Must not contain a path (e.g. 'gs://bucket/prefix' is invalid).
            credentials: Credentials for authenticating with the GCS storage bucket.
            enforce_connection_params: Whether to enforce connection parameters for the
                GCS connection. Defaults to True.
            cx_src_configs_gcs: Optional dictionary of additional configuration
                parameters for GCS source connections.
            cx_dst_configs_gcs: Optional dictionary of additional configuration
                parameters for GCS destination connections.
        """

        self.bucket_uri = bucket_uri
        self.credentials = credentials
        self.enforce_connection_params = enforce_connection_params
        self.cx_src_configs_gcs = cx_src_configs_gcs or {}
        self.cx_dst_configs_gcs = cx_dst_configs_gcs or {}

    @property
    def bucket_uri(self) -> str:
        """
        The GCS bucket URI (e.g. 'gs://mybucket').
        """
        return self._bucket_uri

    @property
    def bucket_name(self) -> str:
        """
        The bare GCS bucket name without the scheme (e.g. 'mybucket').
        """
        return self._bucket_name

    @bucket_uri.setter
    @_td_validate_call(ErrorCode.DECE113)
    def bucket_uri(self, bucket_uri: str):
        """
        Set the GCS bucket URI.

        Args:
            bucket_uri: The GCS bucket URI in the form 'gs://bucket_name'.
                Must not contain a path.
        """
        parsed = urlparse(bucket_uri)
        if parsed.scheme.lower() != GCS_SCHEME:
            raise ConnectionConfigurationError(
                ErrorCode.CXCE9,
                parsed.scheme,
                GCS_SCHEME,
                bucket_uri,
            )
        if parsed.path and parsed.path != "/":
            raise ConnectionConfigurationError(
                ErrorCode.CXCE10,
                self.__class__.__name__,
                f"{GCS_SCHEME}://bucket_name",
                bucket_uri,
            )
        self._bucket_uri = f"{GCS_SCHEME}://{parsed.netloc}"
        self._bucket_name = parsed.netloc

    @property
    def credentials(self) -> GCPCredentials:
        """
        The credentials for authenticating with the GCS storage bucket.
        """
        return self._credentials

    @credentials.setter
    @_td_validate_call(ErrorCode.DECE113)
    def credentials(self, credentials: GCPCredentials):
        """
        Set the credentials for authenticating with the GCS storage bucket.

        Args:
            credentials: The credentials to set.
        """
        self._credentials = credentials

    @property
    def cx_src_configs_gcs(self) -> dict:
        """
        Additional configuration parameters for GCS source connections.
        """
        return self._cx_src_configs_gcs

    @cx_src_configs_gcs.setter
    @_td_validate_call(ErrorCode.DECE113)
    def cx_src_configs_gcs(self, cx_src_configs_gcs: dict | None):
        """
        Set additional configuration parameters for GCS source connections.

        Args:
            cx_src_configs_gcs: A dictionary of additional configuration parameters
                for GCS source connections.
        """
        self._cx_src_configs_gcs = cx_src_configs_gcs or {}

    @property
    def cx_dst_configs_gcs(self) -> dict:
        """
        Additional configuration parameters for GCS destination connections.
        """
        return self._cx_dst_configs_gcs

    @cx_dst_configs_gcs.setter
    @_td_validate_call(ErrorCode.DECE113)
    def cx_dst_configs_gcs(self, cx_dst_configs_gcs: dict | None):
        """
        Set additional configuration parameters for GCS destination connections.

        Args:
            cx_dst_configs_gcs: A dictionary of additional configuration parameters
                for GCS destination connections.
        """
        self._cx_dst_configs_gcs = cx_dst_configs_gcs or {}

    @property
    def enforce_connection_params(self) -> bool:
        """
        Whether to enforce connection parameters for the GCS connection.
        """
        return self._enforce_connection_params

    @enforce_connection_params.setter
    @_td_validate_call(ErrorCode.DECE113)
    def enforce_connection_params(self, enforce_connection_params: bool):
        """
        Set whether to enforce connection parameters for the GCS connection.

        Args:
            enforce_connection_params: Whether to enforce connection parameters for the
                GCS connection.
        """
        self._enforce_connection_params = enforce_connection_params

    def __repr__(self) -> str:
        return (
            f"GCSConn(bucket_uri={self.bucket_uri}, "
            f"credentials={self.credentials}, "
            f"cx_src_configs_gcs={self.cx_src_configs_gcs}, "
            f"cx_dst_configs_gcs={self.cx_dst_configs_gcs}, "
            f"enforce_connection_params={self.enforce_connection_params})"
        )

    @classmethod
    def _load_from_dict(cls, configs: dict) -> "GCSConn":
        credentials = configs.get("credentials")
        if isinstance(credentials, dict):
            configs = dict(configs)
            configs["credentials"] = GCPServiceAccountKeyCredentials(**credentials)
        return cls(**configs)

    def __eq__(self, other: object) -> bool:
        if not isinstance(other, GCSConn):
            return False
        return (
            self.bucket_uri == other.bucket_uri
            and self.credentials == other.credentials
            and self.cx_src_configs_gcs == other.cx_src_configs_gcs
            and self.cx_dst_configs_gcs == other.cx_dst_configs_gcs
            and self.enforce_connection_params == other.enforce_connection_params
        )
