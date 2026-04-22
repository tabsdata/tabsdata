#
# Copyright 2026 Tabs Data Inc.
#

import logging
from typing import TypeAlias
from urllib.parse import urlparse

from tabsdata._credentials import S3Credentials
from tabsdata._io.connections.connections import Conn
from tabsdata._io.constants import S3_SCHEME, SupportedAWSS3Regions
from tabsdata._utils.validation import _td_validate_call
from tabsdata.exceptions import (
    ConnectionConfigurationError,
    ErrorCode,
)

logger = logging.getLogger(__name__)

S3ConnUriSpec: TypeAlias = str | list[str]
S3ConnCredentialsSpec: TypeAlias = S3Credentials


class S3Conn(Conn):
    """
    Represents a connection configuration to an S3 bucket.
    """

    @_td_validate_call(ErrorCode.DECE110)
    def __init__(
        self,
        bucket_uri: str,
        credentials: S3ConnCredentialsSpec,
        region: str | None = None,
        enforce_connection_params: bool = True,
        cx_src_configs_s3: dict | None = None,
        cx_dst_configs_s3: dict | None = None,
    ):
        """
        Initialize an S3Conn instance.

        Args:
            bucket_uri: The S3 bucket URI in the form 's3://bucket_name'.
                Must not contain a path (e.g. 's3://bucket/prefix' is invalid).
            credentials: Credentials for authenticating with the S3 bucket.
            region: The AWS region where the S3 bucket is located.
            enforce_connection_params: Whether to enforce connection parameters for the
                S3 connection. Defaults to True.
            cx_src_configs_s3: Optional dictionary of additional configuration
            parameters
                for S3 source connections.
            cx_dst_configs_s3: Optional dictionary of additional configuration
            parameters
                for S3 destination connections.
        """

        self.bucket_uri = bucket_uri
        self.credentials = credentials
        self.region = region
        self.cx_src_configs_s3 = cx_src_configs_s3 or {}
        self.cx_dst_configs_s3 = cx_dst_configs_s3 or {}
        self.enforce_connection_params = enforce_connection_params

    @property
    def bucket_uri(self) -> str:
        """
        The S3 bucket URI (e.g. 's3://mybucket').
        """
        return self._bucket_uri

    @property
    def bucket_name(self) -> str:
        """
        The bare S3 bucket name without the scheme (e.g. 'mybucket').
        """
        return self._bucket_name

    @bucket_uri.setter
    @_td_validate_call(ErrorCode.DECE110)
    def bucket_uri(self, bucket_uri: str):
        """
        Set the S3 bucket URI.

        Args:
            bucket_uri: The S3 bucket URI in the form 's3://bucket_name'.
                Must not contain a path.
        """
        parsed = urlparse(bucket_uri)
        if parsed.scheme.lower() != S3_SCHEME:
            raise ConnectionConfigurationError(
                ErrorCode.CXCE9,
                parsed.scheme,
                S3_SCHEME,
                bucket_uri,
            )
        # parsed.netloc is the bucket name; parsed.path should be empty or just "/"
        if parsed.path and parsed.path != "/":
            raise ConnectionConfigurationError(
                ErrorCode.CXCE10,
                self.__class__.__name__,
                f"{S3_SCHEME}://bucket_name",
                bucket_uri,
            )
        self._bucket_uri = f"{S3_SCHEME}://{parsed.netloc}"
        self._bucket_name = parsed.netloc

    @property
    def credentials(self) -> S3Credentials | None:
        """
        The credentials for authenticating with the S3 bucket.
        """
        return self._credentials

    @credentials.setter
    @_td_validate_call(ErrorCode.DECE110)
    def credentials(self, credentials: S3ConnCredentialsSpec):
        """
        Set the credentials for authenticating with the S3 bucket.

        Args:
            credentials: The credentials to set
        """
        self._credentials = credentials

    @property
    def region(self) -> str | None:
        """
        The AWS region where the S3 bucket is located.
        """
        return self._region

    def _validate_region(self, region: str | None) -> str | None:
        """
        Validate the AWS region.

        Args:
            region: The AWS region to validate.
        Returns:
            The validated AWS region.
        """
        if not region:
            return None

        supported_regions = [element.value for element in SupportedAWSS3Regions]
        if region not in supported_regions:
            logger.warning(
                "The 'region' parameter for the S3Conn object has value "
                f"'{region}', which is not recognized in our current list of AWS "
                f"regions: {supported_regions}. This could indicate a typo in the "
                "region provided, but it could also occur because you are "
                "using a recently created AWS region or a private AWS region. "
                "You can continue using this region if you are sure it is available"
                " for your AWS account, but if it isn't it will cause an error "
                "during runtime."
            )
        return region

    @region.setter
    @_td_validate_call(ErrorCode.DECE110)
    def region(self, region: str | None):
        """
        Set the AWS region where the S3 bucket is located.

        Args:
            region: The AWS region to set.
        """
        self._region = self._validate_region(region)

    @property
    def cx_src_configs_s3(self) -> dict:
        """
        Additional configuration parameters for S3 source connections.
        """
        return self._cx_src_configs_s3

    @cx_src_configs_s3.setter
    @_td_validate_call(ErrorCode.DECE110)
    def cx_src_configs_s3(self, cx_src_configs_s3: dict | None):
        """
        Set additional configuration parameters for S3 source connections.

        Args:
            cx_src_configs_s3: A dictionary of additional configuration parameters
            for S3 source connections.
        """
        self._cx_src_configs_s3 = cx_src_configs_s3 or {}

    @property
    def cx_dst_configs_s3(self) -> dict:
        """
        Additional configuration parameters for S3 destination connections.
        """
        return self._cx_dst_configs_s3

    @cx_dst_configs_s3.setter
    @_td_validate_call(ErrorCode.DECE110)
    def cx_dst_configs_s3(self, cx_dst_configs_s3: dict | None):
        """
        Set additional configuration parameters for S3 destination connections.

        Args:
            cx_dst_configs_s3: A dictionary of additional configuration parameters
            for S3 destination connections.
        """
        self._cx_dst_configs_s3 = cx_dst_configs_s3 or {}

    @property
    def enforce_connection_params(self) -> bool:
        """
        Whether to enforce connection parameters for the S3 connection.
        """
        return self._enforce_connection_params

    @enforce_connection_params.setter
    @_td_validate_call(ErrorCode.DECE110)
    def enforce_connection_params(self, enforce_connection_params: bool):
        """
        Set whether to enforce connection parameters for the S3 connection.

        Args:
            enforce_connection_params: Whether to enforce connection parameters for the
                S3 connection.
        """
        self._enforce_connection_params = enforce_connection_params

    def __eq__(self, other):
        if not isinstance(other, S3Conn):
            return False
        return (
            self.bucket_uri == other.bucket_uri
            and self.credentials == other.credentials
            and self.region == other.region
            and self.cx_src_configs_s3 == other.cx_src_configs_s3
            and self.cx_dst_configs_s3 == other.cx_dst_configs_s3
            and self.enforce_connection_params == other.enforce_connection_params
        )

    def __repr__(self):
        return (
            f"S3Conn(bucket_uri={self.bucket_uri}, "
            f"credentials={self.credentials}, "
            f"region={self.region}, "
            f"cx_src_configs_s3={self.cx_src_configs_s3}, "
            f"cx_dst_configs_s3={self.cx_dst_configs_s3}, "
            f"enforce_connection_params={self.enforce_connection_params})"
        )
