#
# Copyright 2026 Tabsdata Inc.
#

from __future__ import annotations

from dataclasses import dataclass
from enum import Enum
from typing import Union

from tabsdata._credentials import (
    AzureAccountKeyCredentials,
    GCPServiceAccountKeyCredentials,
    HMACCredentials,
    S3AccessKeyCredentials,
)

# noinspection PyProtectedMember
from tabsdata._utils.validation import _td_validate_call
from tabsdata.exceptions import ErrorCode

#: Type alias for the accepted cloud staging providers.
StagingConfig = Union["S3Config", "AzureConfig", "GCSConfig"]


class CloudProvider(Enum):
    S3 = "s3"
    AZURE = "azure"
    GCS = "gcs"


@dataclass(init=False)
class S3Config:
    """Configuration for staging files in Amazon S3.

    Args:
        bucket: S3 bucket name.
        region: AWS region (e.g. ``"us-east-1"``).
        base_path: Key prefix under which staged files are stored
            (e.g. ``"staging/clickhouse"``).
        credentials: AWS access-key credentials used for uploading staged
            files (via ``boto3``).
    """

    bucket: str
    region: str
    base_path: str
    credentials: S3AccessKeyCredentials

    @_td_validate_call(ErrorCode.CSC1)
    def __init__(
        self,
        bucket: str,
        region: str,
        base_path: str,
        credentials: S3AccessKeyCredentials,
    ):
        self.bucket = bucket
        self.region = region
        self.base_path = base_path
        self.credentials = credentials


@dataclass(init=False)
class AzureConfig:
    """Configuration for staging files in Azure Blob Storage.

    Args:
        container: Blob container name.
        base_path: Blob path prefix for staged files
            (e.g. ``"staging/clickhouse"``).
        credentials: Azure account-key credentials used for uploading
            staged files (via ``azure-storage-blob``).
    """

    container: str
    base_path: str
    credentials: AzureAccountKeyCredentials

    @_td_validate_call(ErrorCode.CSC2)
    def __init__(
        self,
        container: str,
        base_path: str,
        credentials: AzureAccountKeyCredentials,
    ):
        self.container = container
        self.base_path = base_path
        self.credentials = credentials


@dataclass(init=False)
class GCSConfig:
    """Configuration for staging files in Google Cloud Storage.

    Args:
        bucket: GCS bucket name.
        base_path: Object path prefix for staged files
            (e.g. ``"staging/clickhouse"``).
        upload_credentials: Optional GCP service-account credentials used for
            uploading staged files via the GCS SDK. When ``None``, application
            default credentials (ADC) are used.
        hmac_credentials: Optional :class:`~tabsdata.HMACCredentials`
            carrying GCS HMAC access key and secret for S3-compatible access.
            When ``None``, the storage backend operates without explicit
            credentials (suitable for public buckets or server-side IAM).
    """

    bucket: str
    base_path: str
    upload_credentials: GCPServiceAccountKeyCredentials | None = None
    hmac_credentials: HMACCredentials | None = None

    @_td_validate_call(ErrorCode.CSC3)
    def __init__(
        self,
        bucket: str,
        base_path: str,
        upload_credentials: GCPServiceAccountKeyCredentials | None = None,
        hmac_credentials: HMACCredentials | None = None,
    ):
        self.bucket = bucket
        self.base_path = base_path
        self.upload_credentials = upload_credentials
        self.hmac_credentials = hmac_credentials


def build_staging_from_dict(staging_dict: dict) -> StagingConfig:
    """
    Build an S3Config, AzureConfig, or GCSConfig from a plain dictionary.

    The dictionary must contain a ``"provider"`` key with one of ``"s3"``,
    ``"azure"``, or ``"gcs"``.  The remaining keys are passed to the
    corresponding config class constructor after converting any nested
    credential dicts into their proper credential objects.

    Args:
        staging_dict: A dictionary with a ``"provider"`` key and the
            remaining keys matching the config class constructor.
    Returns:
        An S3Config, AzureConfig, or GCSConfig instance.
    """
    staging_dict = dict(staging_dict)
    provider = staging_dict.pop("provider", None)
    if provider is None:
        raise ValueError(
            "staging dict must contain a 'provider' key ('s3', 'azure', or 'gcs')"
        )

    if provider == "s3":
        credentials = staging_dict.get("credentials")
        if isinstance(credentials, dict):
            staging_dict["credentials"] = S3AccessKeyCredentials(**credentials)
        return S3Config(**staging_dict)
    elif provider == "azure":
        credentials = staging_dict.get("credentials")
        if isinstance(credentials, dict):
            staging_dict["credentials"] = AzureAccountKeyCredentials(**credentials)
        return AzureConfig(**staging_dict)
    elif provider == "gcs":
        upload_creds = staging_dict.get("upload_credentials")
        if isinstance(upload_creds, dict):
            staging_dict["upload_credentials"] = GCPServiceAccountKeyCredentials(
                **upload_creds
            )
        hmac_creds = staging_dict.get("hmac_credentials")
        if isinstance(hmac_creds, dict):
            staging_dict["hmac_credentials"] = HMACCredentials(**hmac_creds)
        return GCSConfig(**staging_dict)
    else:
        raise ValueError(
            f"Unknown staging provider: '{provider}'. Expected 's3', 'azure', or 'gcs'."
        )
