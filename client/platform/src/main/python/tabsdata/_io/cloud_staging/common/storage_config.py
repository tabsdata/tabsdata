#
# Copyright 2026 Tabs Data Inc.
#

from __future__ import annotations

from dataclasses import dataclass
from enum import Enum

from tabsdata._credentials import (
    AzureAccountKeyCredentials,
    GCPServiceAccountKeyCredentials,
    HMACCredentials,
    S3AccessKeyCredentials,
)


class CloudProvider(Enum):
    S3 = "s3"
    AZURE = "azure"
    GCS = "gcs"


@dataclass
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


@dataclass
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


@dataclass
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
