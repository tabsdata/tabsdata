#
# Copyright 2026 Tabsdata Inc.
#

from __future__ import annotations

import logging
from abc import ABC, abstractmethod
from pathlib import Path

from tabsdata._io.cloud_staging.common.storage_config import (
    AzureConfig,
    GCSConfig,
    S3Config,
)

logger = logging.getLogger(__name__)


class CloudStorage(ABC):
    """Abstract interface for cloud storage operations.

    Implementations may return physical URIs (S3, Azure, GCS) or logical paths
    (e.g. Snowflake stage references). The dialect must understand both forms.
    """

    @abstractmethod
    def upload(self, upload_id: str, local_paths: list[Path]) -> list[str]:
        """Upload files to storage. Returns a list of storage URIs (or logical paths),
        one per input file, in the same order as local_paths."""

    @abstractmethod
    def delete(self, up_to_id: str) -> None:
        """Delete all staged files with upload_id <= up_to_id.
        Uses lexicographic ordering of base32hex(UUID v7) IDs."""


class S3Storage(CloudStorage):
    """Stages Parquet files in Amazon S3.

    Requires ``boto3`` (``pip install boto3``).

    Upload path layout::

        s3://<bucket>/<base_path>/<upload_id>/<filename>

    Returned staged file refs are HTTPS URLs of the form::

        https://<bucket>.s3.<region>.amazonaws.com/<base_path>/<upload_id>/<filename>

    Args:
        config: S3 bucket and credential configuration.
    """

    def __init__(self, config: S3Config) -> None:
        self._config = config

    def upload(self, upload_id: str, local_paths: list[Path]) -> list[str]:
        try:
            import boto3
        except ImportError as e:
            raise ImportError(
                "boto3 is required for S3 cloud storage. "
                "Install it with: pip install boto3"
            ) from e

        creds = self._config.credentials
        s3 = boto3.client(
            "s3",
            region_name=self._config.region,
            aws_access_key_id=creds.aws_access_key_id.secret_value,
            aws_secret_access_key=creds.aws_secret_access_key.secret_value,
        )
        uris = []
        for path in local_paths:
            key = f"{self._config.base_path.rstrip('/')}/{upload_id}/{path.name}"
            logger.debug("Uploading %s to s3://%s/%s", path, self._config.bucket, key)
            s3.upload_file(str(path), self._config.bucket, key)
            uri = (
                f"https://{self._config.bucket}"
                f".s3.{self._config.region}.amazonaws.com/{key}"
            )
            uris.append(uri)
        return uris

    def delete(self, up_to_id: str) -> None:
        try:
            import boto3
        except ImportError as e:
            raise ImportError(
                "boto3 is required for S3 cloud storage. "
                "Install it with: pip install boto3"
            ) from e

        creds = self._config.credentials
        s3 = boto3.client(
            "s3",
            region_name=self._config.region,
            aws_access_key_id=creds.aws_access_key_id.secret_value,
            aws_secret_access_key=creds.aws_secret_access_key.secret_value,
        )
        prefix = self._config.base_path.rstrip("/") + "/"
        to_delete = []
        paginator = s3.get_paginator("list_objects_v2")
        for page in paginator.paginate(Bucket=self._config.bucket, Prefix=prefix):
            for obj in page.get("Contents", []):
                relative = obj["Key"][len(prefix) :]
                file_upload_id = relative.split("/")[0]
                if file_upload_id <= up_to_id:
                    to_delete.append({"Key": obj["Key"]})

        # S3 delete_objects accepts at most 1000 keys per call
        for i in range(0, len(to_delete), 1000):
            batch = to_delete[i : i + 1000]
            s3.delete_objects(Bucket=self._config.bucket, Delete={"Objects": batch})
        logger.debug(
            "Deleted %d staged S3 object(s) with upload_id <= %s",
            len(to_delete),
            up_to_id,
        )


class GCSStorage(CloudStorage):
    """Stages Parquet files in Google Cloud Storage.

    Requires ``google-cloud-storage`` (``pip install google-cloud-storage``).

    Upload path layout::

        gs://<bucket>/<base_path>/<upload_id>/<filename>

    Returned staged file refs are GCS HTTPS URLs::

        https://storage.googleapis.com/<bucket>/<base_path>/<upload_id>/<filename>

    Args:
        config: GCS bucket and credential configuration.
            ``upload_credentials`` drives the GCS SDK upload; when ``None``
            application default credentials (ADC) are used.
    """

    def __init__(self, config: GCSConfig) -> None:
        self._config = config

    def upload(self, upload_id: str, local_paths: list[Path]) -> list[str]:
        try:
            from google.cloud import storage as gcs
        except ImportError as e:
            raise ImportError(
                "google-cloud-storage is required for GCS cloud storage. "
                "Install it with: pip install google-cloud-storage"
            ) from e

        if self._config.upload_credentials is not None:
            import json

            from google.oauth2 import service_account

            creds = service_account.Credentials.from_service_account_info(
                json.loads(
                    self._config.upload_credentials.service_account_key.secret_value
                )
            )
            client = gcs.Client(credentials=creds)
        else:
            client = gcs.Client()

        bucket = client.bucket(self._config.bucket)
        uris = []
        for path in local_paths:
            blob_name = f"{self._config.base_path.rstrip('/')}/{upload_id}/{path.name}"
            logger.debug(
                "Uploading %s to gs://%s/%s", path, self._config.bucket, blob_name
            )
            blob = bucket.blob(blob_name)
            blob.upload_from_filename(str(path))
            uri = f"https://storage.googleapis.com/{self._config.bucket}/{blob_name}"
            uris.append(uri)
        return uris

    def delete(self, up_to_id: str) -> None:
        try:
            from google.cloud import storage as gcs
        except ImportError as e:
            raise ImportError(
                "google-cloud-storage is required for GCS cloud storage. "
                "Install it with: pip install google-cloud-storage"
            ) from e

        if self._config.upload_credentials is not None:
            import json

            from google.oauth2 import service_account

            creds = service_account.Credentials.from_service_account_info(
                json.loads(
                    self._config.upload_credentials.service_account_key.secret_value
                )
            )
            client = gcs.Client(credentials=creds)
        else:
            client = gcs.Client()

        bucket = client.bucket(self._config.bucket)
        prefix = self._config.base_path.rstrip("/") + "/"

        # list_blobs only returns file objects, not folder/directory objects.
        # Collect the files to delete and track which upload_id subdirectories
        # they belong to so we can clean up the folders separately afterwards.
        file_blobs = []
        deleted_upload_ids = set()
        for blob in bucket.list_blobs(prefix=prefix):
            relative = blob.name[len(prefix) :]
            file_upload_id = relative.split("/")[0]
            if file_upload_id <= up_to_id:
                file_blobs.append(blob)
                deleted_upload_ids.add(file_upload_id)

        for blob in file_blobs:
            blob.delete()

        # In HNS (hierarchical namespace) buckets, GCS creates a real folder
        # object for each upload_id subdirectory. These are not returned by
        # list_blobs and must be deleted explicitly via the storage_control_v2
        # Folders API once their contents have been removed.
        # On standard (non-HNS) buckets this raises NotFound and is silently
        # skipped — it is a safe no-op.
        folder_count = 0
        try:
            from google.cloud import storage_control_v2
        except ImportError:
            storage_control_v2 = None  # type: ignore[assignment]

        if storage_control_v2 is not None:
            sc_client = storage_control_v2.StorageControlClient()
            for upload_id in deleted_upload_ids:
                # folder name has no trailing slash in storage_control_v2
                folder_name = f"{prefix}{upload_id}".rstrip("/")
                folder_resource = sc_client.folder_path(
                    project="_",
                    bucket=self._config.bucket,
                    folder=folder_name,
                )
                try:
                    sc_client.delete_folder(
                        request=storage_control_v2.DeleteFolderRequest(
                            name=folder_resource
                        )
                    )
                    folder_count += 1
                except Exception:
                    pass  # Not an HNS bucket, or folder does not exist

        count = len(file_blobs) + folder_count
        logger.debug(
            "Deleted %d staged GCS object(s) with upload_id <= %s", count, up_to_id
        )


class AzureStorage(CloudStorage):
    """Stages Parquet files in Azure Blob Storage.

    Requires ``azure-storage-blob`` (``pip install azure-storage-blob``).

    Upload path layout::

        <container>/<base_path>/<upload_id>/<filename>

    Returned staged file refs are blob paths (not full HTTPS URLs)::

        <base_path>/<upload_id>/<filename>

    The Azure connection string is derived at runtime from the
    :class:`~tabsdata._credentials.AzureAccountKeyCredentials` stored in
    ``config.credentials``.

    Args:
        config: Azure container and credential configuration.
    """

    def __init__(self, config: AzureConfig) -> None:
        self._config = config

    def _connection_string(self) -> str:
        creds = self._config.credentials
        return (
            f"DefaultEndpointsProtocol=https;"
            f"AccountName={creds.account_name.secret_value};"
            f"AccountKey={creds.account_key.secret_value};"
            f"EndpointSuffix=core.windows.net"
        )

    def upload(self, upload_id: str, local_paths: list[Path]) -> list[str]:
        try:
            from azure.storage.blob import BlobServiceClient
        except ImportError as e:
            raise ImportError(
                "azure-storage-blob is required for Azure cloud storage. "
                "Install it with: pip install azure-storage-blob"
            ) from e

        service = BlobServiceClient.from_connection_string(self._connection_string())
        container = service.get_container_client(self._config.container)
        blob_paths = []
        for path in local_paths:
            blob_name = f"{self._config.base_path.rstrip('/')}/{upload_id}/{path.name}"
            logger.debug(
                "Uploading %s to azure://%s/%s",
                path,
                self._config.container,
                blob_name,
            )
            with open(path, "rb") as data:
                container.upload_blob(name=blob_name, data=data, overwrite=True)
            blob_paths.append(blob_name)
        return blob_paths

    def delete(self, up_to_id: str) -> None:
        try:
            from azure.storage.blob import BlobServiceClient
        except ImportError as e:
            raise ImportError(
                "azure-storage-blob is required for Azure cloud storage. "
                "Install it with: pip install azure-storage-blob"
            ) from e

        service = BlobServiceClient.from_connection_string(self._connection_string())
        container = service.get_container_client(self._config.container)
        prefix = self._config.base_path.rstrip("/") + "/"
        to_delete = []
        for blob in container.list_blobs(name_starts_with=prefix):
            relative = blob.name[len(prefix) :]
            file_upload_id = relative.split("/")[0]
            if file_upload_id <= up_to_id:
                to_delete.append(blob.name)
        for blob_name in to_delete:
            container.delete_blob(blob_name)
        logger.debug(
            "Deleted %d staged Azure blob(s) with upload_id <= %s",
            len(to_delete),
            up_to_id,
        )
