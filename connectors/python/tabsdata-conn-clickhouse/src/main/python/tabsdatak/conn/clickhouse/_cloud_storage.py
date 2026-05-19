from __future__ import annotations

import logging
from abc import ABC, abstractmethod
from pathlib import Path

# noinspection PyProtectedMember
from tabsdatak.conn.clickhouse._storage_config import (
    AzureConfig,
    GCSConfig,
    S3Config,
)

logger = logging.getLogger(__name__)


def _make_key(base_path: str, upload_id: str, filename: str) -> str:
    """Build a slash-joined object key with no leading slash and no
    empty path components (so an empty ``base_path`` simply collapses
    to ``"<upload_id>/<filename>"``)."""
    parts = [
        p
        for p in (base_path.strip("/"), upload_id, filename)
        if p
    ]
    return "/".join(parts)


# --- Generic CloudStorage --------------------------------------------


class CloudStorage(ABC):
    """Abstract interface for cloud storage operations.

    Implementations may return physical URIs (S3, Azure, GCS) or logical
    paths (e.g. Snowflake stage references). The dialect must understand
    both forms.
    """

    @abstractmethod
    def upload(self, upload_id: str, local_paths: list[Path]) -> list[str]:
        """Upload files to storage. Returns a list of storage URIs (or
        logical paths), one per input file, in the same order as
        ``local_paths``."""

    @abstractmethod
    def delete(self, up_to_id: str) -> None:
        """Delete all staged files with upload_id <= ``up_to_id``.
        Uses lexicographic ordering of the IDs."""


class S3Storage(CloudStorage):
    """Stages Parquet files in Amazon S3.

    Requires ``boto3`` (``pip install boto3``).

    Upload path layout::

        s3://<bucket>/<base_path>/<upload_id>/<filename>

    Returned staged file refs are HTTPS URLs of the form::

        https://<bucket>.s3.<region>.amazonaws.com/<base_path>/<upload_id>/<filename>
    """

    def __init__(self, config: S3Config) -> None:
        self._config = config

    def _client(self):
        try:
            import boto3
        except ImportError as e:
            raise ImportError(
                "boto3 is required for S3 cloud storage. "
                "Install it with: pip install boto3"
            ) from e
        return boto3.client(
            "s3",
            region_name=self._config.region,
            aws_access_key_id=self._config.access_key_id,
            aws_secret_access_key=self._config.secret_access_key,
        )

    def upload(self, upload_id: str, local_paths: list[Path]) -> list[str]:
        s3 = self._client()
        uris = []
        for path in local_paths:
            key = _make_key(
                self._config.base_path, upload_id, path.name
            )
            logger.debug(
                "Uploading %s to s3://%s/%s", path, self._config.bucket, key
            )
            s3.upload_file(str(path), self._config.bucket, key)
            uri = (
                f"https://{self._config.bucket}"
                f".s3.{self._config.region}.amazonaws.com/{key}"
            )
            uris.append(uri)
        return uris

    def delete(self, up_to_id: str) -> None:
        s3 = self._client()
        prefix = self._config.base_path.rstrip("/") + "/"
        to_delete = []
        paginator = s3.get_paginator("list_objects_v2")
        for page in paginator.paginate(
            Bucket=self._config.bucket, Prefix=prefix
        ):
            for obj in page.get("Contents", []):
                relative = obj["Key"][len(prefix):]
                file_upload_id = relative.split("/")[0]
                if file_upload_id <= up_to_id:
                    to_delete.append({"Key": obj["Key"]})

        # S3 delete_objects accepts at most 1000 keys per call.
        for i in range(0, len(to_delete), 1000):
            batch = to_delete[i:i + 1000]
            s3.delete_objects(
                Bucket=self._config.bucket, Delete={"Objects": batch}
            )
        logger.debug(
            "Deleted %d staged S3 object(s) with upload_id <= %s",
            len(to_delete),
            up_to_id,
        )


class GCSStorage(CloudStorage):
    """Stages Parquet files in Google Cloud Storage via the native
    GCS API (``google-cloud-storage`` SDK).

    Upload path layout::

        gs://<bucket>/<base_path>/<upload_id>/<filename>

    Returned staged file refs are HTTPS URLs::

        https://storage.googleapis.com/<bucket>/<base_path>/<upload_id>/<filename>
    """

    _ENDPOINT = "https://storage.googleapis.com"

    def __init__(self, config: GCSConfig) -> None:
        self._config = config

    def _client(self):
        try:
            import json

            from google.cloud import storage as gcs
            from google.oauth2 import service_account
        except ImportError as e:
            raise ImportError(
                "google-cloud-storage is required for GCS cloud"
                " storage. Install it with:"
                " pip install google-cloud-storage"
            ) from e
        creds = service_account.Credentials.from_service_account_info(
            json.loads(self._config.service_account_json)
        )
        return gcs.Client(credentials=creds)

    def upload(self, upload_id: str, local_paths: list[Path]) -> list[str]:
        bucket = self._client().bucket(self._config.bucket)
        uris = []
        for path in local_paths:
            key = _make_key(
                self._config.base_path, upload_id, path.name
            )
            logger.debug(
                "Uploading %s to gs://%s/%s",
                path,
                self._config.bucket,
                key,
            )
            bucket.blob(key).upload_from_filename(str(path))
            uri = f"{self._ENDPOINT}/{self._config.bucket}/{key}"
            uris.append(uri)
        return uris

    def delete(self, up_to_id: str) -> None:
        """Delete every staged object with ``upload_id <= up_to_id``.

        Relies on ``upload_id`` being a base32hex-encoded UUIDv7 so
        lexicographic order matches creation time (see
        :func:`tabsdatak.conn.clickhouse._id.new_upload_id`). The
        per-test cleanup therefore also clears any orphaned files
        from older runs.
        """
        bucket = self._client().bucket(self._config.bucket)
        prefix_root = self._config.base_path.strip("/")
        prefix = f"{prefix_root}/" if prefix_root else ""

        file_blobs = []
        deleted_upload_ids: set[str] = set()
        for blob in bucket.list_blobs(prefix=prefix):
            relative = blob.name[len(prefix):]
            file_upload_id = relative.split("/")[0]
            if file_upload_id <= up_to_id:
                file_blobs.append(blob)
                deleted_upload_ids.add(file_upload_id)

        for blob in file_blobs:
            blob.delete()

        # HNS buckets keep folder objects that ``list_blobs`` doesn't
        # see; deleting the underlying files leaves these as orphans
        # unless explicitly removed via ``storage_control_v2``. On a
        # flat (non-HNS) bucket the API returns ``NotFound`` and we
        # silently move on.
        folder_count = self._delete_hns_folders(prefix, deleted_upload_ids)
        logger.debug(
            "Deleted %d staged GCS object(s) with upload_id <= %s",
            len(file_blobs) + folder_count,
            up_to_id,
        )

    def _delete_hns_folders(
        self, prefix: str, upload_ids: set[str]
    ) -> int:
        try:
            from google.cloud import storage_control_v2
        except ImportError:
            return 0

        sc_client = storage_control_v2.StorageControlClient()
        count = 0
        for upload_id in upload_ids:
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
                count += 1
            except Exception:  # noqa: BLE001 -- flat bucket
                pass
        return count


class AzureStorage(CloudStorage):
    """Stages Parquet files in Azure Blob Storage.

    Requires ``azure-storage-blob``.

    Upload path layout::

        <container>/<base_path>/<upload_id>/<filename>

    Returned staged file refs are blob paths (not full HTTPS URLs)::

        <base_path>/<upload_id>/<filename>
    """

    def __init__(self, config: AzureConfig) -> None:
        self._config = config

    def _connection_string(self) -> str:
        return (
            f"DefaultEndpointsProtocol=https;"
            f"AccountName={self._config.account_name};"
            f"AccountKey={self._config.account_key};"
            f"EndpointSuffix=core.windows.net"
        )

    def _service(self):
        try:
            from azure.storage.blob import BlobServiceClient
        except ImportError as e:
            raise ImportError(
                "azure-storage-blob is required for Azure cloud storage."
                " Install it with: pip install azure-storage-blob"
            ) from e
        return BlobServiceClient.from_connection_string(
            self._connection_string()
        )

    def upload(self, upload_id: str, local_paths: list[Path]) -> list[str]:
        container = self._service().get_container_client(
            self._config.container
        )
        blob_paths = []
        for path in local_paths:
            blob_name = _make_key(
                self._config.base_path, upload_id, path.name
            )
            logger.debug(
                "Uploading %s to azure://%s/%s",
                path,
                self._config.container,
                blob_name,
            )
            with open(path, "rb") as data:
                container.upload_blob(
                    name=blob_name, data=data, overwrite=True
                )
            blob_paths.append(blob_name)
        return blob_paths

    def delete(self, up_to_id: str) -> None:
        container = self._service().get_container_client(
            self._config.container
        )
        prefix = self._config.base_path.rstrip("/") + "/"
        to_delete = []
        for blob in container.list_blobs(name_starts_with=prefix):
            relative = blob.name[len(prefix):]
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


# --- ClickHouse-aware cloud storage ----------------------------------


class ClickHouseCloudStorage(CloudStorage, ABC):
    """Extension of :class:`CloudStorage` for ClickHouse bulk loading.

    Adds :meth:`clickhouse_table_function`, which converts a staged file
    reference returned by :meth:`upload` into the ClickHouse table-function
    call used in ``INSERT INTO ... SELECT * FROM <fn>``.
    """

    @abstractmethod
    def clickhouse_table_function(self, staged_file_ref: str) -> str:
        """Return the ClickHouse table function call for the staged file."""


class S3ClickHouseStorage(S3Storage, ClickHouseCloudStorage):
    """``S3Storage`` + ClickHouse ``s3()`` table function."""

    def clickhouse_table_function(self, staged_file_ref: str) -> str:
        return (
            f"s3('{staged_file_ref}', "
            f"'{self._config.access_key_id}', "
            f"'{self._config.secret_access_key}', "
            f"'Parquet')"
        )


class GCSClickHouseStorage(GCSStorage, ClickHouseCloudStorage):
    """``GCSStorage`` + ClickHouse ``s3()`` (S3-compat) table function.

    Uploads happen via the native GCS API (fast) using the service
    account JSON in ``config``. ClickHouse reads the staged file
    through ``s3()`` against ``https://storage.googleapis.com``,
    which only speaks the S3 protocol -- the HMAC credentials in
    ``config`` are inlined into the table-function call.
    """

    def clickhouse_table_function(self, staged_file_ref: str) -> str:
        return (
            f"s3('{staged_file_ref}', "
            f"'{self._config.hmac_access_key}', "
            f"'{self._config.hmac_secret_key}', "
            f"'Parquet')"
        )


class AzureClickHouseStorage(AzureStorage, ClickHouseCloudStorage):
    """``AzureStorage`` + ClickHouse ``azureBlobStorage()`` table function."""

    def clickhouse_table_function(self, staged_file_ref: str) -> str:
        return (
            f"azureBlobStorage("
            f"'{self._connection_string()}', "
            f"'{self._config.container}', "
            f"'{staged_file_ref}', "
            f"'Parquet')"
        )
