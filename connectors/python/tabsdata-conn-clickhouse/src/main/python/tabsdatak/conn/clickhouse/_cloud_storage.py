#
# Copyright 2026 Tabsdata Inc.
#

from __future__ import annotations

import logging
from abc import ABC, abstractmethod
from dataclasses import dataclass
from urllib.parse import urlparse

import polars as pl
import pyarrow.parquet as pq

from tabsdatak.common._storage.transfer import (
    AWS_ACCESS_KEY_ID_OPTION,
    AWS_ENDPOINT_OPTION,
    AWS_REGION_OPTION,
    AWS_SECRET_ACCESS_KEY_OPTION,
    AWS_VIRTUAL_HOSTED_STYLE_REQUEST_OPTION,
    AZURE_STORAGE_ACCOUNT_KEY_OPTION,
    AZURE_STORAGE_ACCOUNT_NAME_OPTION,
    AZURE_STORAGE_ENDPOINT_OPTION,
    GOOGLE_SERVICE_ACCOUNT_KEY_OPTION,
)

# noinspection PyProtectedMember
from tabsdatak.conn.clickhouse._storage_config import (
    AzureConfig,
    GCSConfig,
    S3Config,
)
from tabsdatak.conn.common.file._store import FileStore
from tabsdatak.conn.common.types import cloud_full_path, normalize_dir_path

logger = logging.getLogger(__name__)


def _make_key(base_path: str, upload_id: str, filename: str) -> str:
    """Build the object key ``<base_path>/<upload_id>/<filename>`` with no
    leading slash -- S3 / GCS / Azure object keys are never slash-rooted
    (an empty ``base_path`` collapses to ``"<upload_id>/<filename>"``)."""
    return cloud_full_path(base_path, f"{upload_id}/{filename}")


# ClickHouse's list APIs (S3 ``list_objects_v2`` ``Prefix``, Azure
# ``name_starts_with``, GCS ``list_blobs`` ``prefix``) expect the
# ``base_path`` prefix spelled with a trailing "/" and no leading one, so
# that only objects staged under it match (and it lines up with the keys
# ``_make_key`` produces). An empty ``base_path`` yields "" (whole
# container).
def _list_prefix(base_path: str) -> str:
    return normalize_dir_path(base_path, lead_separator=False, back_separator=True)


# --- Generic CloudStorage --------------------------------------------


@dataclass(frozen=True)
class StagedFile:
    """A staged Parquet object: ``ref`` is what the dialect loads from,
    ``key`` the object's key in :meth:`CloudStorage.store`."""

    ref: str
    key: str


class CloudStorage(ABC):
    """Abstract interface for cloud storage operations.

    Implementations may return physical URIs (S3, Azure, GCS) or logical
    paths (e.g. Snowflake stage references). The dialect must understand
    both forms.
    """

    @property
    @abstractmethod
    def base_path(self) -> str:
        """The key prefix staged objects go under."""

    @abstractmethod
    def store(self) -> FileStore:
        """The object store rooted at the bucket or container."""

    @abstractmethod
    def staged_ref(self, key: str) -> str:
        """The reference the dialect loads the object at ``key`` from."""

    def upload(
        self, upload_id: str, frames: list[tuple[str, pl.LazyFrame]]
    ) -> list[StagedFile]:
        """Write each ``(filename, frame)`` as Parquet under
        ``<base_path>/<upload_id>/<filename>``, in order."""
        store = self.store()
        staged = []
        for filename, frame in frames:
            key = _make_key(self.base_path, upload_id, filename)
            logger.debug("Staging %s at %s", filename, store.uri(key))
            writer = store.open_writer(key)
            frame.sink_parquet(writer)
            writer.close()
            staged.append(StagedFile(self.staged_ref(key), key))
        return staged

    def footer(self, key: str) -> pq.FileMetaData:
        """The Parquet footer of the staged object at ``key``."""
        source = self.store().ranged(key)
        try:
            return pq.read_metadata(source)
        finally:
            source.close()

    @abstractmethod
    def delete(self, up_to_id: str) -> None:
        """Delete all staged files with upload_id <= ``up_to_id``.
        Uses lexicographic ordering of the IDs."""

    def _sweep(self, up_to_id: str) -> int:
        import obstore

        store = self.store()
        prefix = _list_prefix(self.base_path)
        swept = [
            meta["path"]
            for batch in obstore.list(store.store(), prefix=prefix or None)
            for meta in batch
            if meta["path"][len(prefix) :].split("/")[0] <= up_to_id
        ]
        store.delete(swept)
        return len(swept)


class S3Storage(CloudStorage):
    """Stages Parquet files in Amazon S3.

    Upload path layout::

        s3://<bucket>/<base_path>/<upload_id>/<filename>

    Returned staged file refs are HTTPS URLs of the form::

        https://<bucket>.s3.<region>.amazonaws.com/<base_path>/<upload_id>/<filename>

    or, with a custom endpoint in the options, ``<endpoint>/<bucket>/<key>``
    (``<scheme>://<bucket>.<host>/<key>`` when virtual-hosted style is on).
    """

    def __init__(self, config: S3Config) -> None:
        self._config = config

    @property
    def base_path(self) -> str:
        return self._config.base_path

    def store(self) -> FileStore:
        return FileStore(
            f"s3://{self._config.bucket}",
            {
                AWS_ACCESS_KEY_ID_OPTION: self._config.access_key_id,
                AWS_SECRET_ACCESS_KEY_OPTION: self._config.secret_access_key,
                AWS_REGION_OPTION: self._config.region,
                **self._config.options,
            },
        )

    def staged_ref(self, key: str) -> str:
        endpoint = self._config.options.get(AWS_ENDPOINT_OPTION)
        if endpoint is None:
            return (
                f"https://{self._config.bucket}"
                f".s3.{self._config.region}.amazonaws.com/{key}"
            )
        virtual = self._config.options.get(AWS_VIRTUAL_HOSTED_STYLE_REQUEST_OPTION)
        if virtual == "true":
            address = urlparse(endpoint)
            return f"{address.scheme}://{self._config.bucket}.{address.netloc}/{key}"
        return f"{endpoint.rstrip('/')}/{self._config.bucket}/{key}"

    def delete(self, up_to_id: str) -> None:
        swept = self._sweep(up_to_id)
        logger.debug(
            "Deleted %d staged S3 object(s) with upload_id <= %s", swept, up_to_id
        )


class GCSStorage(CloudStorage):
    """Stages Parquet files in Google Cloud Storage; the cleanup sweep
    uses the ``google-cloud-storage`` SDK.

    Upload path layout::

        gs://<bucket>/<base_path>/<upload_id>/<filename>

    Returned staged file refs are HTTPS URLs::

        https://storage.googleapis.com/<bucket>/<base_path>/<upload_id>/<filename>
    """

    _ENDPOINT = "https://storage.googleapis.com"

    def __init__(self, config: GCSConfig) -> None:
        self._config = config

    def _credentials(self):
        try:
            import json

            from google.oauth2 import service_account
        except ImportError as e:
            raise ImportError(
                "google-cloud-storage is required for GCS cloud"
                " storage. Install it with:"
                " pip install google-cloud-storage"
            ) from e
        return service_account.Credentials.from_service_account_info(
            json.loads(self._config.service_account_json)
        )

    def _client(self):
        try:
            from google.cloud import storage as gcs
        except ImportError as e:
            raise ImportError(
                "google-cloud-storage is required for GCS cloud"
                " storage. Install it with:"
                " pip install google-cloud-storage"
            ) from e
        return gcs.Client(credentials=self._credentials())

    @property
    def base_path(self) -> str:
        return self._config.base_path

    def store(self) -> FileStore:
        return FileStore(
            f"gs://{self._config.bucket}",
            {
                GOOGLE_SERVICE_ACCOUNT_KEY_OPTION: self._config.service_account_json,
                **self._config.options,
            },
        )

    def staged_ref(self, key: str) -> str:
        return f"{self._ENDPOINT}/{self._config.bucket}/{key}"

    def delete(self, up_to_id: str) -> None:
        """Delete every staged object with ``upload_id <= up_to_id``.

        Relies on ``upload_id`` being a base32hex-encoded UUIDv7 so
        lexicographic order matches creation time (see
        :func:`tabsdatak.conn.clickhouse._id.new_upload_id`). The
        per-test cleanup therefore also clears any orphaned files
        from older runs.
        """
        bucket = self._client().bucket(self._config.bucket)
        prefix = _list_prefix(self._config.base_path)

        file_blobs = []
        deleted_upload_ids: set[str] = set()
        for blob in bucket.list_blobs(prefix=prefix):
            relative = blob.name[len(prefix) :]
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

    def _delete_hns_folders(self, prefix: str, upload_ids: set[str]) -> int:
        try:
            from google.cloud import storage_control_v2
        except ImportError:
            return 0

        sc_client = storage_control_v2.StorageControlClient(
            credentials=self._credentials()
        )
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
                    request=storage_control_v2.DeleteFolderRequest(name=folder_resource)
                )
                count += 1
            except Exception:  # noqa: BLE001 -- flat bucket
                pass
        return count


class AzureStorage(CloudStorage):
    """Stages Parquet files in Azure Blob Storage.

    Upload path layout::

        <container>/<base_path>/<upload_id>/<filename>

    Returned staged file refs are blob paths (not full HTTPS URLs)::

        <base_path>/<upload_id>/<filename>
    """

    def __init__(self, config: AzureConfig) -> None:
        self._config = config

    @property
    def base_path(self) -> str:
        return self._config.base_path

    def store(self) -> FileStore:
        return FileStore(
            f"az://{self._config.container}",
            {
                AZURE_STORAGE_ACCOUNT_NAME_OPTION: self._config.account_name,
                AZURE_STORAGE_ACCOUNT_KEY_OPTION: self._config.account_key,
                **self._config.options,
            },
        )

    def account_url(self) -> str:
        endpoint = self._config.options.get(AZURE_STORAGE_ENDPOINT_OPTION)
        if endpoint is not None:
            return endpoint.rstrip("/")
        return f"https://{self._config.account_name}.blob.core.windows.net"

    def staged_ref(self, key: str) -> str:
        return key

    def delete(self, up_to_id: str) -> None:
        swept = self._sweep(up_to_id)
        logger.debug(
            "Deleted %d staged Azure blob(s) with upload_id <= %s", swept, up_to_id
        )


# --- ClickHouse-aware cloud storage ----------------------------------


PARQUET = "Parquet"


def string_literal(value: str) -> str:
    escaped = value.replace("\\", "\\\\").replace("'", "\\'")
    return f"'{escaped}'"


def table_function(name: str, *arguments: str) -> str:
    return f"{name}({', '.join(string_literal(argument) for argument in arguments)})"


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
        return table_function(
            "s3",
            staged_file_ref,
            self._config.access_key_id,
            self._config.secret_access_key,
            PARQUET,
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
        return table_function(
            "s3",
            staged_file_ref,
            self._config.hmac_access_key,
            self._config.hmac_secret_key,
            PARQUET,
        )


class AzureClickHouseStorage(AzureStorage, ClickHouseCloudStorage):
    """``AzureStorage`` + ClickHouse ``azureBlobStorage()`` table function."""

    def clickhouse_table_function(self, staged_file_ref: str) -> str:
        # The account key goes in as an argument of its own: ClickHouse hides
        # it in its query log and server logs then, which it does not do for a
        # key inside a connection string.
        return table_function(
            "azureBlobStorage",
            self.account_url(),
            self._config.container,
            staged_file_ref,
            self._config.account_name,
            self._config.account_key,
            PARQUET,
        )
