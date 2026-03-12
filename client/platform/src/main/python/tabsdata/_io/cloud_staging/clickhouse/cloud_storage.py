#
# Copyright 2026 Tabs Data Inc.
#

from __future__ import annotations

from abc import ABC, abstractmethod

from tabsdata._io.cloud_staging.common.cloud_storage import (
    AzureStorage,
    CloudStorage,
    GCSStorage,
    S3Storage,
)


class ClickHouseCloudStorage(CloudStorage, ABC):
    """Extension of ``CloudStorage`` for ClickHouse bulk loading.

    Adds :meth:`clickhouse_table_function`, which converts a staged file
    reference returned by :meth:`upload` into the ClickHouse table-function
    call used in ``INSERT INTO … SELECT * FROM <fn>``.
    """

    @abstractmethod
    def clickhouse_table_function(self, staged_file_ref: str) -> str:
        """Return the ClickHouse table function call for the staged file.

        Args:
            staged_file_ref: A value previously returned by :meth:`upload`.

        Returns:
            A string like ``s3('https://...', 'key', 'secret', 'Parquet')`` or
            ``azureBlobStorage('conn', 'container', 'blob', 'Parquet')``.
        """


class S3ClickHouseStorage(S3Storage, ClickHouseCloudStorage):
    """Extends :class:`~tabsdata._io.cloud_staging.common.cloud_storage.S3Storage`
    with a ClickHouse ``s3()`` table function.

    The :class:`~tabsdata._io.cloud_staging.common.storage_config.S3Config`
    ``credentials`` field supplies both the ``boto3`` upload credentials and
    the key/secret passed to the ``s3()`` table function.
    """

    def clickhouse_table_function(self, staged_file_ref: str) -> str:
        creds = self._config.credentials
        return (
            f"s3('{staged_file_ref}', "
            f"'{creds.aws_access_key_id.secret_value}', "
            f"'{creds.aws_secret_access_key.secret_value}', "
            f"'Parquet')"
        )


class GCSClickHouseStorage(GCSStorage, ClickHouseCloudStorage):
    """Extends :class:`~tabsdata._io.cloud_staging.common.cloud_storage.GCSStorage`
    with a ClickHouse ``s3()`` table function.

    ClickHouse reads GCS files through its S3-compatible interface using HMAC
    keys (``s3()`` table function pointed at ``https://storage.googleapis.com``).

    ``hmac_credentials`` (:class:`~tabsdata.HMACCredentials`) are passed to
    the ``s3()`` call; when ``None`` the table function is built without
    explicit credentials (suitable for public buckets or server-side IAM).
    """

    def clickhouse_table_function(self, staged_file_ref: str) -> str:
        """Uses the S3-compatible ``s3()`` table function pointed at GCS.

        ClickHouse's ``gcs()`` table function is an alias for ``s3()`` with
        S3-compatible mode; we use ``s3()`` explicitly to match the HTTPS URL
        format returned by :meth:`upload`.
        """
        hmac = self._config.hmac_credentials
        if hmac is not None:
            return (
                f"s3('{staged_file_ref}', "
                f"'{hmac.access_key.secret_value}', "
                f"'{hmac.secret_key.secret_value}', "
                f"'Parquet')"
            )
        return f"s3('{staged_file_ref}', 'Parquet')"


class AzureClickHouseStorage(AzureStorage, ClickHouseCloudStorage):
    """Extends :class:`~tabsdata._io.cloud_staging.common.cloud_storage.AzureStorage`
    with a ClickHouse ``azureBlobStorage()`` table function.

    The connection string is derived at runtime from
    :class:`~tabsdata._credentials.AzureAccountKeyCredentials`.
    ``staged_file_ref`` is the blob path returned by :meth:`upload`
    (e.g. ``staging/clickhouse/<upload_id>/file.parquet``).
    """

    def clickhouse_table_function(self, staged_file_ref: str) -> str:
        return (
            f"azureBlobStorage("
            f"'{self._connection_string()}', "
            f"'{self._config.container}', "
            f"'{staged_file_ref}', "
            f"'Parquet')"
        )
