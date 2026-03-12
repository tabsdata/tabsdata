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


class StarRocksCloudStorage(CloudStorage, ABC):
    """Extension of ``CloudStorage`` for StarRocks bulk loading.

    Adds :meth:`starrocks_files_function`, which converts a list of staged
    file references into a single StarRocks ``FILES(...)`` expression used
    in ``INSERT INTO … SELECT * FROM FILES(...)``.

    Unlike ClickHouse (one table-function call per file), StarRocks ``FILES()``
    accepts multiple paths or a wildcard pattern, so all staged files for one
    table can be loaded in a single ``INSERT … SELECT``.
    """

    @abstractmethod
    def starrocks_files_function(self, staged_file_refs: list[str]) -> str:
        """Return a StarRocks ``FILES(...)`` expression for the staged files.

        Args:
            staged_file_refs: Values previously returned by :meth:`upload`.

        Returns:
            A string like ``FILES("path" = "s3://...", "format" = "parquet", ...)``.
        """


def _common_directory(paths: list[str]) -> str:
    """Return the longest common directory prefix shared by all *paths*."""
    parts_list = [p.rsplit("/", 1)[0] for p in paths]
    if len(set(parts_list)) == 1:
        return parts_list[0]
    # Fallback: compute character-level common prefix up to the last '/'
    prefix = parts_list[0]
    for p in parts_list[1:]:
        while not p.startswith(prefix):
            prefix = prefix[: prefix.rfind("/")]
    return prefix


class S3StarRocksStorage(S3Storage, StarRocksCloudStorage):
    """Extends :class:`~tabsdata._io.cloud_staging.common.cloud_storage.S3Storage`
    with a StarRocks ``FILES()`` expression for S3.

    Staged file refs (HTTPS URLs) are converted to ``s3://`` paths as required
    by StarRocks' ``FILES()`` table function.
    """

    def starrocks_files_function(self, staged_file_refs: list[str]) -> str:
        # Convert HTTPS refs to s3:// paths
        https_prefix = (
            f"https://{self._config.bucket}.s3.{self._config.region}.amazonaws.com/"
        )
        s3_paths = [
            f"s3://{self._config.bucket}/{ref[len(https_prefix) :]}"
            for ref in staged_file_refs
        ]

        if len(s3_paths) == 1:
            path_expr = s3_paths[0]
        else:
            path_expr = _common_directory(s3_paths) + "/*"

        creds = self._config.credentials
        return (
            f"FILES("
            f'"path" = "{path_expr}", '
            f'"format" = "parquet", '
            f'"aws.s3.access_key" = "{creds.aws_access_key_id.secret_value}", '
            f'"aws.s3.secret_key" = "{creds.aws_secret_access_key.secret_value}", '
            f'"aws.s3.region" = "{self._config.region}"'
            f")"
        )


class GCSStarRocksStorage(GCSStorage, StarRocksCloudStorage):
    """Extends :class:`~tabsdata._io.cloud_staging.common.cloud_storage.GCSStorage`
    with a StarRocks ``FILES()`` expression for GCS.

    StarRocks accesses GCS via the S3-compatible protocol using HMAC
    credentials.  The ``s3a://`` path scheme together with the GCS
    S3-compatible endpoint lets StarRocks read from GCS without requiring
    access to the GCP metadata service.
    """

    def starrocks_files_function(self, staged_file_refs: list[str]) -> str:
        # Convert GCS HTTPS refs to s3a:// paths
        gcs_prefix = f"https://storage.googleapis.com/{self._config.bucket}/"
        s3a_paths = [
            f"s3a://{self._config.bucket}/{ref[len(gcs_prefix) :]}"
            for ref in staged_file_refs
        ]

        if len(s3a_paths) == 1:
            path_expr = s3a_paths[0]
        else:
            path_expr = _common_directory(s3a_paths) + "/*"

        hmac = self._config.hmac_credentials
        return (
            f"FILES("
            f'"path" = "{path_expr}", '
            f'"format" = "parquet", '
            f'"aws.s3.access_key" = "{hmac.access_key.secret_value}", '
            f'"aws.s3.secret_key" = "{hmac.secret_key.secret_value}", '
            f'"aws.s3.endpoint" = "https://storage.googleapis.com"'
            f")"
        )


class AzureStarRocksStorage(AzureStorage, StarRocksCloudStorage):
    """Extends :class:`~tabsdata._io.cloud_staging.common.cloud_storage.AzureStorage`
    with a StarRocks ``FILES()`` expression for Azure Blob Storage.

    Staged file refs (blob paths) are converted to ``wasbs://`` URIs as
    required by StarRocks' ``FILES()`` table function.
    """

    def starrocks_files_function(self, staged_file_refs: list[str]) -> str:
        creds = self._config.credentials
        account = creds.account_name.secret_value
        container = self._config.container

        # Convert blob paths to wasbs:// URIs
        wasbs_paths = [
            f"wasbs://{container}@{account}.blob.core.windows.net/{ref}"
            for ref in staged_file_refs
        ]

        if len(wasbs_paths) == 1:
            path_expr = wasbs_paths[0]
        else:
            path_expr = _common_directory(wasbs_paths) + "/*"

        return (
            f"FILES("
            f'"path" = "{path_expr}", '
            f'"format" = "parquet", '
            f'"azure.blob.shared_key" = "{creds.account_key.secret_value}"'
            f")"
        )
