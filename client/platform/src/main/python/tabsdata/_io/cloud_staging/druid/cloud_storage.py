#
# Copyright 2026 Tabsdata Inc.
#

from __future__ import annotations

import logging
from abc import ABC, abstractmethod

from tabsdata._io.cloud_staging.common.cloud_storage import (
    AzureStorage,
    CloudStorage,
    GCSStorage,
    S3Storage,
)

logger = logging.getLogger(__name__)


class DruidCloudStorage(CloudStorage, ABC):
    """Extension of ``CloudStorage`` for Druid native batch ingestion.

    Adds :meth:`druid_input_source`, which converts a list of staged file
    references returned by :meth:`upload` into the ``inputSource`` JSON
    object used in a Druid ``index_parallel`` ingestion spec.
    """

    @abstractmethod
    def druid_input_source(self, staged_file_refs: list[str]) -> dict:
        """Return the Druid ``inputSource`` dict for the staged files.

        Args:
            staged_file_refs: Values previously returned by :meth:`upload`.

        Returns:
            A dict suitable for use as ``spec.ioConfig.inputSource`` in a
            Druid ``index_parallel`` ingestion spec.
        """


class S3DruidStorage(S3Storage, DruidCloudStorage):
    """Extends :class:`~tabsdata._io.cloud_staging.common.cloud_storage.S3Storage`
    with a Druid S3 ``inputSource``.

    The base :meth:`upload` returns HTTPS URLs.  This class converts them to
    ``s3://`` URIs for Druid's S3 input source and passes AWS credentials
    via the ``properties`` block.
    """

    def _https_to_s3_uri(self, https_url: str) -> str:
        """Convert an HTTPS S3 URL to an ``s3://`` URI.

        Uses the known bucket name and region from ``self._config`` to strip
        the URL prefix, which correctly handles bucket names containing dots.

        Raises:
            ValueError: If the URL does not match the expected S3 format.
        """
        prefix = (
            f"https://{self._config.bucket}.s3.{self._config.region}.amazonaws.com/"
        )
        if https_url.startswith(prefix):
            return f"s3://{self._config.bucket}/{https_url[len(prefix) :]}"
        raise ValueError(
            f"Cannot convert S3 URL to s3:// URI (unexpected format). "
            f"Expected prefix '{prefix}', got: {https_url}"
        )

    def druid_input_source(self, staged_file_refs: list[str]) -> dict:
        creds = self._config.credentials
        uris = [self._https_to_s3_uri(ref) for ref in staged_file_refs]
        logger.debug(
            "Building S3 Druid inputSource with %d URIs (bucket=%s)",
            len(uris),
            self._config.bucket,
        )
        return {
            "type": "s3",
            "uris": uris,
            "endpointConfig": {
                "signingRegion": self._config.region,
            },
            "properties": {
                "accessKeyId": creds.aws_access_key_id.secret_value,
                "secretAccessKey": creds.aws_secret_access_key.secret_value,
            },
        }


class GCSDruidStorage(GCSStorage, DruidCloudStorage):
    """Extends :class:`~tabsdata._io.cloud_staging.common.cloud_storage.GCSStorage`
    with a Druid-compatible ``inputSource`` for Google Cloud Storage.

    Uploads go through the native GCS SDK (using ``upload_credentials``).
    For the Druid input source, the strategy depends on available credentials:

    * **HMAC credentials** (``hmac_credentials`` on :class:`GCSConfig`):
      Uses Druid's S3 input source with GCS's S3-compatible endpoint
      (``https://storage.googleapis.com``), passing HMAC access/secret keys
      as per-task credentials.  This is the recommended approach because
      Druid's native ``google`` input source does *not* support per-task
      credentials.

    * **No HMAC credentials**: Falls back to Druid's ``google`` input source
      without per-task credentials.  This only works when the Druid process
      itself is configured with ``GOOGLE_APPLICATION_CREDENTIALS``.
    """

    def _convert_gcs_url(self, https_url: str, scheme: str) -> str:
        """Convert a GCS HTTPS URL to a ``<scheme>://`` URI.

        Args:
            https_url: The HTTPS URL returned by :meth:`upload`.
            scheme: Target URI scheme (e.g. ``"s3"`` or ``"gs"``).

        Raises:
            ValueError: If the URL does not match the expected GCS format.
        """
        prefix = f"https://storage.googleapis.com/{self._config.bucket}/"
        if https_url.startswith(prefix):
            return f"{scheme}://{self._config.bucket}/{https_url[len(prefix) :]}"
        raise ValueError(
            f"Cannot convert GCS URL to {scheme}:// URI (unexpected format). "
            f"Expected prefix '{prefix}', got: {https_url}"
        )

    def druid_input_source(self, staged_file_refs: list[str]) -> dict:
        hmac = self._config.hmac_credentials
        if hmac is not None:
            uris = [self._convert_gcs_url(ref, "s3") for ref in staged_file_refs]
            logger.debug(
                "Building GCS Druid inputSource via S3-compatible API "
                "with %d URIs (bucket=%s)",
                len(uris),
                self._config.bucket,
            )
            return {
                "type": "s3",
                "uris": uris,
                "endpointConfig": {
                    "url": "https://storage.googleapis.com",
                    "signingRegion": "auto",
                },
                "properties": {
                    "accessKeyId": hmac.access_key.secret_value,
                    "secretAccessKey": hmac.secret_key.secret_value,
                },
            }

        uris = [self._convert_gcs_url(ref, "gs") for ref in staged_file_refs]
        logger.debug(
            "Building GCS Druid inputSource (native google type, no per-task "
            "credentials) with %d URIs (bucket=%s)",
            len(uris),
            self._config.bucket,
        )
        return {
            "type": "google",
            "uris": uris,
        }


class AzureDruidStorage(AzureStorage, DruidCloudStorage):
    """Extends :class:`~tabsdata._io.cloud_staging.common.cloud_storage.AzureStorage`
    with a Druid Azure Blob Storage ``inputSource``.

    The base :meth:`upload` returns blob paths (not full URLs).  This class
    builds the ``azureStorage`` input source with account credentials.
    """

    def druid_input_source(self, staged_file_refs: list[str]) -> dict:
        creds = self._config.credentials
        prefixes = [
            f"azureStorage://{creds.account_name.secret_value}"
            f"/{self._config.container}/{ref}"
            for ref in staged_file_refs
        ]
        logger.debug(
            "Building Azure Druid inputSource with %d URIs (container=%s)",
            len(prefixes),
            self._config.container,
        )
        return {
            "type": "azureStorage",
            "uris": prefixes,
            "properties": {
                "key": creds.account_key.secret_value,
            },
        }
