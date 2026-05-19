from __future__ import annotations

from dataclasses import dataclass


@dataclass(frozen=True)
class S3Config:
    """Configuration for staging files in Amazon S3.

    Credentials are passed as raw strings (the new-model connector resolves
    Secret/Env into plain values before constructing the config).
    """

    bucket: str
    region: str
    base_path: str
    access_key_id: str
    secret_access_key: str


@dataclass(frozen=True)
class AzureConfig:
    """Configuration for staging files in Azure Blob Storage."""

    container: str
    base_path: str
    account_name: str
    account_key: str


@dataclass(frozen=True)
class GCSConfig:
    """Configuration for staging files in Google Cloud Storage.

    Carries both credential sets the conn-clickhouse pipeline needs:

    - ``service_account_json`` -- consumed by ``google-cloud-storage``
      for uploads / lists / deletes via the native GCS API (much
      faster than the S3-compat translation layer for bulk ops).
    - ``hmac_access_key`` / ``hmac_secret_key`` -- inlined into
      ClickHouse's ``s3()`` table function so the server can fetch
      the staged file via the S3-compatible endpoint.
    """

    bucket: str
    base_path: str
    service_account_json: str
    hmac_access_key: str
    hmac_secret_key: str
