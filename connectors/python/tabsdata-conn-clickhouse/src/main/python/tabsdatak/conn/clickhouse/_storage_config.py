from __future__ import annotations

from dataclasses import dataclass


@dataclass(frozen=True)
class S3Config:
    """Configuration for staging files in Amazon S3.

    Credentials are passed as raw strings (the new-model connector resolves
    Secret/Var into plain values before constructing the config).
    """

    bucket: str
    region: str
    base_path: str
    access_key_id: str
    secret_access_key: str

    def __repr__(self) -> str:
        return (
            f"S3Config(bucket={self.bucket!r}, "
            f"region={self.region!r}, "
            f"base_path={self.base_path!r}, "
            f"access_key_id='***redacted***', "
            f"secret_access_key='***redacted***')"
        )


@dataclass(frozen=True)
class AzureConfig:
    """Configuration for staging files in Azure Blob Storage."""

    container: str
    base_path: str
    account_name: str
    account_key: str

    def __repr__(self) -> str:
        return (
            f"AzureConfig("
            f"container={self.container!r}, "
            f"base_path={self.base_path!r}, "
            f"account_name={self.account_name!r}, "
            f"account_key='***redacted***')"
        )


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

    def __repr__(self) -> str:
        return (
            f"GCSConfig(bucket={self.bucket!r}, "
            f"base_path={self.base_path!r}, "
            f"service_account_json='***redacted***', "
            f"hmac_access_key='***redacted***', "
            f"hmac_secret_key='***redacted***')"
        )
