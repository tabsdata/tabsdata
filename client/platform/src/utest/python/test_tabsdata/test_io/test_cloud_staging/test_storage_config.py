#
# Copyright 2026 Tabs Data Inc.
#

import pytest

from tabsdata._credentials import (
    AzureAccountKeyCredentials,
    GCPServiceAccountKeyCredentials,
    HMACCredentials,
    S3AccessKeyCredentials,
)
from tabsdata._io.cloud_staging.common.storage_config import (
    AzureConfig,
    GCSConfig,
    S3Config,
)
from tabsdata.exceptions import ErrorCode, ParameterValidationError


# ── Fixtures ──────────────────────────────────────────────────────────────────────────


@pytest.fixture
def s3_credentials():
    return S3AccessKeyCredentials("AKIAIOSFODNN7EXAMPLE", "secret")


@pytest.fixture
def azure_credentials():
    return AzureAccountKeyCredentials("myaccount", "base64key==")


@pytest.fixture
def gcp_upload_credentials():
    return GCPServiceAccountKeyCredentials('{"type": "service_account"}')


@pytest.fixture
def hmac_credentials():
    return HMACCredentials("hmac-access-key", "hmac-secret")


# ── S3Config: initialization and parameter storage ────────────────────────────────────


def test_s3config_stores_bucket(s3_credentials):
    cfg = S3Config(
        bucket="my-bucket",
        region="us-east-1",
        base_path="staging",
        credentials=s3_credentials,
    )
    assert cfg.bucket == "my-bucket"


def test_s3config_stores_region(s3_credentials):
    cfg = S3Config(
        bucket="my-bucket",
        region="eu-west-1",
        base_path="staging",
        credentials=s3_credentials,
    )
    assert cfg.region == "eu-west-1"


def test_s3config_stores_base_path(s3_credentials):
    cfg = S3Config(
        bucket="my-bucket",
        region="us-east-1",
        base_path="staging/clickhouse",
        credentials=s3_credentials,
    )
    assert cfg.base_path == "staging/clickhouse"


def test_s3config_stores_credentials(s3_credentials):
    cfg = S3Config(
        bucket="my-bucket",
        region="us-east-1",
        base_path="staging",
        credentials=s3_credentials,
    )
    assert cfg.credentials is s3_credentials


# ── S3Config: type validation errors ──────────────────────────────────────────────────


def test_s3config_wrong_type_bucket_raises(s3_credentials):
    with pytest.raises(ParameterValidationError) as exc:
        S3Config(
            bucket=123,
            region="us-east-1",
            base_path="staging",
            credentials=s3_credentials,
        )
    assert exc.value.error_code == ErrorCode.CSC1


def test_s3config_wrong_type_region_raises(s3_credentials):
    with pytest.raises(ParameterValidationError) as exc:
        S3Config(
            bucket="my-bucket",
            region=123,
            base_path="staging",
            credentials=s3_credentials,
        )
    assert exc.value.error_code == ErrorCode.CSC1


def test_s3config_wrong_type_base_path_raises(s3_credentials):
    with pytest.raises(ParameterValidationError) as exc:
        S3Config(
            bucket="my-bucket",
            region="us-east-1",
            base_path=123,
            credentials=s3_credentials,
        )
    assert exc.value.error_code == ErrorCode.CSC1


def test_s3config_wrong_type_credentials_raises():
    with pytest.raises(ParameterValidationError) as exc:
        S3Config(
            bucket="my-bucket",
            region="us-east-1",
            base_path="staging",
            credentials="not-credentials",
        )
    assert exc.value.error_code == ErrorCode.CSC1


def test_s3config_wrong_credentials_class_raises(azure_credentials):
    with pytest.raises(ParameterValidationError) as exc:
        S3Config(
            bucket="my-bucket",
            region="us-east-1",
            base_path="staging",
            credentials=azure_credentials,
        )
    assert exc.value.error_code == ErrorCode.CSC1


def test_s3config_missing_bucket_raises(s3_credentials):
    with pytest.raises(ParameterValidationError) as exc:
        S3Config(
            region="us-east-1",
            base_path="staging",
            credentials=s3_credentials,
        )
    assert exc.value.error_code == ErrorCode.CSC1


# ── AzureConfig: initialization and parameter storage ─────────────────────────────────


def test_azureconfig_stores_container(azure_credentials):
    cfg = AzureConfig(
        container="my-container",
        base_path="staging",
        credentials=azure_credentials,
    )
    assert cfg.container == "my-container"


def test_azureconfig_stores_base_path(azure_credentials):
    cfg = AzureConfig(
        container="my-container",
        base_path="staging/clickhouse",
        credentials=azure_credentials,
    )
    assert cfg.base_path == "staging/clickhouse"


def test_azureconfig_stores_credentials(azure_credentials):
    cfg = AzureConfig(
        container="my-container",
        base_path="staging",
        credentials=azure_credentials,
    )
    assert cfg.credentials is azure_credentials


# ── AzureConfig: type validation errors ───────────────────────────────────────────────


def test_azureconfig_wrong_type_container_raises(azure_credentials):
    with pytest.raises(ParameterValidationError) as exc:
        AzureConfig(
            container=123,
            base_path="staging",
            credentials=azure_credentials,
        )
    assert exc.value.error_code == ErrorCode.CSC2


def test_azureconfig_wrong_type_base_path_raises(azure_credentials):
    with pytest.raises(ParameterValidationError) as exc:
        AzureConfig(
            container="my-container",
            base_path=123,
            credentials=azure_credentials,
        )
    assert exc.value.error_code == ErrorCode.CSC2


def test_azureconfig_wrong_type_credentials_raises():
    with pytest.raises(ParameterValidationError) as exc:
        AzureConfig(
            container="my-container",
            base_path="staging",
            credentials="not-credentials",
        )
    assert exc.value.error_code == ErrorCode.CSC2


def test_azureconfig_wrong_credentials_class_raises(s3_credentials):
    with pytest.raises(ParameterValidationError) as exc:
        AzureConfig(
            container="my-container",
            base_path="staging",
            credentials=s3_credentials,
        )
    assert exc.value.error_code == ErrorCode.CSC2


def test_azureconfig_missing_container_raises(azure_credentials):
    with pytest.raises(ParameterValidationError) as exc:
        AzureConfig(
            base_path="staging",
            credentials=azure_credentials,
        )
    assert exc.value.error_code == ErrorCode.CSC2


# ── GCSConfig: initialization and parameter storage ───────────────────────────────────


def test_gcsconfig_stores_bucket():
    cfg = GCSConfig(bucket="my-gcs-bucket", base_path="staging")
    assert cfg.bucket == "my-gcs-bucket"


def test_gcsconfig_stores_base_path():
    cfg = GCSConfig(bucket="my-gcs-bucket", base_path="staging/clickhouse")
    assert cfg.base_path == "staging/clickhouse"


def test_gcsconfig_defaults_upload_credentials_to_none():
    cfg = GCSConfig(bucket="my-gcs-bucket", base_path="staging")
    assert cfg.upload_credentials is None


def test_gcsconfig_defaults_hmac_credentials_to_none():
    cfg = GCSConfig(bucket="my-gcs-bucket", base_path="staging")
    assert cfg.hmac_credentials is None


def test_gcsconfig_stores_upload_credentials(gcp_upload_credentials):
    cfg = GCSConfig(
        bucket="my-gcs-bucket",
        base_path="staging",
        upload_credentials=gcp_upload_credentials,
    )
    assert cfg.upload_credentials is gcp_upload_credentials


def test_gcsconfig_stores_hmac_credentials(hmac_credentials):
    cfg = GCSConfig(
        bucket="my-gcs-bucket",
        base_path="staging",
        hmac_credentials=hmac_credentials,
    )
    assert cfg.hmac_credentials is hmac_credentials


def test_gcsconfig_stores_both_credentials(gcp_upload_credentials, hmac_credentials):
    cfg = GCSConfig(
        bucket="my-gcs-bucket",
        base_path="staging",
        upload_credentials=gcp_upload_credentials,
        hmac_credentials=hmac_credentials,
    )
    assert cfg.upload_credentials is gcp_upload_credentials
    assert cfg.hmac_credentials is hmac_credentials


# ── GCSConfig: type validation errors ─────────────────────────────────────────────────


def test_gcsconfig_wrong_type_bucket_raises():
    with pytest.raises(ParameterValidationError) as exc:
        GCSConfig(bucket=123, base_path="staging")
    assert exc.value.error_code == ErrorCode.CSC3


def test_gcsconfig_wrong_type_base_path_raises():
    with pytest.raises(ParameterValidationError) as exc:
        GCSConfig(bucket="my-gcs-bucket", base_path=123)
    assert exc.value.error_code == ErrorCode.CSC3


def test_gcsconfig_wrong_type_upload_credentials_raises():
    with pytest.raises(ParameterValidationError) as exc:
        GCSConfig(
            bucket="my-gcs-bucket",
            base_path="staging",
            upload_credentials="not-credentials",
        )
    assert exc.value.error_code == ErrorCode.CSC3


def test_gcsconfig_wrong_upload_credentials_class_raises(s3_credentials):
    with pytest.raises(ParameterValidationError) as exc:
        GCSConfig(
            bucket="my-gcs-bucket",
            base_path="staging",
            upload_credentials=s3_credentials,
        )
    assert exc.value.error_code == ErrorCode.CSC3


def test_gcsconfig_wrong_type_hmac_credentials_raises():
    with pytest.raises(ParameterValidationError) as exc:
        GCSConfig(
            bucket="my-gcs-bucket",
            base_path="staging",
            hmac_credentials="not-credentials",
        )
    assert exc.value.error_code == ErrorCode.CSC3


def test_gcsconfig_wrong_hmac_credentials_class_raises(s3_credentials):
    with pytest.raises(ParameterValidationError) as exc:
        GCSConfig(
            bucket="my-gcs-bucket",
            base_path="staging",
            hmac_credentials=s3_credentials,
        )
    assert exc.value.error_code == ErrorCode.CSC3


def test_gcsconfig_missing_bucket_raises():
    with pytest.raises(ParameterValidationError) as exc:
        GCSConfig(base_path="staging")
    assert exc.value.error_code == ErrorCode.CSC3
