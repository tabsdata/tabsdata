#
# Copyright 2026 Tabsdata Inc.
#

import pytest

from tabsdata._credentials import (
    AzureAccountKeyCredentials,
    GCPServiceAccountKeyCredentials,
    HMACCredentials,
    S3AccessKeyCredentials,
    UserPasswordCredentials,
)
from tabsdata._io.cloud_staging.common.storage_config import (
    AzureConfig,
    GCSConfig,
    S3Config,
)
from tabsdata._io.connections.connections import Conn
from tabsdata._io.connections.druid_connection import DruidConn
from tabsdata.exceptions import ErrorCode, ParameterValidationError


# ── Fixtures ──────────────────────────────────────────────────────────────────────────


@pytest.fixture
def s3_config():
    return S3Config(
        bucket="my-bucket",
        region="us-east-1",
        base_path="staging/druid",
        credentials=S3AccessKeyCredentials("AKIAIOSFODNN7EXAMPLE", "secret"),
    )


@pytest.fixture
def azure_config():
    return AzureConfig(
        container="my-container",
        base_path="staging/druid",
        credentials=AzureAccountKeyCredentials("myaccount", "base64key=="),
    )


@pytest.fixture
def gcs_config():
    return GCSConfig(
        bucket="my-gcs-bucket",
        base_path="staging/druid",
    )


@pytest.fixture
def gcs_config_with_hmac():
    return GCSConfig(
        bucket="my-gcs-bucket",
        base_path="staging/druid",
        hmac_credentials=HMACCredentials("hmac-access-key", "hmac-secret"),
    )


@pytest.fixture
def gcs_config_with_upload_creds():
    return GCSConfig(
        bucket="my-gcs-bucket",
        base_path="staging/druid",
        upload_credentials=GCPServiceAccountKeyCredentials(
            '{"type": "service_account"}'
        ),
    )


@pytest.fixture
def druid_credentials():
    return UserPasswordCredentials("admin", "s3cr3t")


# ── Construction: all three cloud providers ───────────────────────────────────────────


def test_s3_staging_minimal(s3_config):
    conn = DruidConn(
        overlord_url="http://druid-overlord:8090", staging=s3_config
    )
    assert conn.overlord_url == "http://druid-overlord:8090"
    assert conn.staging is s3_config
    assert isinstance(conn, Conn)


def test_azure_staging_minimal(azure_config):
    conn = DruidConn(
        overlord_url="http://druid-overlord:8090", staging=azure_config
    )
    assert conn.overlord_url == "http://druid-overlord:8090"
    assert conn.staging is azure_config
    assert isinstance(conn, Conn)


def test_gcs_staging_minimal(gcs_config):
    conn = DruidConn(
        overlord_url="http://druid-overlord:8090", staging=gcs_config
    )
    assert conn.overlord_url == "http://druid-overlord:8090"
    assert conn.staging is gcs_config


def test_gcs_staging_with_hmac(gcs_config_with_hmac):
    conn = DruidConn(
        overlord_url="http://druid-overlord:8090", staging=gcs_config_with_hmac
    )
    assert conn.staging.hmac_credentials is not None


def test_gcs_staging_with_upload_credentials(gcs_config_with_upload_creds):
    conn = DruidConn(
        overlord_url="http://druid-overlord:8090",
        staging=gcs_config_with_upload_creds,
    )
    assert conn.staging.upload_credentials is not None


def test_gcs_staging_with_both_credentials():
    staging = GCSConfig(
        bucket="my-gcs-bucket",
        base_path="staging/druid",
        upload_credentials=GCPServiceAccountKeyCredentials(
            '{"type": "service_account"}'
        ),
        hmac_credentials=HMACCredentials("hmac-access-key", "hmac-secret"),
    )
    conn = DruidConn(
        overlord_url="http://druid-overlord:8090", staging=staging
    )
    assert conn.staging.upload_credentials is not None
    assert conn.staging.hmac_credentials is not None


# ── Default values ────────────────────────────────────────────────────────────────────


def test_default_time_column(s3_config):
    conn = DruidConn(
        overlord_url="http://druid-overlord:8090", staging=s3_config
    )
    assert conn.time_column is None


def test_default_segment_granularity(s3_config):
    conn = DruidConn(
        overlord_url="http://druid-overlord:8090", staging=s3_config
    )
    assert conn.segment_granularity == "ALL"


def test_default_max_concurrent_subtasks(s3_config):
    conn = DruidConn(
        overlord_url="http://druid-overlord:8090", staging=s3_config
    )
    assert conn.max_concurrent_subtasks == 1


def test_default_max_rows_per_segment(s3_config):
    conn = DruidConn(
        overlord_url="http://druid-overlord:8090", staging=s3_config
    )
    assert conn.max_rows_per_segment == 5_000_000


def test_default_max_rows_in_memory(s3_config):
    conn = DruidConn(
        overlord_url="http://druid-overlord:8090", staging=s3_config
    )
    assert conn.max_rows_in_memory == 1_000_000


def test_default_http_timeout(s3_config):
    conn = DruidConn(
        overlord_url="http://druid-overlord:8090", staging=s3_config
    )
    assert conn.http_timeout_sec == 300


def test_default_credentials_none(s3_config):
    conn = DruidConn(
        overlord_url="http://druid-overlord:8090", staging=s3_config
    )
    assert conn.credentials is None


# ── Custom parameter values ───────────────────────────────────────────────────────────


def test_custom_time_column(s3_config):
    conn = DruidConn(
        overlord_url="http://druid-overlord:8090",
        staging=s3_config,
        time_column="event_ts",
    )
    assert conn.time_column == "event_ts"


def test_custom_segment_granularity(s3_config):
    conn = DruidConn(
        overlord_url="http://druid-overlord:8090",
        staging=s3_config,
        segment_granularity="DAY",
    )
    assert conn.segment_granularity == "DAY"


def test_custom_max_concurrent_subtasks(s3_config):
    conn = DruidConn(
        overlord_url="http://druid-overlord:8090",
        staging=s3_config,
        max_concurrent_subtasks=8,
    )
    assert conn.max_concurrent_subtasks == 8


def test_custom_max_rows_per_segment(s3_config):
    conn = DruidConn(
        overlord_url="http://druid-overlord:8090",
        staging=s3_config,
        max_rows_per_segment=10_000_000,
    )
    assert conn.max_rows_per_segment == 10_000_000


def test_custom_max_rows_in_memory(s3_config):
    conn = DruidConn(
        overlord_url="http://druid-overlord:8090",
        staging=s3_config,
        max_rows_in_memory=500_000,
    )
    assert conn.max_rows_in_memory == 500_000


def test_custom_http_timeout(s3_config):
    conn = DruidConn(
        overlord_url="http://druid-overlord:8090",
        staging=s3_config,
        http_timeout_sec=600,
    )
    assert conn.http_timeout_sec == 600


def test_credentials_provided(s3_config, druid_credentials):
    conn = DruidConn(
        overlord_url="http://druid-overlord:8090",
        staging=s3_config,
        credentials=druid_credentials,
    )
    assert conn.credentials is not None


def test_all_params_s3(s3_config, druid_credentials):
    conn = DruidConn(
        overlord_url="http://druid-overlord:8090",
        staging=s3_config,
        time_column="event_ts",
        segment_granularity="HOUR",
        max_concurrent_subtasks=4,
        max_rows_per_segment=3_000_000,
        max_rows_in_memory=750_000,
        http_timeout_sec=600,
        credentials=druid_credentials,
    )
    assert conn.overlord_url == "http://druid-overlord:8090"
    assert conn.staging is s3_config
    assert conn.time_column == "event_ts"
    assert conn.segment_granularity == "HOUR"
    assert conn.max_concurrent_subtasks == 4
    assert conn.max_rows_per_segment == 3_000_000
    assert conn.max_rows_in_memory == 750_000
    assert conn.http_timeout_sec == 600
    assert conn.credentials is not None


# ── Type validation errors ────────────────────────────────────────────────────────────


def test_wrong_type_overlord_url_raises(s3_config):
    with pytest.raises(ParameterValidationError) as exc:
        DruidConn(overlord_url=12345, staging=s3_config)
    assert exc.value.error_code == ErrorCode.DECE111


def test_wrong_type_max_concurrent_subtasks_raises(s3_config):
    with pytest.raises(ParameterValidationError) as exc:
        DruidConn(
            overlord_url="http://druid-overlord:8090",
            staging=s3_config,
            max_concurrent_subtasks="not-an-int",
        )
    assert exc.value.error_code == ErrorCode.DECE111


def test_wrong_type_credentials_raises(s3_config):
    wrong_creds = S3AccessKeyCredentials("key_id", "secret_key")
    with pytest.raises(ParameterValidationError) as exc:
        DruidConn(
            overlord_url="http://druid-overlord:8090",
            staging=s3_config,
            credentials=wrong_creds,
        )
    assert exc.value.error_code == ErrorCode.DECE111


def test_missing_overlord_url_raises(s3_config):
    with pytest.raises(ParameterValidationError) as exc:
        DruidConn(staging=s3_config)
    assert exc.value.error_code == ErrorCode.DECE111


def test_missing_staging_raises():
    with pytest.raises(ParameterValidationError) as exc:
        DruidConn(overlord_url="http://druid-overlord:8090")
    assert exc.value.error_code == ErrorCode.DECE111


# ── Equality ──────────────────────────────────────────────────────────────────────────


def test_eq_identical_s3(s3_config):
    conn1 = DruidConn(
        overlord_url="http://druid-overlord:8090", staging=s3_config
    )
    conn2 = DruidConn(
        overlord_url="http://druid-overlord:8090", staging=s3_config
    )
    assert conn1 == conn2


def test_eq_identical_azure(azure_config):
    conn1 = DruidConn(
        overlord_url="http://druid-overlord:8090", staging=azure_config
    )
    conn2 = DruidConn(
        overlord_url="http://druid-overlord:8090", staging=azure_config
    )
    assert conn1 == conn2


def test_eq_identical_gcs(gcs_config):
    conn1 = DruidConn(
        overlord_url="http://druid-overlord:8090", staging=gcs_config
    )
    conn2 = DruidConn(
        overlord_url="http://druid-overlord:8090", staging=gcs_config
    )
    assert conn1 == conn2


def test_eq_different_overlord_url(s3_config):
    conn1 = DruidConn(
        overlord_url="http://druid1:8090", staging=s3_config
    )
    conn2 = DruidConn(
        overlord_url="http://druid2:8090", staging=s3_config
    )
    assert conn1 != conn2


def test_eq_different_staging_provider(s3_config, azure_config):
    conn1 = DruidConn(
        overlord_url="http://druid-overlord:8090", staging=s3_config
    )
    conn2 = DruidConn(
        overlord_url="http://druid-overlord:8090", staging=azure_config
    )
    assert conn1 != conn2


def test_eq_different_time_column(s3_config):
    conn1 = DruidConn(
        overlord_url="http://druid-overlord:8090",
        staging=s3_config,
        time_column="ts1",
    )
    conn2 = DruidConn(
        overlord_url="http://druid-overlord:8090",
        staging=s3_config,
        time_column="ts2",
    )
    assert conn1 != conn2


def test_eq_different_segment_granularity(s3_config):
    conn1 = DruidConn(
        overlord_url="http://druid-overlord:8090",
        staging=s3_config,
        segment_granularity="ALL",
    )
    conn2 = DruidConn(
        overlord_url="http://druid-overlord:8090",
        staging=s3_config,
        segment_granularity="DAY",
    )
    assert conn1 != conn2


def test_eq_different_credentials(s3_config):
    conn1 = DruidConn(
        overlord_url="http://druid-overlord:8090",
        staging=s3_config,
        credentials=UserPasswordCredentials("user1", "pass1"),
    )
    conn2 = DruidConn(
        overlord_url="http://druid-overlord:8090",
        staging=s3_config,
        credentials=UserPasswordCredentials("user2", "pass2"),
    )
    assert conn1 != conn2


def test_eq_different_max_concurrent_subtasks(s3_config):
    conn1 = DruidConn(
        overlord_url="http://druid-overlord:8090",
        staging=s3_config,
        max_concurrent_subtasks=2,
    )
    conn2 = DruidConn(
        overlord_url="http://druid-overlord:8090",
        staging=s3_config,
        max_concurrent_subtasks=8,
    )
    assert conn1 != conn2


def test_eq_different_max_rows_per_segment(s3_config):
    conn1 = DruidConn(
        overlord_url="http://druid-overlord:8090",
        staging=s3_config,
        max_rows_per_segment=5_000_000,
    )
    conn2 = DruidConn(
        overlord_url="http://druid-overlord:8090",
        staging=s3_config,
        max_rows_per_segment=10_000_000,
    )
    assert conn1 != conn2


def test_eq_different_http_timeout(s3_config):
    conn1 = DruidConn(
        overlord_url="http://druid-overlord:8090",
        staging=s3_config,
        http_timeout_sec=300,
    )
    conn2 = DruidConn(
        overlord_url="http://druid-overlord:8090",
        staging=s3_config,
        http_timeout_sec=600,
    )
    assert conn1 != conn2


def test_eq_different_max_rows_in_memory(s3_config):
    conn1 = DruidConn(
        overlord_url="http://druid-overlord:8090",
        staging=s3_config,
        max_rows_in_memory=1_000_000,
    )
    conn2 = DruidConn(
        overlord_url="http://druid-overlord:8090",
        staging=s3_config,
        max_rows_in_memory=500_000,
    )
    assert conn1 != conn2


def test_eq_against_non_druid_conn(s3_config):
    conn = DruidConn(
        overlord_url="http://druid-overlord:8090", staging=s3_config
    )
    assert conn != "not-a-conn"
    assert conn != 42
    assert conn != None  # noqa: E711


# ── Repr ──────────────────────────────────────────────────────────────────────────────


def test_repr_contains_overlord_url(s3_config):
    conn = DruidConn(
        overlord_url="http://druid-overlord:8090", staging=s3_config
    )
    r = repr(conn)
    assert "druid-overlord" in r
    assert "DruidConn" in r


def test_repr_contains_time_column(s3_config):
    conn = DruidConn(
        overlord_url="http://druid-overlord:8090",
        staging=s3_config,
        time_column="event_ts",
    )
    assert "event_ts" in repr(conn)


def test_repr_contains_segment_granularity(s3_config):
    conn = DruidConn(
        overlord_url="http://druid-overlord:8090",
        staging=s3_config,
        segment_granularity="DAY",
    )
    assert "DAY" in repr(conn)


def test_repr_contains_http_timeout(s3_config):
    conn = DruidConn(
        overlord_url="http://druid-overlord:8090",
        staging=s3_config,
        http_timeout_sec=600,
    )
    assert "600" in repr(conn)


def test_repr_is_string(s3_config):
    conn = DruidConn(
        overlord_url="http://druid-overlord:8090", staging=s3_config
    )
    assert isinstance(repr(conn), str)
    assert repr(conn)


# ── segment_granularity validation ───────────────────────────────────────────────


def test_invalid_segment_granularity_raises(s3_config):
    with pytest.raises(ParameterValidationError) as exc:
        DruidConn(
            overlord_url="http://druid-overlord:8090",
            staging=s3_config,
            segment_granularity="INVALID",
        )
    assert exc.value.error_code == ErrorCode.DECE111


def test_lowercase_segment_granularity_raises(s3_config):
    with pytest.raises(ParameterValidationError) as exc:
        DruidConn(
            overlord_url="http://druid-overlord:8090",
            staging=s3_config,
            segment_granularity="day",
        )
    assert exc.value.error_code == ErrorCode.DECE111
