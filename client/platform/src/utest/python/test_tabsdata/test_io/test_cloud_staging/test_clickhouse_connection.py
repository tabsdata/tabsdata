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
from tabsdata._io.connections.clickhouse_connection import ClickHouseConn
from tabsdata._io.connections.connections import Conn
from tabsdata.exceptions import ErrorCode, ParameterValidationError


# ── Fixtures ──────────────────────────────────────────────────────────────────────────


@pytest.fixture
def s3_config():
    return S3Config(
        bucket="my-bucket",
        region="us-east-1",
        base_path="staging/clickhouse",
        credentials=S3AccessKeyCredentials("AKIAIOSFODNN7EXAMPLE", "secret"),
    )


@pytest.fixture
def azure_config():
    return AzureConfig(
        container="my-container",
        base_path="staging/clickhouse",
        credentials=AzureAccountKeyCredentials("myaccount", "base64key=="),
    )


@pytest.fixture
def gcs_config():
    return GCSConfig(
        bucket="my-gcs-bucket",
        base_path="staging/clickhouse",
    )


@pytest.fixture
def gcs_config_with_hmac():
    return GCSConfig(
        bucket="my-gcs-bucket",
        base_path="staging/clickhouse",
        hmac_credentials=HMACCredentials("hmac-access-key", "hmac-secret"),
    )


@pytest.fixture
def gcs_config_with_upload_creds():
    return GCSConfig(
        bucket="my-gcs-bucket",
        base_path="staging/clickhouse",
        upload_credentials=GCPServiceAccountKeyCredentials('{"type": "service_account"}'),
    )


@pytest.fixture
def ch_credentials():
    return UserPasswordCredentials("admin", "s3cr3t")


# ── Construction: all three cloud providers ───────────────────────────────────────────


def test_s3_staging_minimal(s3_config):
    conn = ClickHouseConn(host="ch.example.com", staging=s3_config)
    assert conn.host == "ch.example.com"
    assert conn.staging is s3_config
    assert isinstance(conn, Conn)


def test_azure_staging_minimal(azure_config):
    conn = ClickHouseConn(host="ch.example.com", staging=azure_config)
    assert conn.host == "ch.example.com"
    assert conn.staging is azure_config
    assert isinstance(conn, Conn)


def test_gcs_staging_minimal(gcs_config):
    conn = ClickHouseConn(host="ch.example.com", staging=gcs_config)
    assert conn.host == "ch.example.com"
    assert conn.staging is gcs_config


def test_gcs_staging_with_hmac(gcs_config_with_hmac):
    conn = ClickHouseConn(host="ch.example.com", staging=gcs_config_with_hmac)
    assert conn.staging.hmac_credentials is not None


def test_gcs_staging_with_upload_credentials(gcs_config_with_upload_creds):
    conn = ClickHouseConn(host="ch.example.com", staging=gcs_config_with_upload_creds)
    assert conn.staging.upload_credentials is not None


def test_gcs_staging_with_both_credentials():
    staging = GCSConfig(
        bucket="bucket",
        base_path="staging",
        upload_credentials=GCPServiceAccountKeyCredentials('{"type": "service_account"}'),
        hmac_credentials=HMACCredentials("key", "secret"),
    )
    conn = ClickHouseConn(host="ch.example.com", staging=staging)
    assert conn.staging.upload_credentials is not None
    assert conn.staging.hmac_credentials is not None


# ── Default values ────────────────────────────────────────────────────────────────────


def test_default_port(s3_config):
    conn = ClickHouseConn(host="ch.example.com", staging=s3_config)
    assert conn.port == 8123


def test_default_database(s3_config):
    conn = ClickHouseConn(host="ch.example.com", staging=s3_config)
    assert conn.database == "default"


def test_default_secure(s3_config):
    conn = ClickHouseConn(host="ch.example.com", staging=s3_config)
    assert conn.secure is False


def test_default_credentials_none(s3_config):
    conn = ClickHouseConn(host="ch.example.com", staging=s3_config)
    assert conn.credentials is None


def test_default_enforce_connection_params(s3_config):
    conn = ClickHouseConn(host="ch.example.com", staging=s3_config)
    assert conn.enforce_connection_params is True


def test_default_cx_src_configs_clickhouse(s3_config):
    conn = ClickHouseConn(host="ch.example.com", staging=s3_config)
    assert conn.cx_src_configs_clickhouse == {}


def test_default_cx_dst_configs_clickhouse(s3_config):
    conn = ClickHouseConn(host="ch.example.com", staging=s3_config)
    assert conn.cx_dst_configs_clickhouse == {}


# ── Custom parameter values ───────────────────────────────────────────────────────────


def test_custom_port(s3_config):
    conn = ClickHouseConn(host="ch.example.com", staging=s3_config, port=8443)
    assert conn.port == 8443


def test_custom_database(s3_config):
    conn = ClickHouseConn(host="ch.example.com", staging=s3_config, database="analytics")
    assert conn.database == "analytics"


def test_secure_true(s3_config):
    conn = ClickHouseConn(host="ch.example.com", staging=s3_config, secure=True)
    assert conn.secure is True


def test_credentials_provided(s3_config, ch_credentials):
    conn = ClickHouseConn(
        host="ch.example.com", staging=s3_config, credentials=ch_credentials
    )
    assert conn.credentials is not None


def test_enforce_connection_params_false(s3_config):
    conn = ClickHouseConn(
        host="ch.example.com", staging=s3_config, enforce_connection_params=False
    )
    assert conn.enforce_connection_params is False


def test_cx_src_configs_clickhouse_custom(s3_config):
    conn = ClickHouseConn(
        host="ch.example.com",
        staging=s3_config,
        cx_src_configs_clickhouse={"max_block_size": 65536},
    )
    assert conn.cx_src_configs_clickhouse == {"max_block_size": 65536}


def test_cx_dst_configs_clickhouse_custom(s3_config):
    conn = ClickHouseConn(
        host="ch.example.com",
        staging=s3_config,
        cx_dst_configs_clickhouse={"async_insert": 1},
    )
    assert conn.cx_dst_configs_clickhouse == {"async_insert": 1}


def test_cx_src_configs_none_becomes_empty_dict(s3_config):
    conn = ClickHouseConn(
        host="ch.example.com", staging=s3_config, cx_src_configs_clickhouse=None
    )
    assert conn.cx_src_configs_clickhouse == {}


def test_cx_dst_configs_none_becomes_empty_dict(s3_config):
    conn = ClickHouseConn(
        host="ch.example.com", staging=s3_config, cx_dst_configs_clickhouse=None
    )
    assert conn.cx_dst_configs_clickhouse == {}


def test_all_params_s3(s3_config, ch_credentials):
    conn = ClickHouseConn(
        host="ch.example.com",
        staging=s3_config,
        port=8443,
        database="analytics",
        credentials=ch_credentials,
        secure=True,
        enforce_connection_params=False,
        cx_src_configs_clickhouse={"src_key": "src_val"},
        cx_dst_configs_clickhouse={"dst_key": "dst_val"},
    )
    assert conn.host == "ch.example.com"
    assert conn.port == 8443
    assert conn.database == "analytics"
    assert conn.credentials is not None
    assert conn.secure is True
    assert conn.staging is s3_config
    assert conn.enforce_connection_params is False
    assert conn.cx_src_configs_clickhouse == {"src_key": "src_val"}
    assert conn.cx_dst_configs_clickhouse == {"dst_key": "dst_val"}


# ── Type validation errors ────────────────────────────────────────────────────────────


def test_wrong_type_port_raises(s3_config):
    with pytest.raises(ParameterValidationError) as exc:
        ClickHouseConn(host="ch.example.com", staging=s3_config, port="not-an-int")
    assert exc.value.error_code == ErrorCode.DECE109

def test_wrong_type_secure_raises(s3_config):
    with pytest.raises(ParameterValidationError) as exc:
        ClickHouseConn(host="ch.example.com", staging=s3_config, secure="not-a-bool")
    assert exc.value.error_code == ErrorCode.DECE109


def test_wrong_type_host_raises(s3_config):
    with pytest.raises(ParameterValidationError) as exc:
        ClickHouseConn(host=12345, staging=s3_config)
    assert exc.value.error_code == ErrorCode.DECE109


def test_wrong_type_credentials_raises(s3_config):
    wrong_creds = S3AccessKeyCredentials("key_id", "secret_key")
    with pytest.raises(ParameterValidationError) as exc:
        ClickHouseConn(host="ch.example.com", staging=s3_config, credentials=wrong_creds)
    assert exc.value.error_code == ErrorCode.DECE109


def test_missing_host_raises(s3_config):
    with pytest.raises(ParameterValidationError) as exc:
        ClickHouseConn(staging=s3_config)
    assert exc.value.error_code == ErrorCode.DECE109


def test_missing_staging_raises():
    with pytest.raises(ParameterValidationError) as exc:
        ClickHouseConn(host="ch.example.com")
    assert exc.value.error_code == ErrorCode.DECE109


def test_wrong_type_enforce_connection_params_raises(s3_config):
    with pytest.raises(ParameterValidationError) as exc:
        ClickHouseConn(
            host="ch.example.com",
            staging=s3_config,
            enforce_connection_params="not-a-bool",
        )
    assert exc.value.error_code == ErrorCode.DECE109


def test_wrong_type_cx_src_configs_raises(s3_config):
    with pytest.raises(ParameterValidationError) as exc:
        ClickHouseConn(
            host="ch.example.com",
            staging=s3_config,
            cx_src_configs_clickhouse="not-a-dict",
        )
    assert exc.value.error_code == ErrorCode.DECE109


def test_wrong_type_cx_dst_configs_raises(s3_config):
    with pytest.raises(ParameterValidationError) as exc:
        ClickHouseConn(
            host="ch.example.com",
            staging=s3_config,
            cx_dst_configs_clickhouse="not-a-dict",
        )
    assert exc.value.error_code == ErrorCode.DECE109


# ── Equality ──────────────────────────────────────────────────────────────────────────


def test_eq_identical_s3(s3_config):
    conn1 = ClickHouseConn(host="ch.example.com", staging=s3_config)
    conn2 = ClickHouseConn(host="ch.example.com", staging=s3_config)
    assert conn1 == conn2


def test_eq_identical_azure(azure_config):
    conn1 = ClickHouseConn(host="ch.example.com", staging=azure_config)
    conn2 = ClickHouseConn(host="ch.example.com", staging=azure_config)
    assert conn1 == conn2


def test_eq_identical_gcs(gcs_config):
    conn1 = ClickHouseConn(host="ch.example.com", staging=gcs_config)
    conn2 = ClickHouseConn(host="ch.example.com", staging=gcs_config)
    assert conn1 == conn2


def test_eq_different_host(s3_config):
    conn1 = ClickHouseConn(host="ch1.example.com", staging=s3_config)
    conn2 = ClickHouseConn(host="ch2.example.com", staging=s3_config)
    assert conn1 != conn2


def test_eq_different_port(s3_config):
    conn1 = ClickHouseConn(host="ch.example.com", staging=s3_config, port=8123)
    conn2 = ClickHouseConn(host="ch.example.com", staging=s3_config, port=8443)
    assert conn1 != conn2


def test_eq_different_database(s3_config):
    conn1 = ClickHouseConn(host="ch.example.com", staging=s3_config, database="db1")
    conn2 = ClickHouseConn(host="ch.example.com", staging=s3_config, database="db2")
    assert conn1 != conn2


def test_eq_different_secure(s3_config):
    conn1 = ClickHouseConn(host="ch.example.com", staging=s3_config, secure=False)
    conn2 = ClickHouseConn(host="ch.example.com", staging=s3_config, secure=True)
    assert conn1 != conn2


def test_eq_different_staging_provider(s3_config, azure_config):
    conn1 = ClickHouseConn(host="ch.example.com", staging=s3_config)
    conn2 = ClickHouseConn(host="ch.example.com", staging=azure_config)
    assert conn1 != conn2


def test_eq_different_ch_credentials(s3_config):
    conn1 = ClickHouseConn(
        host="ch.example.com",
        staging=s3_config,
        credentials=UserPasswordCredentials("user1", "pass1"),
    )
    conn2 = ClickHouseConn(
        host="ch.example.com",
        staging=s3_config,
        credentials=UserPasswordCredentials("user2", "pass2"),
    )
    assert conn1 != conn2


def test_eq_different_enforce_connection_params(s3_config):
    conn1 = ClickHouseConn(
        host="ch.example.com", staging=s3_config, enforce_connection_params=True
    )
    conn2 = ClickHouseConn(
        host="ch.example.com", staging=s3_config, enforce_connection_params=False
    )
    assert conn1 != conn2


def test_eq_different_cx_src_configs(s3_config):
    conn1 = ClickHouseConn(
        host="ch.example.com",
        staging=s3_config,
        cx_src_configs_clickhouse={"key": "a"},
    )
    conn2 = ClickHouseConn(
        host="ch.example.com",
        staging=s3_config,
        cx_src_configs_clickhouse={"key": "b"},
    )
    assert conn1 != conn2


def test_eq_different_cx_dst_configs(s3_config):
    conn1 = ClickHouseConn(
        host="ch.example.com",
        staging=s3_config,
        cx_dst_configs_clickhouse={"key": 1},
    )
    conn2 = ClickHouseConn(
        host="ch.example.com",
        staging=s3_config,
        cx_dst_configs_clickhouse={"key": 2},
    )
    assert conn1 != conn2


def test_eq_against_non_clickhouse_conn(s3_config):
    conn = ClickHouseConn(host="ch.example.com", staging=s3_config)
    assert conn != "not-a-conn"
    assert conn != 42
    assert conn != None  # noqa: E711


# ── Repr ──────────────────────────────────────────────────────────────────────────────


def test_repr_contains_host(s3_config):
    conn = ClickHouseConn(host="ch.example.com", staging=s3_config)
    r = repr(conn)
    assert "ch.example.com" in r
    assert "ClickHouseConn" in r


def test_repr_contains_database(s3_config):
    conn = ClickHouseConn(
        host="ch.example.com", staging=s3_config, database="analytics"
    )
    assert "analytics" in repr(conn)


def test_repr_contains_port(s3_config):
    conn = ClickHouseConn(host="ch.example.com", staging=s3_config, port=8443)
    assert "8443" in repr(conn)


def test_repr_contains_enforce_connection_params(s3_config):
    conn = ClickHouseConn(
        host="ch.example.com", staging=s3_config, enforce_connection_params=False
    )
    assert "enforce_connection_params=False" in repr(conn)


def test_repr_contains_cx_configs(s3_config):
    conn = ClickHouseConn(
        host="ch.example.com",
        staging=s3_config,
        cx_src_configs_clickhouse={"src": 1},
        cx_dst_configs_clickhouse={"dst": 2},
    )
    r = repr(conn)
    assert "cx_src_configs_clickhouse" in r
    assert "cx_dst_configs_clickhouse" in r


def test_repr_is_string(s3_config):
    conn = ClickHouseConn(host="ch.example.com", staging=s3_config)
    assert isinstance(repr(conn), str)
    assert repr(conn)
