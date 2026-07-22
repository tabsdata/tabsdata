#
# Copyright 2026 Tabsdata Inc.
#

from . import pytestmark  # noqa: F401

import polars as pl
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
from tabsdata._io.connections.druid_connection import DruidConn
from tabsdata._io.outputs.sql.druid_output import DruidDest
from tabsdata._io.plugin import DestinationPlugin
from tabsdata.exceptions import (
    DestinationConfigurationError,
    ErrorCode,
    ParameterValidationError,
)


# ── Fixtures ──────────────────────────────────────────────────────────────────────────


@pytest.fixture
def s3_conn():
    staging = S3Config(
        bucket="my-bucket",
        region="us-east-1",
        base_path="staging/druid",
        credentials=S3AccessKeyCredentials("AKIAIOSFODNN7EXAMPLE", "secret"),
    )
    return DruidConn(overlord_url="http://druid-overlord:8090", staging=staging)


@pytest.fixture
def azure_conn():
    staging = AzureConfig(
        container="my-container",
        base_path="staging/druid",
        credentials=AzureAccountKeyCredentials("myaccount", "base64key=="),
    )
    return DruidConn(overlord_url="http://druid-overlord:8090", staging=staging)


@pytest.fixture
def gcs_conn():
    staging = GCSConfig(
        bucket="my-gcs-bucket",
        base_path="staging/druid",
        hmac_credentials=HMACCredentials("hmac-key", "hmac-secret"),
    )
    return DruidConn(overlord_url="http://druid-overlord:8090", staging=staging)


# ── Construction: defaults ────────────────────────────────────────────────────────────


def test_str_table_defaults(s3_conn):
    dest = DruidDest(s3_conn, "events")
    assert dest.conn == s3_conn
    assert dest.destination_tables == ["events"]
    assert dest.if_table_exists == "append"
    assert isinstance(dest, DestinationPlugin)


def test_list_tables_defaults(s3_conn):
    dest = DruidDest(s3_conn, ["events", "users"])
    assert dest.destination_tables == ["events", "users"]
    assert dest.if_table_exists == "append"


def test_all_params_explicit(s3_conn):
    dest = DruidDest(
        conn=s3_conn,
        destination_tables=["events", "users"],
        if_table_exists="replace",
    )
    assert dest.destination_tables == ["events", "users"]
    assert dest.if_table_exists == "replace"


# ── if_table_exists ───────────────────────────────────────────────────────────────────


def test_if_table_exists_append(s3_conn):
    dest = DruidDest(s3_conn, "events", if_table_exists="append")
    assert dest.if_table_exists == "append"


def test_if_table_exists_replace(s3_conn):
    dest = DruidDest(s3_conn, "events", if_table_exists="replace")
    assert dest.if_table_exists == "replace"


def test_if_table_exists_invalid_raises(s3_conn):
    with pytest.raises(ParameterValidationError) as exc:
        DruidDest(s3_conn, "events", if_table_exists="upsert")
    assert exc.value.error_code == ErrorCode.DECE112


# ── destination_tables normalisation ─────────────────────────────────────────────────


def test_str_table_normalized_to_list(s3_conn):
    dest = DruidDest(s3_conn, "events")
    assert dest.destination_tables == ["events"]


def test_multiple_tables(s3_conn):
    dest = DruidDest(s3_conn, ["t1", "t2", "t3"])
    assert dest.destination_tables == ["t1", "t2", "t3"]


def test_empty_str_table_raises(s3_conn):
    with pytest.raises(DestinationConfigurationError) as exc:
        DruidDest(s3_conn, "")
    assert exc.value.error_code == ErrorCode.DECE54


def test_whitespace_str_table_raises(s3_conn):
    with pytest.raises(DestinationConfigurationError) as exc:
        DruidDest(s3_conn, "   ")
    assert exc.value.error_code == ErrorCode.DECE54


def test_invalid_destination_tables_type_raises(s3_conn):
    with pytest.raises(ParameterValidationError) as exc:
        DruidDest(s3_conn, 12345)
    assert exc.value.error_code == ErrorCode.DECE112


def test_none_destination_tables_raises(s3_conn):
    with pytest.raises(ParameterValidationError) as exc:
        DruidDest(s3_conn, None)
    assert exc.value.error_code == ErrorCode.DECE112


def test_list_with_non_str_element_raises(s3_conn):
    with pytest.raises(ParameterValidationError) as exc:
        DruidDest(s3_conn, ["events", None])
    assert exc.value.error_code == ErrorCode.DECE112


# ── conn validation ───────────────────────────────────────────────────────────────────


def test_invalid_conn_type_raises():
    with pytest.raises(ParameterValidationError) as exc:
        DruidDest("not-a-conn", "events")
    assert exc.value.error_code == ErrorCode.DECE112


def test_conn_with_druid_credentials():
    staging = S3Config(
        bucket="b",
        region="us-east-1",
        base_path="p",
        credentials=S3AccessKeyCredentials("k", "s"),
    )
    conn = DruidConn(
        overlord_url="http://druid-overlord:8090",
        staging=staging,
        credentials=UserPasswordCredentials("admin", "pass"),
    )
    dest = DruidDest(conn, "events")
    assert dest.conn.credentials is not None


# ── All three cloud providers ─────────────────────────────────────────────────────────


def test_azure_staging(azure_conn):
    dest = DruidDest(azure_conn, "events")
    assert dest.conn == azure_conn
    assert dest.destination_tables == ["events"]


def test_gcs_staging(gcs_conn):
    dest = DruidDest(gcs_conn, "events")
    assert dest.conn == gcs_conn
    assert dest.destination_tables == ["events"]


def test_gcs_staging_with_upload_credentials():
    staging = GCSConfig(
        bucket="b",
        base_path="p",
        upload_credentials=GCPServiceAccountKeyCredentials(
            '{"type": "service_account"}'
        ),
    )
    conn = DruidConn(overlord_url="http://druid-overlord:8090", staging=staging)
    dest = DruidDest(conn, "events")
    assert dest.conn.staging.upload_credentials is not None


# ── conn property ─────────────────────────────────────────────────────────────────────


def test_conn_property_returns_same_object(s3_conn):
    dest = DruidDest(s3_conn, "events")
    assert dest.conn is s3_conn


# ── stream length mismatch ────────────────────────────────────────────────────────────


def test_stream_len_mismatch(s3_conn):
    dest = DruidDest(s3_conn, ["t1", "t2"])
    with pytest.raises(ValueError):
        dest.stream(".", pl.LazyFrame())


def test_stream_single_table_mismatch(s3_conn):
    dest = DruidDest(s3_conn, "t1")
    with pytest.raises(ValueError):
        dest.stream(".", pl.LazyFrame(), pl.LazyFrame())


# ── repr ──────────────────────────────────────────────────────────────────────────────


def test_repr_contains_class_name(s3_conn):
    dest = DruidDest(s3_conn, "events")
    assert "DruidDest" in repr(dest)


def test_repr_contains_table_name(s3_conn):
    dest = DruidDest(s3_conn, "events")
    assert "events" in repr(dest)


def test_repr_contains_if_table_exists(s3_conn):
    dest = DruidDest(s3_conn, "events", if_table_exists="replace")
    assert "replace" in repr(dest)


def test_repr_is_nonempty_string(s3_conn):
    dest = DruidDest(s3_conn, "events")
    assert isinstance(repr(dest), str)
    assert repr(dest)


# ── __td_dummy_time collision guard ──────────────────────────────────────────────


def test_stream_raises_on_dummy_time_collision(s3_conn, tmp_path):
    """When time_column is None, a column named __td_dummy_time in the data
    should raise ValueError to prevent silent collision with the synthetic
    timestamp."""
    dest = DruidDest(s3_conn, "events")
    df = pl.LazyFrame({"__td_dummy_time": [1, 2, 3], "value": [10, 20, 30]})
    with pytest.raises(ValueError, match="__td_dummy_time"):
        dest.stream(str(tmp_path), df)
