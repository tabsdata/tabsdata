#
# Copyright 2026 Tabsdata Inc.
#

import pytest
from pydantic import ValidationError

from tabsdata._credentials import S3AccessKeyCredentials
from tabsdata._format import CSVFormat, ParquetFormat
from tabsdata._io.connections.file_connections.s3_connection import S3Conn
from tabsdata._io.outputs.file.s3_output import S3Dest
from tabsdata._io.outputs.file_outputs import AWSGlue
from tabsdata.exceptions import (
    ConnectionConfigurationError,
    DestinationConfigurationError,
    ErrorCode,
    FormatConfigurationError,
    ParameterValidationError,
)

TEST_ACCESS_KEY_ID = "test_access_key_id"
TEST_SECRET_ACCESS_KEY = "test_secret_access_key"
S3_CREDENTIALS = S3AccessKeyCredentials(
    aws_access_key_id=TEST_ACCESS_KEY_ID,
    aws_secret_access_key=TEST_SECRET_ACCESS_KEY,
)

VALID_BUCKET = "s3://mybucket"


@pytest.fixture
def valid_conn():
    return S3Conn(
        bucket_uri=VALID_BUCKET, credentials=S3_CREDENTIALS, region="us-east-1"
    )


# --- S3Conn constructor tests ---


@pytest.mark.s3
@pytest.mark.unit
def test_s3conn_properties():
    conn = S3Conn(
        bucket_uri=VALID_BUCKET, credentials=S3_CREDENTIALS, region="us-east-1"
    )
    assert conn.bucket_uri == VALID_BUCKET
    assert conn.bucket_name == "mybucket"
    assert conn.credentials == S3_CREDENTIALS
    assert conn.region == "us-east-1"
    assert conn.cx_src_configs_s3 == {}
    assert conn.cx_dst_configs_s3 == {}
    assert conn.enforce_connection_params is True
    assert "S3Conn" in repr(conn)


@pytest.mark.s3
@pytest.mark.unit
def test_s3conn_invalid_credentials():
    with pytest.raises(ParameterValidationError):
        S3Conn(bucket_uri=VALID_BUCKET, credentials="not_a_valid_credentials")


@pytest.mark.s3
@pytest.mark.unit
def test_s3conn_invalid_bucket_type():
    with pytest.raises(ParameterValidationError):
        S3Conn(bucket_uri=25, credentials=S3_CREDENTIALS)


@pytest.mark.s3
@pytest.mark.unit
def test_s3conn_invalid_bucket_scheme():
    with pytest.raises(ConnectionConfigurationError) as exc:
        S3Conn(bucket_uri="http://mybucket", credentials=S3_CREDENTIALS)
    assert exc.value.error_code == ErrorCode.CXCE9


@pytest.mark.s3
@pytest.mark.unit
def test_s3conn_bucket_with_path_rejected():
    with pytest.raises(ConnectionConfigurationError) as exc:
        S3Conn(bucket_uri="s3://mybucket/some/prefix", credentials=S3_CREDENTIALS)
    assert exc.value.error_code == ErrorCode.CXCE10


@pytest.mark.s3
@pytest.mark.unit
def test_s3conn_bucket_normalizes():
    conn = S3Conn(bucket_uri="s3://mybucket", credentials=S3_CREDENTIALS)
    assert conn.bucket_uri == "s3://mybucket"
    assert conn.bucket_name == "mybucket"


@pytest.mark.s3
@pytest.mark.unit
def test_s3conn_bucket_trailing_slash_accepted():
    conn = S3Conn(bucket_uri="s3://mybucket/", credentials=S3_CREDENTIALS)
    assert conn.bucket_uri == "s3://mybucket"
    assert conn.bucket_name == "mybucket"


@pytest.mark.s3
@pytest.mark.unit
def test_s3conn_invalid_region():
    conn = S3Conn(
        bucket_uri=VALID_BUCKET, credentials=S3_CREDENTIALS, region="not-a-region"
    )
    assert conn.region == "not-a-region"  # Warning is logged, but not an error


@pytest.mark.s3
@pytest.mark.unit
def test_s3conn_cx_configs():
    src_config = {"key1": "value1"}
    dst_config = {"key2": "value2"}
    conn = S3Conn(
        bucket_uri=VALID_BUCKET,
        credentials=S3_CREDENTIALS,
        cx_src_configs_s3=src_config,
        cx_dst_configs_s3=dst_config,
    )
    assert conn.cx_src_configs_s3 == src_config
    assert conn.cx_dst_configs_s3 == dst_config


@pytest.mark.s3
@pytest.mark.unit
def test_s3conn_cx_configs_default_to_empty_dict():
    conn = S3Conn(bucket_uri=VALID_BUCKET, credentials=S3_CREDENTIALS)
    assert conn.cx_src_configs_s3 == {}
    assert conn.cx_dst_configs_s3 == {}


# --- S3Conn setter tests ---


@pytest.mark.s3
@pytest.mark.unit
def test_s3conn_set_bucket(valid_conn):
    valid_conn.bucket_uri = "s3://otherbucket"
    assert valid_conn.bucket_uri == "s3://otherbucket"
    assert valid_conn.bucket_name == "otherbucket"


@pytest.mark.s3
@pytest.mark.unit
def test_s3conn_set_bucket_invalid_type(valid_conn):
    with pytest.raises(ParameterValidationError):
        valid_conn.bucket_uri = 123


@pytest.mark.s3
@pytest.mark.unit
def test_s3conn_set_bucket_invalid_scheme(valid_conn):
    with pytest.raises(ConnectionConfigurationError) as exc:
        valid_conn.bucket_uri = "gs://mybucket"
    assert exc.value.error_code == ErrorCode.CXCE9


@pytest.mark.s3
@pytest.mark.unit
def test_s3conn_set_bucket_with_path_rejected(valid_conn):
    with pytest.raises(ConnectionConfigurationError) as exc:
        valid_conn.bucket_uri = "s3://mybucket/prefix"
    assert exc.value.error_code == ErrorCode.CXCE10


@pytest.mark.s3
@pytest.mark.unit
def test_s3conn_set_credentials(valid_conn):
    new_creds = S3AccessKeyCredentials(
        aws_access_key_id="new_key", aws_secret_access_key="new_secret"
    )
    valid_conn.credentials = new_creds
    assert valid_conn.credentials == new_creds


@pytest.mark.s3
@pytest.mark.unit
def test_s3conn_set_credentials_invalid(valid_conn):
    with pytest.raises(ParameterValidationError):
        valid_conn.credentials = "not_valid"


@pytest.mark.s3
@pytest.mark.unit
def test_s3conn_set_region(valid_conn):
    valid_conn.region = "eu-west-1"
    assert valid_conn.region == "eu-west-1"


@pytest.mark.s3
@pytest.mark.unit
def test_s3conn_set_region_none(valid_conn):
    valid_conn.region = None
    assert valid_conn.region is None


@pytest.mark.s3
@pytest.mark.unit
def test_s3conn_set_cx_src_configs(valid_conn):
    valid_conn.cx_src_configs_s3 = {"new_key": "new_value"}
    assert valid_conn.cx_src_configs_s3 == {"new_key": "new_value"}


@pytest.mark.s3
@pytest.mark.unit
def test_s3conn_set_cx_dst_configs(valid_conn):
    valid_conn.cx_dst_configs_s3 = {"new_key": "new_value"}
    assert valid_conn.cx_dst_configs_s3 == {"new_key": "new_value"}


@pytest.mark.s3
@pytest.mark.unit
def test_s3conn_set_cx_configs_none_becomes_empty(valid_conn):
    valid_conn.cx_src_configs_s3 = None
    valid_conn.cx_dst_configs_s3 = None
    assert valid_conn.cx_src_configs_s3 == {}
    assert valid_conn.cx_dst_configs_s3 == {}


@pytest.mark.s3
@pytest.mark.unit
def test_s3conn_enforce_connection_params_default():
    conn = S3Conn(bucket_uri=VALID_BUCKET, credentials=S3_CREDENTIALS)
    assert conn.enforce_connection_params is True


@pytest.mark.s3
@pytest.mark.unit
def test_s3conn_enforce_connection_params_explicit_true():
    conn = S3Conn(
        bucket_uri=VALID_BUCKET,
        credentials=S3_CREDENTIALS,
        enforce_connection_params=True,
    )
    assert conn.enforce_connection_params is True


@pytest.mark.s3
@pytest.mark.unit
def test_s3conn_enforce_connection_params_explicit_false():
    conn = S3Conn(
        bucket_uri=VALID_BUCKET,
        credentials=S3_CREDENTIALS,
        enforce_connection_params=False,
    )
    assert conn.enforce_connection_params is False


@pytest.mark.s3
@pytest.mark.unit
def test_s3conn_set_enforce_connection_params(valid_conn):
    assert valid_conn.enforce_connection_params is True
    valid_conn.enforce_connection_params = False
    assert valid_conn.enforce_connection_params is False
    valid_conn.enforce_connection_params = True
    assert valid_conn.enforce_connection_params is True


@pytest.mark.s3
@pytest.mark.unit
def test_s3conn_inequality_enforce_connection_params():
    conn1 = S3Conn(
        bucket_uri=VALID_BUCKET,
        credentials=S3_CREDENTIALS,
        enforce_connection_params=True,
    )
    conn2 = S3Conn(
        bucket_uri=VALID_BUCKET,
        credentials=S3_CREDENTIALS,
        enforce_connection_params=False,
    )
    assert conn1 != conn2


@pytest.mark.s3
@pytest.mark.unit
def test_s3conn_equality():
    conn1 = S3Conn(
        bucket_uri=VALID_BUCKET, credentials=S3_CREDENTIALS, region="us-east-1"
    )
    conn2 = S3Conn(
        bucket_uri=VALID_BUCKET, credentials=S3_CREDENTIALS, region="us-east-1"
    )
    assert conn1 == conn2


@pytest.mark.s3
@pytest.mark.unit
def test_s3conn_inequality_bucket():
    conn1 = S3Conn(bucket_uri="s3://bucket1", credentials=S3_CREDENTIALS)
    conn2 = S3Conn(bucket_uri="s3://bucket2", credentials=S3_CREDENTIALS)
    assert conn1 != conn2


# --- S3Dest constructor tests ---


@pytest.mark.s3
@pytest.mark.unit
def test_s3dest_properties(valid_conn):
    dest = S3Dest(conn=valid_conn, path="data/output.parquet", format=ParquetFormat())
    assert dest.conn == valid_conn
    assert dest.path == ["data/output.parquet"]
    assert isinstance(dest.format, ParquetFormat)
    assert dest.catalog is None
    assert dest.allow_fragments is True
    assert dest._stream_require_ec is True
    assert "S3Dest" in repr(dest)


@pytest.mark.s3
@pytest.mark.unit
def test_s3dest_path_list(valid_conn):
    paths = ["data/file1.parquet", "data/file2.parquet"]
    dest = S3Dest(conn=valid_conn, path=paths, format=ParquetFormat())
    assert dest.path == paths


@pytest.mark.s3
@pytest.mark.unit
def test_s3dest_full_uri_computed(valid_conn):
    dest = S3Dest(conn=valid_conn, path="data/output.parquet", format=ParquetFormat())
    assert dest._full_uri == ["s3://mybucket/data/output.parquet"]


@pytest.mark.s3
@pytest.mark.unit
def test_s3dest_full_uri_strips_leading_slash(valid_conn):
    dest = S3Dest(conn=valid_conn, path="/data/output.parquet", format=ParquetFormat())
    assert dest._full_uri == ["s3://mybucket/data/output.parquet"]


@pytest.mark.s3
@pytest.mark.unit
def test_s3dest_full_uri_list(valid_conn):
    paths = ["data/file1.parquet", "data/file2.parquet"]
    dest = S3Dest(conn=valid_conn, path=paths, format=ParquetFormat())
    assert dest._full_uri == [
        "s3://mybucket/data/file1.parquet",
        "s3://mybucket/data/file2.parquet",
    ]


@pytest.mark.s3
@pytest.mark.unit
def test_s3dest_invalid_format(valid_conn):
    with pytest.raises(FormatConfigurationError) as exc:
        S3Dest(conn=valid_conn, path="data.parquet", format="not_a_supported_format")
    assert exc.value.error_code == ErrorCode.FOCE4


@pytest.mark.s3
@pytest.mark.unit
def test_s3dest_invalid_format_dtype(valid_conn):
    with pytest.raises(ParameterValidationError):
        S3Dest(conn=valid_conn, path="data.parquet", format=3)


@pytest.mark.s3
@pytest.mark.unit
def test_s3dest_catalog_only_parquet(valid_conn):
    catalog = AWSGlue(
        definition={
            "name": "default",
            "uri": "sqlite:////tmp/uri/pyiceberg_catalog.db",
            "warehouse": "file:///tmp/uri",
        },
        tables=["output1", "output2"],
    )
    with pytest.raises(DestinationConfigurationError) as exc:
        S3Dest(
            conn=valid_conn,
            path="data.csv",
            format=CSVFormat(),
            catalog=catalog,
        )
    assert exc.value.error_code == ErrorCode.DECE37


@pytest.mark.s3
@pytest.mark.unit
def test_s3dest_valid_catalog(valid_conn):
    catalog = AWSGlue(
        definition={
            "name": "default",
            "uri": "sqlite:////tmp/uri/pyiceberg_catalog.db",
            "warehouse": "file:///tmp/uri",
        },
        tables=["output1", "output2"],
    )
    dest = S3Dest(
        conn=valid_conn,
        path="data.parquet",
        format=ParquetFormat(),
        catalog=catalog,
    )
    assert dest.catalog is not None


@pytest.mark.s3
@pytest.mark.unit
def test_s3dest_repr(valid_conn):
    dest = S3Dest(conn=valid_conn, path="data.parquet", format=ParquetFormat())
    r = repr(dest)
    assert "S3Dest" in r
    assert "format=" in r


# --- S3Dest setter tests ---


@pytest.mark.s3
@pytest.mark.unit
def test_s3dest_set_conn(valid_conn):
    dest = S3Dest(conn=valid_conn, path="data.parquet", format=ParquetFormat())
    conn2 = S3Conn(bucket_uri="s3://otherbucket", credentials=S3_CREDENTIALS)
    dest.conn = conn2
    assert dest.conn == conn2


@pytest.mark.s3
@pytest.mark.unit
def test_s3dest_set_conn_invalid(valid_conn):
    dest = S3Dest(conn=valid_conn, path="data.parquet", format=ParquetFormat())
    with pytest.raises(ParameterValidationError):
        dest.conn = "not_a_conn"


@pytest.mark.s3
@pytest.mark.unit
def test_s3dest_set_path(valid_conn):
    dest = S3Dest(conn=valid_conn, path="data/old.parquet", format=ParquetFormat())
    dest.path = "data/new.parquet"
    assert dest.path == ["data/new.parquet"]
    assert dest._full_uri == ["s3://mybucket/data/new.parquet"]


@pytest.mark.s3
@pytest.mark.unit
def test_s3dest_set_path_list(valid_conn):
    dest = S3Dest(conn=valid_conn, path="data/old.parquet", format=ParquetFormat())
    new_paths = ["data/a.parquet", "data/b.parquet"]
    dest.path = new_paths
    assert dest.path == new_paths
    assert dest._full_uri == [
        "s3://mybucket/data/a.parquet",
        "s3://mybucket/data/b.parquet",
    ]


@pytest.mark.s3
@pytest.mark.unit
def test_s3dest_set_format(valid_conn):
    dest = S3Dest(conn=valid_conn, path="data.parquet", format=ParquetFormat())
    dest.format = CSVFormat()
    assert isinstance(dest.format, CSVFormat)


@pytest.mark.s3
@pytest.mark.unit
def test_s3dest_set_format_from_string(valid_conn):
    dest = S3Dest(conn=valid_conn, path="data.parquet", format=ParquetFormat())
    dest.format = "csv"
    assert isinstance(dest.format, CSVFormat)


@pytest.mark.s3
@pytest.mark.unit
def test_s3dest_set_format_invalid(valid_conn):
    dest = S3Dest(conn=valid_conn, path="data.parquet", format=ParquetFormat())
    with pytest.raises(FormatConfigurationError) as exc:
        dest.format = "not_a_supported_format"
    assert exc.value.error_code == ErrorCode.FOCE4


@pytest.mark.s3
@pytest.mark.unit
def test_s3dest_set_format_invalid_type(valid_conn):
    dest = S3Dest(conn=valid_conn, path="data.parquet", format=ParquetFormat())
    with pytest.raises(ParameterValidationError):
        dest.format = 42


@pytest.mark.s3
@pytest.mark.unit
def test_s3dest_set_catalog(valid_conn):
    dest = S3Dest(conn=valid_conn, path="data.parquet", format=ParquetFormat())
    assert dest.catalog is None
    catalog = AWSGlue(
        definition={
            "name": "default",
            "uri": "sqlite:////tmp/uri/pyiceberg_catalog.db",
            "warehouse": "file:///tmp/uri",
        },
        tables=["output1"],
    )
    dest.catalog = catalog
    assert dest.catalog is not None


@pytest.mark.s3
@pytest.mark.unit
def test_s3dest_set_catalog_none(valid_conn):
    catalog = AWSGlue(
        definition={
            "name": "default",
            "uri": "sqlite:////tmp/uri/pyiceberg_catalog.db",
            "warehouse": "file:///tmp/uri",
        },
        tables=["output1"],
    )
    dest = S3Dest(
        conn=valid_conn, path="data.parquet", format=ParquetFormat(), catalog=catalog
    )
    assert dest.catalog is not None
    dest.catalog = None
    assert dest.catalog is None


@pytest.mark.s3
@pytest.mark.unit
def test_s3dest_set_catalog_rejects_non_parquet(valid_conn):
    dest = S3Dest(conn=valid_conn, path="data.csv", format=CSVFormat())
    catalog = AWSGlue(
        definition={
            "name": "default",
            "uri": "sqlite:////tmp/uri/pyiceberg_catalog.db",
            "warehouse": "file:///tmp/uri",
        },
        tables=["output1"],
    )
    with pytest.raises(DestinationConfigurationError) as exc:
        dest.catalog = catalog
    assert exc.value.error_code == ErrorCode.DECE37
