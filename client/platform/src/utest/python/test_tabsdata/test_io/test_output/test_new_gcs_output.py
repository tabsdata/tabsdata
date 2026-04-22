import pytest

from tabsdata._credentials import GCPServiceAccountKeyCredentials
from tabsdata._format import CSVFormat, ParquetFormat
from tabsdata._io.connections.file_connections.gcp_connection import GCSConn
from tabsdata._io.outputs.file.gcs_output import GCSDest
from tabsdata.exceptions import (
    ConnectionConfigurationError,
    DestinationConfigurationError,
    ErrorCode,
    FormatConfigurationError,
    ParameterValidationError,
)

GCS_CREDENTIALS = GCPServiceAccountKeyCredentials(
    service_account_key="test_service_account_key",
)

VALID_BUCKET = "gs://mybucket"


@pytest.fixture
def valid_conn():
    return GCSConn(bucket_uri=VALID_BUCKET, credentials=GCS_CREDENTIALS)


# --- GCSConn constructor tests ---


@pytest.mark.gcs
@pytest.mark.unit
def test_gcsconn_properties():
    conn = GCSConn(bucket_uri=VALID_BUCKET, credentials=GCS_CREDENTIALS)
    assert conn.bucket_uri == VALID_BUCKET
    assert conn.bucket_name == "mybucket"
    assert conn.credentials == GCS_CREDENTIALS
    assert conn.cx_src_configs_gcs == {}
    assert conn.cx_dst_configs_gcs == {}
    assert conn.enforce_connection_params is True
    assert "GCSConn" in repr(conn)


@pytest.mark.gcs
@pytest.mark.unit
def test_gcsconn_invalid_credentials():
    with pytest.raises(ParameterValidationError):
        GCSConn(bucket_uri=VALID_BUCKET, credentials="not_a_valid_credentials")


@pytest.mark.gcs
@pytest.mark.unit
def test_gcsconn_invalid_bucket_type():
    with pytest.raises(ParameterValidationError):
        GCSConn(bucket_uri=25, credentials=GCS_CREDENTIALS)


@pytest.mark.gcs
@pytest.mark.unit
def test_gcsconn_invalid_scheme():
    with pytest.raises(ConnectionConfigurationError) as exc:
        GCSConn(bucket_uri="http://mybucket", credentials=GCS_CREDENTIALS)
    assert exc.value.error_code == ErrorCode.CXCE9


@pytest.mark.gcs
@pytest.mark.unit
def test_gcsconn_s3_scheme_invalid():
    with pytest.raises(ConnectionConfigurationError) as exc:
        GCSConn(bucket_uri="s3://mybucket", credentials=GCS_CREDENTIALS)
    assert exc.value.error_code == ErrorCode.CXCE9


@pytest.mark.gcs
@pytest.mark.unit
def test_gcsconn_bucket_with_path_rejected():
    with pytest.raises(ConnectionConfigurationError) as exc:
        GCSConn(bucket_uri="gs://mybucket/some/prefix", credentials=GCS_CREDENTIALS)
    assert exc.value.error_code == ErrorCode.CXCE10


@pytest.mark.gcs
@pytest.mark.unit
def test_gcsconn_bucket_normalizes():
    conn = GCSConn(bucket_uri="gs://mybucket", credentials=GCS_CREDENTIALS)
    assert conn.bucket_uri == "gs://mybucket"
    assert conn.bucket_name == "mybucket"


@pytest.mark.gcs
@pytest.mark.unit
def test_gcsconn_bucket_trailing_slash_accepted():
    conn = GCSConn(bucket_uri="gs://mybucket/", credentials=GCS_CREDENTIALS)
    assert conn.bucket_uri == "gs://mybucket"
    assert conn.bucket_name == "mybucket"


# --- GCSConn setter tests ---


@pytest.mark.gcs
@pytest.mark.unit
def test_gcsconn_set_bucket_uri(valid_conn):
    valid_conn.bucket_uri = "gs://otherbucket"
    assert valid_conn.bucket_uri == "gs://otherbucket"
    assert valid_conn.bucket_name == "otherbucket"


@pytest.mark.gcs
@pytest.mark.unit
def test_gcsconn_set_bucket_uri_invalid_type(valid_conn):
    with pytest.raises(ParameterValidationError):
        valid_conn.bucket_uri = 123


@pytest.mark.gcs
@pytest.mark.unit
def test_gcsconn_set_bucket_uri_invalid_scheme(valid_conn):
    with pytest.raises(ConnectionConfigurationError) as exc:
        valid_conn.bucket_uri = "s3://mybucket"
    assert exc.value.error_code == ErrorCode.CXCE9


@pytest.mark.gcs
@pytest.mark.unit
def test_gcsconn_set_bucket_uri_with_path_rejected(valid_conn):
    with pytest.raises(ConnectionConfigurationError) as exc:
        valid_conn.bucket_uri = "gs://mybucket/prefix"
    assert exc.value.error_code == ErrorCode.CXCE10


@pytest.mark.gcs
@pytest.mark.unit
def test_gcsconn_set_credentials(valid_conn):
    new_creds = GCPServiceAccountKeyCredentials(service_account_key="new_key")
    valid_conn.credentials = new_creds
    assert valid_conn.credentials == new_creds


@pytest.mark.gcs
@pytest.mark.unit
def test_gcsconn_set_credentials_invalid(valid_conn):
    with pytest.raises(ParameterValidationError):
        valid_conn.credentials = "not_valid"


@pytest.mark.gcs
@pytest.mark.unit
def test_gcsconn_cx_configs():
    src_config = {"key1": "value1"}
    dst_config = {"key2": "value2"}
    conn = GCSConn(
        bucket_uri=VALID_BUCKET,
        credentials=GCS_CREDENTIALS,
        cx_src_configs_gcs=src_config,
        cx_dst_configs_gcs=dst_config,
    )
    assert conn.cx_src_configs_gcs == src_config
    assert conn.cx_dst_configs_gcs == dst_config


@pytest.mark.gcs
@pytest.mark.unit
def test_gcsconn_cx_configs_default_to_empty_dict():
    conn = GCSConn(bucket_uri=VALID_BUCKET, credentials=GCS_CREDENTIALS)
    assert conn.cx_src_configs_gcs == {}
    assert conn.cx_dst_configs_gcs == {}


@pytest.mark.gcs
@pytest.mark.unit
def test_gcsconn_set_cx_src_configs(valid_conn):
    valid_conn.cx_src_configs_gcs = {"new_key": "new_value"}
    assert valid_conn.cx_src_configs_gcs == {"new_key": "new_value"}


@pytest.mark.gcs
@pytest.mark.unit
def test_gcsconn_set_cx_dst_configs(valid_conn):
    valid_conn.cx_dst_configs_gcs = {"new_key": "new_value"}
    assert valid_conn.cx_dst_configs_gcs == {"new_key": "new_value"}


@pytest.mark.gcs
@pytest.mark.unit
def test_gcsconn_set_cx_configs_none_becomes_empty(valid_conn):
    valid_conn.cx_src_configs_gcs = None
    valid_conn.cx_dst_configs_gcs = None
    assert valid_conn.cx_src_configs_gcs == {}
    assert valid_conn.cx_dst_configs_gcs == {}


@pytest.mark.gcs
@pytest.mark.unit
def test_gcsconn_enforce_connection_params_default():
    conn = GCSConn(bucket_uri=VALID_BUCKET, credentials=GCS_CREDENTIALS)
    assert conn.enforce_connection_params is True


@pytest.mark.gcs
@pytest.mark.unit
def test_gcsconn_enforce_connection_params_explicit_false():
    conn = GCSConn(
        bucket_uri=VALID_BUCKET,
        credentials=GCS_CREDENTIALS,
        enforce_connection_params=False,
    )
    assert conn.enforce_connection_params is False


@pytest.mark.gcs
@pytest.mark.unit
def test_gcsconn_set_enforce_connection_params(valid_conn):
    assert valid_conn.enforce_connection_params is True
    valid_conn.enforce_connection_params = False
    assert valid_conn.enforce_connection_params is False
    valid_conn.enforce_connection_params = True
    assert valid_conn.enforce_connection_params is True


@pytest.mark.gcs
@pytest.mark.unit
def test_gcsconn_inequality_enforce_connection_params():
    conn1 = GCSConn(
        bucket_uri=VALID_BUCKET,
        credentials=GCS_CREDENTIALS,
        enforce_connection_params=True,
    )
    conn2 = GCSConn(
        bucket_uri=VALID_BUCKET,
        credentials=GCS_CREDENTIALS,
        enforce_connection_params=False,
    )
    assert conn1 != conn2


@pytest.mark.gcs
@pytest.mark.unit
def test_gcsconn_equality():
    conn1 = GCSConn(bucket_uri=VALID_BUCKET, credentials=GCS_CREDENTIALS)
    conn2 = GCSConn(bucket_uri=VALID_BUCKET, credentials=GCS_CREDENTIALS)
    assert conn1 == conn2


@pytest.mark.gcs
@pytest.mark.unit
def test_gcsconn_inequality():
    conn1 = GCSConn(bucket_uri="gs://bucket1", credentials=GCS_CREDENTIALS)
    conn2 = GCSConn(bucket_uri="gs://bucket2", credentials=GCS_CREDENTIALS)
    assert conn1 != conn2


# --- GCSDest constructor tests ---


@pytest.mark.gcs
@pytest.mark.unit
def test_gcsdest_properties(valid_conn):
    dest = GCSDest(conn=valid_conn, path="data/output.parquet", format=ParquetFormat())
    assert dest.conn == valid_conn
    assert dest.path == ["data/output.parquet"]
    assert isinstance(dest.format, ParquetFormat)
    assert dest.allow_fragments is False
    assert dest._stream_require_ec is True


@pytest.mark.gcs
@pytest.mark.unit
def test_gcsdest_path_list(valid_conn):
    paths = ["data/file1.parquet", "data/file2.parquet"]
    dest = GCSDest(conn=valid_conn, path=paths, format=ParquetFormat())
    assert dest.path == paths


@pytest.mark.gcs
@pytest.mark.unit
def test_gcsdest_full_uri_computed(valid_conn):
    dest = GCSDest(conn=valid_conn, path="data/output.parquet", format=ParquetFormat())
    assert dest._full_uri == ["gs://mybucket/data/output.parquet"]


@pytest.mark.gcs
@pytest.mark.unit
def test_gcsdest_full_uri_strips_leading_slash(valid_conn):
    dest = GCSDest(conn=valid_conn, path="/data/output.parquet", format=ParquetFormat())
    assert dest._full_uri == ["gs://mybucket/data/output.parquet"]


@pytest.mark.gcs
@pytest.mark.unit
def test_gcsdest_format_inferred_from_path():
    conn = GCSConn(bucket_uri=VALID_BUCKET, credentials=GCS_CREDENTIALS)
    dest = GCSDest(conn=conn, path="data/output.parquet")
    assert isinstance(dest.format, ParquetFormat)


@pytest.mark.gcs
@pytest.mark.unit
def test_gcsdest_invalid_format(valid_conn):
    with pytest.raises(FormatConfigurationError) as exc:
        GCSDest(conn=valid_conn, path="data.parquet", format="not_a_supported_format")
    assert exc.value.error_code == ErrorCode.FOCE4


@pytest.mark.gcs
@pytest.mark.unit
def test_gcsdest_invalid_format_dtype(valid_conn):
    with pytest.raises(ParameterValidationError):
        GCSDest(conn=valid_conn, path="data.parquet", format=3)


@pytest.mark.gcs
@pytest.mark.unit
def test_gcsdest_rejects_fragment_placeholder(valid_conn):
    with pytest.raises(DestinationConfigurationError) as exc:
        GCSDest(
            conn=valid_conn,
            path="data_$FRAGMENT_IDX.parquet",
            format=ParquetFormat(),
        )
    assert exc.value.error_code == ErrorCode.DECE38


# --- GCSDest setter tests ---


@pytest.mark.gcs
@pytest.mark.unit
def test_gcsdest_set_conn(valid_conn):
    dest = GCSDest(conn=valid_conn, path="data.parquet", format=ParquetFormat())
    conn2 = GCSConn(bucket_uri="gs://otherbucket", credentials=GCS_CREDENTIALS)
    dest.conn = conn2
    assert dest.conn == conn2


@pytest.mark.gcs
@pytest.mark.unit
def test_gcsdest_set_conn_invalid(valid_conn):
    dest = GCSDest(conn=valid_conn, path="data.parquet", format=ParquetFormat())
    with pytest.raises(ParameterValidationError):
        dest.conn = "not_a_conn"


@pytest.mark.gcs
@pytest.mark.unit
def test_gcsdest_set_path(valid_conn):
    dest = GCSDest(conn=valid_conn, path="data/old.parquet", format=ParquetFormat())
    dest.path = "data/new.parquet"
    assert dest.path == ["data/new.parquet"]
    assert dest._full_uri == ["gs://mybucket/data/new.parquet"]


@pytest.mark.gcs
@pytest.mark.unit
def test_gcsdest_set_path_rejects_fragment_placeholder(valid_conn):
    dest = GCSDest(conn=valid_conn, path="data.parquet", format=ParquetFormat())
    with pytest.raises(DestinationConfigurationError) as exc:
        dest.path = "data_$FRAGMENT_IDX.parquet"
    assert exc.value.error_code == ErrorCode.DECE38


@pytest.mark.gcs
@pytest.mark.unit
def test_gcsdest_set_format(valid_conn):
    dest = GCSDest(conn=valid_conn, path="data.parquet", format=ParquetFormat())
    dest.format = CSVFormat()
    assert isinstance(dest.format, CSVFormat)


@pytest.mark.gcs
@pytest.mark.unit
def test_gcsdest_set_format_from_string(valid_conn):
    dest = GCSDest(conn=valid_conn, path="data.parquet", format=ParquetFormat())
    dest.format = "csv"
    assert isinstance(dest.format, CSVFormat)


@pytest.mark.gcs
@pytest.mark.unit
def test_gcsdest_set_format_invalid(valid_conn):
    dest = GCSDest(conn=valid_conn, path="data.parquet", format=ParquetFormat())
    with pytest.raises(FormatConfigurationError) as exc:
        dest.format = "not_a_supported_format"
    assert exc.value.error_code == ErrorCode.FOCE4


@pytest.mark.gcs
@pytest.mark.unit
def test_gcsdest_set_format_invalid_type(valid_conn):
    dest = GCSDest(conn=valid_conn, path="data.parquet", format=ParquetFormat())
    with pytest.raises(ParameterValidationError):
        dest.format = 42
