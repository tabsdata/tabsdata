import pytest

from tabsdata._credentials import AzureAccountKeyCredentials
from tabsdata._format import CSVFormat, ParquetFormat
from tabsdata._io.connections.file_connections.azure_connection import AzureConn
from tabsdata._io.outputs.file.azure_output import AzureDest
from tabsdata.exceptions import (
    ConnectionConfigurationError,
    DestinationConfigurationError,
    ErrorCode,
    FormatConfigurationError,
    ParameterValidationError,
)

AZURE_CREDENTIALS = AzureAccountKeyCredentials(
    account_name="test_account_name",
    account_key="test_account_key",
)

VALID_CONTAINER = "az://mycontainer"


@pytest.fixture
def valid_conn():
    return AzureConn(container_uri=VALID_CONTAINER, credentials=AZURE_CREDENTIALS)


# --- AzureConn constructor tests ---


@pytest.mark.azure
@pytest.mark.unit
def test_azureconn_properties():
    conn = AzureConn(container_uri=VALID_CONTAINER, credentials=AZURE_CREDENTIALS)
    assert conn.container_uri == VALID_CONTAINER
    assert conn.container_name == "mycontainer"
    assert conn.credentials == AZURE_CREDENTIALS
    assert conn.cx_src_configs_azure == {}
    assert conn.cx_dst_configs_azure == {}
    assert conn.enforce_connection_params is True
    assert "AzureConn" in repr(conn)


@pytest.mark.azure
@pytest.mark.unit
def test_azureconn_invalid_credentials():
    with pytest.raises(ParameterValidationError):
        AzureConn(container_uri=VALID_CONTAINER, credentials="not_a_valid_credentials")


@pytest.mark.azure
@pytest.mark.unit
def test_azureconn_invalid_container_type():
    with pytest.raises(ParameterValidationError):
        AzureConn(container_uri=25, credentials=AZURE_CREDENTIALS)


@pytest.mark.azure
@pytest.mark.unit
def test_azureconn_invalid_scheme():
    with pytest.raises(ConnectionConfigurationError) as exc:
        AzureConn(container_uri="http://mycontainer", credentials=AZURE_CREDENTIALS)
    assert exc.value.error_code == ErrorCode.CXCE9


@pytest.mark.azure
@pytest.mark.unit
def test_azureconn_s3_scheme_invalid():
    with pytest.raises(ConnectionConfigurationError) as exc:
        AzureConn(container_uri="s3://mybucket", credentials=AZURE_CREDENTIALS)
    assert exc.value.error_code == ErrorCode.CXCE9


@pytest.mark.azure
@pytest.mark.unit
def test_azureconn_container_with_path_rejected():
    with pytest.raises(ConnectionConfigurationError) as exc:
        AzureConn(
            container_uri="az://mycontainer/some/prefix",
            credentials=AZURE_CREDENTIALS,
        )
    assert exc.value.error_code == ErrorCode.CXCE10


@pytest.mark.azure
@pytest.mark.unit
def test_azureconn_container_normalizes():
    conn = AzureConn(container_uri="az://mycontainer", credentials=AZURE_CREDENTIALS)
    assert conn.container_uri == "az://mycontainer"
    assert conn.container_name == "mycontainer"


@pytest.mark.azure
@pytest.mark.unit
def test_azureconn_container_trailing_slash_accepted():
    conn = AzureConn(container_uri="az://mycontainer/", credentials=AZURE_CREDENTIALS)
    assert conn.container_uri == "az://mycontainer"
    assert conn.container_name == "mycontainer"


@pytest.mark.azure
@pytest.mark.unit
def test_azureconn_cx_configs():
    src_config = {"key1": "value1"}
    dst_config = {"key2": "value2"}
    conn = AzureConn(
        container_uri=VALID_CONTAINER,
        credentials=AZURE_CREDENTIALS,
        cx_src_configs_azure=src_config,
        cx_dst_configs_azure=dst_config,
    )
    assert conn.cx_src_configs_azure == src_config
    assert conn.cx_dst_configs_azure == dst_config


@pytest.mark.azure
@pytest.mark.unit
def test_azureconn_cx_configs_default_to_empty_dict():
    conn = AzureConn(container_uri=VALID_CONTAINER, credentials=AZURE_CREDENTIALS)
    assert conn.cx_src_configs_azure == {}
    assert conn.cx_dst_configs_azure == {}


# --- AzureConn setter tests ---


@pytest.mark.azure
@pytest.mark.unit
def test_azureconn_set_container_uri(valid_conn):
    valid_conn.container_uri = "az://othercontainer"
    assert valid_conn.container_uri == "az://othercontainer"
    assert valid_conn.container_name == "othercontainer"


@pytest.mark.azure
@pytest.mark.unit
def test_azureconn_set_container_uri_invalid_type(valid_conn):
    with pytest.raises(ParameterValidationError):
        valid_conn.container_uri = 123


@pytest.mark.azure
@pytest.mark.unit
def test_azureconn_set_container_uri_invalid_scheme(valid_conn):
    with pytest.raises(ConnectionConfigurationError) as exc:
        valid_conn.container_uri = "gs://mybucket"
    assert exc.value.error_code == ErrorCode.CXCE9


@pytest.mark.azure
@pytest.mark.unit
def test_azureconn_set_container_uri_with_path_rejected(valid_conn):
    with pytest.raises(ConnectionConfigurationError) as exc:
        valid_conn.container_uri = "az://mycontainer/prefix"
    assert exc.value.error_code == ErrorCode.CXCE10


@pytest.mark.azure
@pytest.mark.unit
def test_azureconn_set_credentials(valid_conn):
    new_creds = AzureAccountKeyCredentials(
        account_name="new_name", account_key="new_key"
    )
    valid_conn.credentials = new_creds
    assert valid_conn.credentials == new_creds


@pytest.mark.azure
@pytest.mark.unit
def test_azureconn_set_credentials_invalid(valid_conn):
    with pytest.raises(ParameterValidationError):
        valid_conn.credentials = "not_valid"


@pytest.mark.azure
@pytest.mark.unit
def test_azureconn_set_cx_src_configs(valid_conn):
    valid_conn.cx_src_configs_azure = {"new_key": "new_value"}
    assert valid_conn.cx_src_configs_azure == {"new_key": "new_value"}


@pytest.mark.azure
@pytest.mark.unit
def test_azureconn_set_cx_dst_configs(valid_conn):
    valid_conn.cx_dst_configs_azure = {"new_key": "new_value"}
    assert valid_conn.cx_dst_configs_azure == {"new_key": "new_value"}


@pytest.mark.azure
@pytest.mark.unit
def test_azureconn_set_cx_configs_none_becomes_empty(valid_conn):
    valid_conn.cx_src_configs_azure = None
    valid_conn.cx_dst_configs_azure = None
    assert valid_conn.cx_src_configs_azure == {}
    assert valid_conn.cx_dst_configs_azure == {}


@pytest.mark.azure
@pytest.mark.unit
def test_azureconn_enforce_connection_params_default():
    conn = AzureConn(container_uri=VALID_CONTAINER, credentials=AZURE_CREDENTIALS)
    assert conn.enforce_connection_params is True


@pytest.mark.azure
@pytest.mark.unit
def test_azureconn_enforce_connection_params_explicit_false():
    conn = AzureConn(
        container_uri=VALID_CONTAINER,
        credentials=AZURE_CREDENTIALS,
        enforce_connection_params=False,
    )
    assert conn.enforce_connection_params is False


@pytest.mark.azure
@pytest.mark.unit
def test_azureconn_set_enforce_connection_params(valid_conn):
    assert valid_conn.enforce_connection_params is True
    valid_conn.enforce_connection_params = False
    assert valid_conn.enforce_connection_params is False
    valid_conn.enforce_connection_params = True
    assert valid_conn.enforce_connection_params is True


@pytest.mark.azure
@pytest.mark.unit
def test_azureconn_inequality_enforce_connection_params():
    conn1 = AzureConn(
        container_uri=VALID_CONTAINER,
        credentials=AZURE_CREDENTIALS,
        enforce_connection_params=True,
    )
    conn2 = AzureConn(
        container_uri=VALID_CONTAINER,
        credentials=AZURE_CREDENTIALS,
        enforce_connection_params=False,
    )
    assert conn1 != conn2


@pytest.mark.azure
@pytest.mark.unit
def test_azureconn_equality():
    conn1 = AzureConn(container_uri=VALID_CONTAINER, credentials=AZURE_CREDENTIALS)
    conn2 = AzureConn(container_uri=VALID_CONTAINER, credentials=AZURE_CREDENTIALS)
    assert conn1 == conn2


@pytest.mark.azure
@pytest.mark.unit
def test_azureconn_inequality():
    conn1 = AzureConn(container_uri="az://container1", credentials=AZURE_CREDENTIALS)
    conn2 = AzureConn(container_uri="az://container2", credentials=AZURE_CREDENTIALS)
    assert conn1 != conn2


# --- AzureDest constructor tests ---


@pytest.mark.azure
@pytest.mark.unit
def test_azuredest_properties(valid_conn):
    dest = AzureDest(
        conn=valid_conn, path="data/output.parquet", format=ParquetFormat()
    )
    assert dest.conn == valid_conn
    assert dest.path == ["data/output.parquet"]
    assert isinstance(dest.format, ParquetFormat)
    assert dest.allow_fragments is False
    assert dest._stream_require_ec is True


@pytest.mark.azure
@pytest.mark.unit
def test_azuredest_path_list(valid_conn):
    paths = ["data/file1.parquet", "data/file2.parquet"]
    dest = AzureDest(conn=valid_conn, path=paths, format=ParquetFormat())
    assert dest.path == paths


@pytest.mark.azure
@pytest.mark.unit
def test_azuredest_full_uri_computed(valid_conn):
    dest = AzureDest(
        conn=valid_conn, path="data/output.parquet", format=ParquetFormat()
    )
    assert dest._full_uri == ["az://mycontainer/data/output.parquet"]


@pytest.mark.azure
@pytest.mark.unit
def test_azuredest_full_uri_strips_leading_slash(valid_conn):
    dest = AzureDest(
        conn=valid_conn, path="/data/output.parquet", format=ParquetFormat()
    )
    assert dest._full_uri == ["az://mycontainer/data/output.parquet"]


@pytest.mark.azure
@pytest.mark.unit
def test_azuredest_format_inferred_from_path():
    conn = AzureConn(container_uri=VALID_CONTAINER, credentials=AZURE_CREDENTIALS)
    dest = AzureDest(conn=conn, path="data/output.parquet")
    assert isinstance(dest.format, ParquetFormat)


@pytest.mark.azure
@pytest.mark.unit
def test_azuredest_invalid_format(valid_conn):
    with pytest.raises(FormatConfigurationError) as exc:
        AzureDest(conn=valid_conn, path="data.parquet", format="not_a_supported_format")
    assert exc.value.error_code == ErrorCode.FOCE4


@pytest.mark.azure
@pytest.mark.unit
def test_azuredest_invalid_format_dtype(valid_conn):
    with pytest.raises(ParameterValidationError):
        AzureDest(conn=valid_conn, path="data.parquet", format=3)


@pytest.mark.azure
@pytest.mark.unit
def test_azuredest_rejects_fragment_placeholder(valid_conn):
    with pytest.raises(DestinationConfigurationError) as exc:
        AzureDest(
            conn=valid_conn,
            path="data_$FRAGMENT_IDX.parquet",
            format=ParquetFormat(),
        )
    assert exc.value.error_code == ErrorCode.DECE38


@pytest.mark.azure
@pytest.mark.unit
def test_azuredest_repr(valid_conn):
    dest = AzureDest(conn=valid_conn, path="data.parquet", format=ParquetFormat())
    r = repr(dest)
    assert "AzureDest" in r
    assert "format=" in r


# --- AzureDest setter tests ---


@pytest.mark.azure
@pytest.mark.unit
def test_azuredest_set_conn(valid_conn):
    dest = AzureDest(conn=valid_conn, path="data.parquet", format=ParquetFormat())
    conn2 = AzureConn(container_uri="az://other", credentials=AZURE_CREDENTIALS)
    dest.conn = conn2
    assert dest.conn == conn2


@pytest.mark.azure
@pytest.mark.unit
def test_azuredest_set_conn_invalid(valid_conn):
    dest = AzureDest(conn=valid_conn, path="data.parquet", format=ParquetFormat())
    with pytest.raises(ParameterValidationError):
        dest.conn = "not_a_conn"


@pytest.mark.azure
@pytest.mark.unit
def test_azuredest_set_path(valid_conn):
    dest = AzureDest(conn=valid_conn, path="data/old.parquet", format=ParquetFormat())
    dest.path = "data/new.parquet"
    assert dest.path == ["data/new.parquet"]
    assert dest._full_uri == ["az://mycontainer/data/new.parquet"]


@pytest.mark.azure
@pytest.mark.unit
def test_azuredest_set_path_rejects_fragment_placeholder(valid_conn):
    dest = AzureDest(conn=valid_conn, path="data.parquet", format=ParquetFormat())
    with pytest.raises(DestinationConfigurationError) as exc:
        dest.path = "data_$FRAGMENT_IDX.parquet"
    assert exc.value.error_code == ErrorCode.DECE38


@pytest.mark.azure
@pytest.mark.unit
def test_azuredest_set_format(valid_conn):
    dest = AzureDest(conn=valid_conn, path="data.parquet", format=ParquetFormat())
    dest.format = CSVFormat()
    assert isinstance(dest.format, CSVFormat)


@pytest.mark.azure
@pytest.mark.unit
def test_azuredest_set_format_from_string(valid_conn):
    dest = AzureDest(conn=valid_conn, path="data.parquet", format=ParquetFormat())
    dest.format = "csv"
    assert isinstance(dest.format, CSVFormat)


@pytest.mark.azure
@pytest.mark.unit
def test_azuredest_set_format_invalid(valid_conn):
    dest = AzureDest(conn=valid_conn, path="data.parquet", format=ParquetFormat())
    with pytest.raises(FormatConfigurationError) as exc:
        dest.format = "not_a_supported_format"
    assert exc.value.error_code == ErrorCode.FOCE4


@pytest.mark.azure
@pytest.mark.unit
def test_azuredest_set_format_invalid_type(valid_conn):
    dest = AzureDest(conn=valid_conn, path="data.parquet", format=ParquetFormat())
    with pytest.raises(ParameterValidationError):
        dest.format = 42
