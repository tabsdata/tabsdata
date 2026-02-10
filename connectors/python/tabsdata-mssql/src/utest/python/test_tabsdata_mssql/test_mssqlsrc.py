import pytest

from tabsdata._credentials import UserPasswordCredentials
from tabsdata._io.plugin import SourcePlugin
from tabsdata.exceptions import (
    ConnectionConfigurationError,
    ErrorCode,
    ParameterValidationError,
    SourceConfigurationError,
)
from tabsdata.tableframe.dtypes import Float64, Int64
from tabsdata.tableframe.typing import DataType
from tabsdata.connector.mssql.connection.mssql_connection import MSSQLConn
from tabsdata.connector.mssql.input.mssql_input import MSSQLSrc


@pytest.mark.mssql
def test_all_correct_query_list():
    conn_str = (
        "DRIVER=ODBC Driver 18 for SQL Server;"
        "SERVER=localhost,1433;"
        "DATABASE=testing;"
        "TrustServerCertificate=yes;"
    )
    query = [
        "select * from INVOICE_HEADER where id > 0",
        "select * from INVOICE_ITEM where id > 0",
    ]
    credentials = UserPasswordCredentials("sa", "password123")
    conn = MSSQLConn(connection_string_or_uri=conn_str, credentials=credentials)
    src = MSSQLSrc(conn, query)
    assert src.conn == conn
    assert src.queries == query
    assert src.initial_values == {}
    assert isinstance(src, MSSQLSrc)
    assert isinstance(src, SourcePlugin)
    assert src.__repr__()


@pytest.mark.mssql
def test_all_correct_query_string():
    conn_str = (
        "DRIVER=ODBC Driver 18 for SQL Server;"
        "SERVER=localhost,1433;"
        "DATABASE=testing;"
        "TrustServerCertificate=yes;"
    )
    query = "select * from INVOICE_HEADER where id > 0"
    credentials = UserPasswordCredentials("sa", "password123")
    conn = MSSQLConn(connection_string_or_uri=conn_str, credentials=credentials)
    src = MSSQLSrc(conn, query)
    assert src.conn == conn
    assert src.queries == [query]
    assert src.initial_values == {}
    assert isinstance(src, MSSQLSrc)
    assert isinstance(src, SourcePlugin)
    assert src.__repr__()


@pytest.mark.mssql
def test_all_correct_query_string_no_credentials():
    conn_str = (
        "DRIVER=ODBC Driver 18 for SQL Server;"
        "SERVER=localhost,1433;"
        "DATABASE=testing;"
        "TrustServerCertificate=yes;"
    )
    query = "select * from INVOICE_HEADER where id > 0"
    conn = MSSQLConn(connection_string_or_uri=conn_str)
    src = MSSQLSrc(conn, query)
    assert src.conn == conn
    assert src.queries == [query]
    assert src.initial_values == {}
    assert isinstance(src, MSSQLSrc)
    assert isinstance(src, SourcePlugin)
    assert src.__repr__()


@pytest.mark.mssql
def test_query_wrong_type_raises_type_error():
    conn_str = (
        "DRIVER=ODBC Driver 18 for SQL Server;"
        "SERVER=localhost,1433;"
        "DATABASE=testing;"
        "TrustServerCertificate=yes;"
    )
    query = 42
    conn = MSSQLConn(connection_string_or_uri=conn_str)
    with pytest.raises(ParameterValidationError) as e:
        MSSQLSrc(conn, query)
    assert e.value.error_code == ErrorCode.SOCE108


@pytest.mark.mssql
def test_query_list_wrong_type_raises_type_error():
    conn_str = (
        "DRIVER=ODBC Driver 18 for SQL Server;"
        "SERVER=localhost,1433;"
        "DATABASE=testing;"
        "TrustServerCertificate=yes;"
    )
    query = [
        "select * from INVOICE_HEADER where id > 0",
        "select * from INVOICE_ITEM where id > 0",
        42,
    ]
    conn = MSSQLConn(connection_string_or_uri=conn_str)
    with pytest.raises(ParameterValidationError) as e:
        MSSQLSrc(conn, query)
    assert e.value.error_code == ErrorCode.SOCE108


@pytest.mark.mssql
def test_enforce_connection_params_default_true():
    conn_str = (
        "DRIVER=ODBC Driver 18 for SQL Server;"
        "SERVER=localhost,1433;"
        "DATABASE=testing;"
        "TrustServerCertificate=yes;"
    )
    conn = MSSQLConn(connection_string_or_uri=conn_str)
    assert conn.enforce_connection_params is True


@pytest.mark.mssql
def test_enforce_connection_params_setter():
    conn_str = (
        "DRIVER=ODBC Driver 18 for SQL Server;"
        "SERVER=localhost,1433;"
        "DATABASE=testing;"
        "TrustServerCertificate=yes;"
    )
    conn = MSSQLConn(connection_string_or_uri=conn_str, enforce_connection_params=False)
    assert conn.enforce_connection_params is False


@pytest.mark.mssql
def test_enforce_connection_params_invalid_type_raises():
    conn_str = (
        "DRIVER=ODBC Driver 18 for SQL Server;"
        "SERVER=localhost,1433;"
        "DATABASE=testing;"
        "TrustServerCertificate=yes;"
    )
    with pytest.raises(ParameterValidationError) as e:
        MSSQLConn(
            connection_string_or_uri=conn_str, enforce_connection_params="not_bool"
        )
    assert e.value.error_code == ErrorCode.DECE104


@pytest.mark.mssql
def test_cx_src_configs_mssql_default_and_setter():
    conn_str = (
        "DRIVER=ODBC Driver 18 for SQL Server;"
        "SERVER=localhost,1433;"
        "DATABASE=testing;"
        "TrustServerCertificate=yes;"
    )
    conn = MSSQLConn(connection_string_or_uri=conn_str)
    assert conn.cx_src_configs_mssql == {}
    conn = MSSQLConn(
        connection_string_or_uri=conn_str, cx_src_configs_mssql={"foo": "bar"}
    )
    assert conn.cx_src_configs_mssql == {"foo": "bar"}


@pytest.mark.mssql
def test_cx_src_configs_mssql_invalid_type_raises():
    conn_str = (
        "DRIVER=ODBC Driver 18 for SQL Server;"
        "SERVER=localhost,1433;"
        "DATABASE=testing;"
        "TrustServerCertificate=yes;"
    )
    conn = MSSQLConn(connection_string_or_uri=conn_str)
    with pytest.raises(ParameterValidationError) as e:
        conn = MSSQLConn(
            connection_string_or_uri=conn_str, cx_dst_configs_mssql="not_a_dict"
        )
    assert e.value.error_code == ErrorCode.DECE104


@pytest.mark.mssql
def test_cx_dst_configs_mssql_default_and_setter():
    conn_str = (
        "DRIVER=ODBC Driver 18 for SQL Server;"
        "SERVER=localhost,1433;"
        "DATABASE=testing;"
        "TrustServerCertificate=yes;"
    )
    conn = MSSQLConn(connection_string_or_uri=conn_str)
    assert conn.cx_dst_configs_mssql == {}
    conn = MSSQLConn(
        connection_string_or_uri=conn_str, cx_dst_configs_mssql={"baz": 123}
    )
    assert conn.cx_dst_configs_mssql == {"baz": 123}


@pytest.mark.mssql
def test_cx_dst_configs_mssql_invalid_type_raises():
    conn_str = (
        "DRIVER=ODBC Driver 18 for SQL Server;"
        "SERVER=localhost,1433;"
        "DATABASE=testing;"
        "TrustServerCertificate=yes;"
    )
    conn = MSSQLConn(connection_string_or_uri=conn_str)
    with pytest.raises(ParameterValidationError) as e:
        conn = MSSQLConn(
            connection_string_or_uri=conn_str, cx_dst_configs_mssql="not_a_dict"
        )
    assert e.value.error_code == ErrorCode.DECE104


@pytest.mark.mssql
def test_mssqlconn_eq_true_and_false():
    conn_str = (
        "DRIVER=ODBC Driver 18 for SQL Server;"
        "SERVER=localhost,1433;"
        "DATABASE=testing;"
        "TrustServerCertificate=yes;"
    )
    creds = UserPasswordCredentials("user", "pass")
    conn1 = MSSQLConn(
        connection_string_or_uri=conn_str,
        credentials=creds,
        cx_src_configs_mssql={"foo": "bar"},
        cx_dst_configs_mssql={"baz": 123},
    )
    conn2 = MSSQLConn(
        connection_string_or_uri=conn_str,
        credentials=creds,
        cx_src_configs_mssql={"foo": "bar"},
        cx_dst_configs_mssql={"baz": 123},
    )
    conn3 = MSSQLConn(
        connection_string_or_uri=conn_str,
        credentials=UserPasswordCredentials("user", "pass1"),
        enforce_connection_params=False,
        cx_src_configs_mssql={"foo": "bar"},
        cx_dst_configs_mssql={"baz": 123},
    )
    assert conn1 == conn2
    assert not (conn1 == conn3)
    assert conn1 != "not_a_mssqlconn"


@pytest.mark.mssql
def test_all_correct_initial_values():
    conn_str = (
        "DRIVER=ODBC Driver 18 for SQL Server;"
        "SERVER=localhost,1433;"
        "DATABASE=testing;"
        "TrustServerCertificate=yes;"
    )
    query = [
        "select * from INVOICE_HEADER where id > :number",
        "select * from INVOICE_ITEM where id > :number",
    ]
    credentials = UserPasswordCredentials("sa", "password123")
    initial_values = {
        "number": "0",
    }
    conn = MSSQLConn(connection_string_or_uri=conn_str, credentials=credentials)
    src = MSSQLSrc(conn, query, initial_values=initial_values)
    assert src.conn == conn
    assert src.queries == query
    assert src.initial_values == initial_values
    assert isinstance(src, MSSQLSrc)
    assert isinstance(src, SourcePlugin)
    assert src.__repr__()


@pytest.mark.mssql
def test_wrong_type_initial_values_raises_type_error():
    conn_str = (
        "DRIVER=ODBC Driver 18 for SQL Server;"
        "SERVER=localhost,1433;"
        "DATABASE=testing;"
        "TrustServerCertificate=yes;"
    )
    query = "select * from INVOICE_HEADER where id > :number"
    initial_values = 42
    conn = MSSQLConn(connection_string_or_uri=conn_str)
    with pytest.raises(ParameterValidationError) as e:
        MSSQLSrc(conn, query, initial_values=initial_values)
    assert e.value.error_code == ErrorCode.SOCE108


@pytest.mark.mssql
def test_none_initial_values_converted_empty_dict():
    conn_str = (
        "DRIVER=ODBC Driver 18 for SQL Server;"
        "SERVER=localhost,1433;"
        "DATABASE=testing;"
        "TrustServerCertificate=yes;"
    )
    query = "select * from INVOICE_HEADER where id > 0"
    initial_values = None
    conn = MSSQLConn(connection_string_or_uri=conn_str)
    src = MSSQLSrc(conn, query, initial_values=initial_values)
    assert src.initial_values == {}


@pytest.mark.mssql
def test_transactional_default_true():
    conn_str = (
        "DRIVER=ODBC Driver 18 for SQL Server;"
        "SERVER=localhost,1433;"
        "DATABASE=testing;"
        "TrustServerCertificate=yes;"
    )
    query = "select * from INVOICE_HEADER"
    conn = MSSQLConn(connection_string_or_uri=conn_str)
    src = MSSQLSrc(conn, query)
    assert src.transactional is True


@pytest.mark.mssql
def test_transactional_false():
    conn_str = (
        "DRIVER=ODBC Driver 18 for SQL Server;"
        "SERVER=localhost,1433;"
        "DATABASE=testing;"
        "TrustServerCertificate=yes;"
    )
    query = "select * from INVOICE_HEADER"
    conn = MSSQLConn(connection_string_or_uri=conn_str)
    src = MSSQLSrc(conn, query, transactional=False)
    assert src.transactional is False


@pytest.mark.mssql
def test_transactional_invalid_type_raises():
    conn_str = (
        "DRIVER=ODBC Driver 18 for SQL Server;"
        "SERVER=localhost,1433;"
        "DATABASE=testing;"
        "TrustServerCertificate=yes;"
    )
    query = "select * from INVOICE_HEADER"
    conn = MSSQLConn(connection_string_or_uri=conn_str)
    with pytest.raises(ParameterValidationError) as e:
        MSSQLSrc(conn, query, transactional=3)
    assert e.value.error_code == ErrorCode.SOCE108


@pytest.mark.mssql
def test_chunk_size_default():
    conn_str = (
        "DRIVER=ODBC Driver 18 for SQL Server;"
        "SERVER=localhost,1433;"
        "DATABASE=testing;"
        "TrustServerCertificate=yes;"
    )
    query = "select * from INVOICE_HEADER"
    conn = MSSQLConn(connection_string_or_uri=conn_str)
    src = MSSQLSrc(conn, query)
    assert src.chunk_size == 100000


@pytest.mark.mssql
def test_chunk_size_custom():
    conn_str = (
        "DRIVER=ODBC Driver 18 for SQL Server;"
        "SERVER=localhost,1433;"
        "DATABASE=testing;"
        "TrustServerCertificate=yes;"
    )
    query = "select * from INVOICE_HEADER"
    conn = MSSQLConn(connection_string_or_uri=conn_str)
    src = MSSQLSrc(conn, query, chunk_size=5000)
    assert src.chunk_size == 5000


@pytest.mark.mssql
def test_chunk_size_invalid_type_raises():
    conn_str = (
        "DRIVER=ODBC Driver 18 for SQL Server;"
        "SERVER=localhost,1433;"
        "DATABASE=testing;"
        "TrustServerCertificate=yes;"
    )
    query = "select * from INVOICE_HEADER"
    conn = MSSQLConn(connection_string_or_uri=conn_str)
    with pytest.raises(ParameterValidationError) as e:
        MSSQLSrc(conn, query, chunk_size="not_an_int")
    assert e.value.error_code == ErrorCode.SOCE108


@pytest.mark.mssql
def test_chunk_size_invalid_value_raises():
    conn_str = (
        "DRIVER=ODBC Driver 18 for SQL Server;"
        "SERVER=localhost,1433;"
        "DATABASE=testing;"
        "TrustServerCertificate=yes;"
    )
    query = "select * from INVOICE_HEADER"
    conn = MSSQLConn(connection_string_or_uri=conn_str)
    with pytest.raises(ParameterValidationError) as e:
        MSSQLSrc(conn, query, chunk_size=-2)
    assert e.value.error_code == ErrorCode.SOCE108


@pytest.mark.mssql
def test_loader_param_polars_sqlalchemy():
    conn_str = (
        "DRIVER=ODBC Driver 18 for SQL Server;"
        "SERVER=localhost,1433;"
        "DATABASE=testing;"
        "TrustServerCertificate=yes;"
    )
    query = "select * from INVOICE_HEADER"
    conn = MSSQLConn(connection_string_or_uri=conn_str)
    src = MSSQLSrc(conn, query, loader="polars_sqlalchemy")
    assert src.loader == "polars_sqlalchemy"


@pytest.mark.mssql
def test_loader_param_pandas():
    conn_str = (
        "DRIVER=ODBC Driver 18 for SQL Server;"
        "SERVER=localhost,1433;"
        "DATABASE=testing;"
        "TrustServerCertificate=yes;"
    )
    query = "select * from INVOICE_HEADER"
    conn = MSSQLConn(connection_string_or_uri=conn_str)
    src = MSSQLSrc(conn, query, loader="pandas")
    assert src.loader == "pandas"


@pytest.mark.mssql
def test_loader_param_invalid_raises_type_error():
    conn_str = (
        "DRIVER=ODBC Driver 18 for SQL Server;"
        "SERVER=localhost,1433;"
        "DATABASE=testing;"
        "TrustServerCertificate=yes;"
    )
    query = "select * from INVOICE_HEADER"
    conn = MSSQLConn(connection_string_or_uri=conn_str)
    with pytest.raises(ParameterValidationError) as e:
        MSSQLSrc(conn, query, loader="sqlalchemy")
    assert e.value.error_code == ErrorCode.SOCE108


@pytest.mark.mssql
def test_schema_overrides_accepts_dict_of_datatype():
    conn_str = (
        "DRIVER=ODBC Driver 18 for SQL Server;"
        "SERVER=localhost,1433;"
        "DATABASE=testing;"
        "TrustServerCertificate=yes;"
    )
    query = "select * from INVOICE_HEADER"
    conn = MSSQLConn(connection_string_or_uri=conn_str)
    schema_overrides = {"col1": Float64(), "col2": Int64()}
    src = MSSQLSrc(conn, query, schema_overrides=schema_overrides)
    assert src.schema_overrides == [schema_overrides]


@pytest.mark.mssql
def test_schema_overrides_accepts_list_of_dicts():
    conn_str = (
        "DRIVER=ODBC Driver 18 for SQL Server;"
        "SERVER=localhost,1433;"
        "DATABASE=testing;"
        "TrustServerCertificate=yes;"
    )
    query = ["select * from INVOICE_HEADER", "select * from INVOICE_ITEM"]
    conn = MSSQLConn(connection_string_or_uri=conn_str)
    schema_overrides = [{"col1": Float64()}, {"col2": Int64()}]
    src = MSSQLSrc(conn, query, schema_overrides=schema_overrides)
    assert src.schema_overrides == schema_overrides


@pytest.mark.mssql
def test_schema_overrides_none_is_allowed():
    conn_str = (
        "DRIVER=ODBC Driver 18 for SQL Server;"
        "SERVER=localhost,1433;"
        "DATABASE=testing;"
        "TrustServerCertificate=yes;"
    )
    query = "select * from INVOICE_HEADER"
    conn = MSSQLConn(connection_string_or_uri=conn_str)
    src = MSSQLSrc(conn, query, schema_overrides=None)
    assert src.schema_overrides is None


@pytest.mark.mssql
def test_schema_overrides_wrong_type_raises():
    conn_str = (
        "DRIVER=ODBC Driver 18 for SQL Server;"
        "SERVER=localhost,1433;"
        "DATABASE=testing;"
        "TrustServerCertificate=yes;"
    )
    query = "select * from INVOICE_HEADER"
    conn = MSSQLConn(connection_string_or_uri=conn_str)
    with pytest.raises(ParameterValidationError) as e:
        MSSQLSrc(conn, query, schema_overrides="not_a_dict")
    assert e.value.error_code == ErrorCode.SOCE108


@pytest.mark.mssql
def test_schema_overrides_list_with_non_dict_raises():
    conn_str = (
        "DRIVER=ODBC Driver 18 for SQL Server;"
        "SERVER=localhost,1433;"
        "DATABASE=testing;"
        "TrustServerCertificate=yes;"
    )
    query = ["select * from INVOICE_HEADER", "select * from INVOICE_ITEM"]
    conn = MSSQLConn(connection_string_or_uri=conn_str)
    schema_overrides = [{"col1": Float64()}, "not_a_dict"]
    with pytest.raises(ParameterValidationError) as e:
        MSSQLSrc(conn, query, schema_overrides=schema_overrides)
    assert e.value.error_code == ErrorCode.SOCE108


@pytest.mark.mssql
def test_schema_overrides_dict_with_non_str_key_raises():
    conn_str = (
        "DRIVER=ODBC Driver 18 for SQL Server;"
        "SERVER=localhost,1433;"
        "DATABASE=testing;"
        "TrustServerCertificate=yes;"
    )
    query = "select * from INVOICE_HEADER"
    conn = MSSQLConn(connection_string_or_uri=conn_str)
    schema_overrides = {42: Float64()}
    with pytest.raises(ParameterValidationError) as e:
        MSSQLSrc(conn, query, schema_overrides=schema_overrides)
    assert e.value.error_code == ErrorCode.SOCE108


@pytest.mark.mssql
def test_schema_overrides_dict_with_non_datatype_value_raises():
    conn_str = (
        "DRIVER=ODBC Driver 18 for SQL Server;"
        "SERVER=localhost,1433;"
        "DATABASE=testing;"
        "TrustServerCertificate=yes;"
    )
    query = "select * from INVOICE_HEADER"
    conn = MSSQLConn(connection_string_or_uri=conn_str)
    schema_overrides = {"col1": float()}
    with pytest.raises(ParameterValidationError) as e:
        MSSQLSrc(conn, query, schema_overrides=schema_overrides)
    assert e.value.error_code == ErrorCode.SOCE108


@pytest.mark.mssql
def test_schema_overrides_list_length_mismatch_raises():
    conn_str = (
        "DRIVER=ODBC Driver 18 for SQL Server;"
        "SERVER=localhost,1433;"
        "DATABASE=testing;"
        "TrustServerCertificate=yes;"
    )
    query = ["select * from INVOICE_HEADER", "select * from INVOICE_ITEM"]
    conn = MSSQLConn(connection_string_or_uri=conn_str)
    schema_overrides = [{"col1": Float64()}]
    with pytest.raises(SourceConfigurationError) as e:
        MSSQLSrc(conn, query, schema_overrides=schema_overrides)
    assert e.value.error_code == ErrorCode.SOCE57


@pytest.mark.mssql
def test_schema_overrides_dict_with_multiple_queries_raises():
    conn_str = (
        "DRIVER=ODBC Driver 18 for SQL Server;"
        "SERVER=localhost,1433;"
        "DATABASE=testing;"
        "TrustServerCertificate=yes;"
    )
    query = ["select * from INVOICE_HEADER", "select * from INVOICE_ITEM"]
    conn = MSSQLConn(connection_string_or_uri=conn_str)
    schema_overrides = {"col1": Float64()}
    with pytest.raises(SourceConfigurationError) as e:
        MSSQLSrc(conn, query, schema_overrides=schema_overrides)
    assert e.value.error_code == ErrorCode.SOCE58


@pytest.mark.mssql
def test_wrong_type_initial_values_key_raises_error():
    conn_str = (
        "DRIVER=ODBC Driver 18 for SQL Server;"
        "SERVER=localhost,1433;"
        "DATABASE=testing;"
        "TrustServerCertificate=yes;"
    )
    query = "select * from INVOICE_HEADER where id > :number"
    initial_values = {"a": 2, 42: 2}
    conn = MSSQLConn(connection_string_or_uri=conn_str)
    with pytest.raises(ParameterValidationError) as e:
        MSSQLSrc(conn, query, initial_values=initial_values)
    assert e.value.error_code == ErrorCode.SOCE108
