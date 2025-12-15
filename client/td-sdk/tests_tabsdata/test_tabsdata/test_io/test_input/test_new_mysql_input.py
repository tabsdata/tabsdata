#
# Copyright 2025 Tabs Data Inc.
#

import pytest

from tabsdata._credentials import S3AccessKeyCredentials, UserPasswordCredentials
from tabsdata._io.connections.mysql_connection import MySQLConn
from tabsdata._io.inputs.sql.mysql_input import MySQLSrc
from tabsdata._io.plugin import SourcePlugin
from tabsdata.exceptions import (
    ConnectionConfigurationError,
    ErrorCode,
    SourceConfigurationError,
)
from tabsdata.tableframe.dtypes import Float64, Int64
from tabsdata.tableframe.typing import DataType


def test_all_correct_query_list():
    uri = "mysql://DATABASE_IP:DATABASE_PORT/testing"
    query = [
        "select * from INVOICE_HEADER where id > 0",
        "select * from INVOICE_ITEM where id > 0",
    ]
    credentials = UserPasswordCredentials("admin", "admin")
    conn = MySQLConn(uri=uri, credentials=credentials)
    input = MySQLSrc(conn, query)
    assert input.conn == conn
    assert input.queries == query
    assert input.initial_values == {}
    assert isinstance(input, MySQLSrc)
    assert isinstance(input, SourcePlugin)
    assert input.__repr__()


def test_all_correct_query_string():
    uri = "mysql://DATABASE_IP:DATABASE_PORT/testing"
    query = "select * from INVOICE_HEADER where id > 0"
    credentials = UserPasswordCredentials("admin", "admin")
    conn = MySQLConn(uri=uri, credentials=credentials)
    input = MySQLSrc(conn, query)
    assert input.conn == conn
    assert input.queries == [query]
    assert input.initial_values == {}
    assert isinstance(input, MySQLSrc)
    assert isinstance(input, SourcePlugin)
    assert input.__repr__()


def test_all_correct_query_string_no_credentials():
    uri = "mysql://DATABASE_IP:DATABASE_PORT/testing"
    query = "select * from INVOICE_HEADER where id > 0"
    conn = MySQLConn(uri=uri)
    input = MySQLSrc(conn, query)
    assert input.conn == conn
    assert input.queries == [query]
    assert input.initial_values == {}
    assert isinstance(input, MySQLSrc)
    assert isinstance(input, SourcePlugin)
    assert input.__repr__()


def test_uri_upper():
    uri = "mysql://DATABASE_IP:DATABASE_PORT/testing".upper()
    query = "select * from INVOICE_HEADER where id > 0"
    conn = MySQLConn(uri=uri)
    input = MySQLSrc(conn, query)
    assert input.conn.uri == uri


def test_wrong_scheme_raises_value_error():
    uri = "wrongscheme://path/to/query"
    query = "select * from INVOICE_HEADER where id > 0"
    with pytest.raises(ConnectionConfigurationError) as e:
        conn = MySQLConn(uri=uri)
        MySQLSrc(conn, query)
    assert e.value.error_code == ErrorCode.CXCE2


def test_query_list():
    uri = "mysql://DATABASE_IP:DATABASE_PORT/testing"
    query = [
        "select * from INVOICE_HEADER where id > 0",
        "select * from INVOICE_ITEM where id > 0",
    ]
    conn = MySQLConn(uri=uri)
    input = MySQLSrc(conn, query)
    assert input.queries == query


def test_query_wrong_type_raises_type_error():
    uri = "mysql://DATABASE_IP:DATABASE_PORT/testing"
    query = 42
    conn = MySQLConn(uri=uri)
    with pytest.raises(SourceConfigurationError) as e:
        MySQLSrc(conn, query)
    assert e.value.error_code == ErrorCode.SOCE47


def test_query_list_wrong_type_raises_type_error():
    uri = "mysql://DATABASE_IP:DATABASE_PORT/testing"
    query = [
        "select * from INVOICE_HEADER where id > 0",
        "select * from INVOICE_ITEM where id > 0",
        42,
    ]
    conn = MySQLConn(uri=uri)
    with pytest.raises(SourceConfigurationError) as e:
        MySQLSrc(conn, query)
    assert e.value.error_code == ErrorCode.SOCE47


def test_wrong_credentials_type_raises_error():
    uri = "mysql://DATABASE_IP:DATABASE_PORT/testing"
    query = "select * from INVOICE_HEADER where id > 0"
    credentials = S3AccessKeyCredentials("access_key_id", "secret_access_key")
    with pytest.raises(ConnectionConfigurationError) as e:
        conn = MySQLConn(uri=uri, credentials=credentials)
        MySQLSrc(conn, query)
    assert e.value.error_code == ErrorCode.CXCE3


def test_empty_uri():
    uri = ""
    query = "select * from INVOICE_HEADER where id > 0"
    with pytest.raises(ConnectionConfigurationError) as e:
        conn = MySQLConn(uri=uri)
        MySQLSrc(conn, query)
    assert e.value.error_code == ErrorCode.CXCE2


def test_enforce_connection_params_default_true():
    uri = "mysql://DATABASE_IP:DATABASE_PORT/testing"
    conn = MySQLConn(uri=uri)
    assert conn.enforce_connection_params is True


def test_enforce_connection_params_setter():
    uri = "mysql://DATABASE_IP:DATABASE_PORT/testing"
    conn = MySQLConn(uri=uri)
    conn.enforce_connection_params = False
    assert conn.enforce_connection_params is False


def test_enforce_connection_params_invalid_type_raises():
    uri = "mysql://DATABASE_IP:DATABASE_PORT/testing"
    with pytest.raises(ConnectionConfigurationError) as e:
        MySQLConn(uri=uri, enforce_connection_params="not_bool")
    assert e.value.error_code == ErrorCode.CXCE4


def test_cx_src_configs_sql_default_and_setter():
    uri = "mysql://DATABASE_IP:DATABASE_PORT/testing"
    conn = MySQLConn(uri=uri)
    assert conn.cx_src_configs_sql == {}
    conn.cx_src_configs_sql = {"foo": "bar"}
    assert conn.cx_src_configs_sql == {"foo": "bar"}


def test_cx_src_configs_sql_invalid_type_raises():
    uri = "mysql://DATABASE_IP:DATABASE_PORT/testing"
    conn = MySQLConn(uri=uri)
    with pytest.raises(ConnectionConfigurationError) as e:
        conn.cx_src_configs_sql = "not_a_dict"
    assert e.value.error_code == ErrorCode.CXCE5


def test_cx_dst_configs_sql_default_and_setter():
    uri = "mysql://DATABASE_IP:DATABASE_PORT/testing"
    conn = MySQLConn(uri=uri)
    assert conn.cx_dst_configs_sql == {}
    conn.cx_dst_configs_sql = {"baz": 123}
    assert conn.cx_dst_configs_sql == {"baz": 123}


def test_cx_dst_configs_sql_invalid_type_raises():
    uri = "mysql://DATABASE_IP:DATABASE_PORT/testing"
    conn = MySQLConn(uri=uri)
    with pytest.raises(ConnectionConfigurationError) as e:
        conn.cx_dst_configs_sql = ["not", "a", "dict"]
    assert e.value.error_code == ErrorCode.CXCE6


def test_mysqlconn_eq_true_and_false():
    uri = "mysql://DATABASE_IP:DATABASE_PORT/testing"
    creds = UserPasswordCredentials("user", "pass")
    conn1 = MySQLConn(
        uri=uri,
        credentials=creds,
        cx_src_configs_mysql={"foo": "bar"},
        cx_dst_configs_mysql={"baz": 123},
    )
    conn2 = MySQLConn(
        uri=uri,
        credentials=creds,
        cx_src_configs_mysql={"foo": "bar"},
        cx_dst_configs_mysql={"baz": 123},
    )
    conn3 = MySQLConn(
        uri=uri,
        credentials=UserPasswordCredentials("user", "pass1"),
        enforce_connection_params=False,
        cx_src_configs_mysql={"foo": "bar"},
        cx_dst_configs_mysql={"baz": 123},
    )
    assert conn1 == conn2
    assert not (conn1 == conn3)
    conn1.enforce_connection_params = False
    conn3.credentials = creds
    assert conn1 == conn3
    assert conn1 != "not_a_mysqlconn"


def test_all_correct_initial_values():
    uri = "mysql://DATABASE_IP:DATABASE_PORT/testing"
    query = [
        "select * from INVOICE_HEADER where id > :number",
        "select * from INVOICE_ITEM where id > :number",
    ]
    credentials = UserPasswordCredentials("admin", "admin")
    initial_values = {
        "number": "0",
    }
    conn = MySQLConn(uri=uri, credentials=credentials)
    input = MySQLSrc(conn, query, initial_values=initial_values)
    assert input.conn == conn
    assert input.queries == query
    assert input.initial_values == initial_values
    assert isinstance(input, MySQLSrc)
    assert isinstance(input, SourcePlugin)
    assert input.__repr__()


def test_wrong_type_initial_values_raises_type_error():
    uri = "mysql://DATABASE_IP:DATABASE_PORT/testing"
    query = "select * from INVOICE_HEADER where id > :number"
    initial_values = 42
    conn = MySQLConn(uri=uri)
    with pytest.raises(SourceConfigurationError) as e:
        MySQLSrc(conn, query, initial_values=initial_values)
    assert e.value.error_code == ErrorCode.SOCE48


def test_none_initial_values_converted_empty_dict():
    uri = "mysql://DATABASE_IP:DATABASE_PORT/testing"
    query = "select * from INVOICE_HEADER where id > 0"
    initial_values = None
    conn = MySQLConn(uri=uri)
    input = MySQLSrc(conn, query, initial_values=initial_values)
    assert input.initial_values == {}


def test_update_initial_values():
    uri = "mysql://DATABASE_IP:DATABASE_PORT/testing"
    query = "select * from INVOICE_HEADER where id > :number"
    credentials = UserPasswordCredentials("admin", "admin")
    initial_values = {
        "number": "0",
    }
    conn = MySQLConn(uri=uri, credentials=credentials)
    input = MySQLSrc(conn, query, initial_values=initial_values)
    assert input.initial_values == initial_values
    initial_values["number"] = "0"
    assert input.initial_values == initial_values
    input.initial_values = {"number": "2"}
    assert input.initial_values == {"number": "2"}


def test_update_queries():
    uri = "mysql://DATABASE_IP:DATABASE_PORT/testing"
    query = "select * from INVOICE_HEADER where id > :number"
    credentials = UserPasswordCredentials("admin", "admin")
    conn = MySQLConn(uri=uri, credentials=credentials)
    input = MySQLSrc(conn, query)
    assert input.queries == [query]
    query = "select * from ITEMS_HEADER where id > :number"
    input.queries = query
    assert input.queries == [query]


def test_update_credentials():
    uri = "mysql://DATABASE_IP:DATABASE_PORT/testing"
    query = "select * from INVOICE_HEADER where id > 0"
    credentials = UserPasswordCredentials("admin", "admin")
    conn = MySQLConn(uri=uri, credentials=credentials)
    input = MySQLSrc(conn, query)
    assert input.conn.credentials == credentials
    credentials2 = UserPasswordCredentials("admin2", "admin2")
    input.conn.credentials = credentials2
    assert input.conn.credentials == credentials2
    input.conn.credentials = None
    assert input.conn.credentials is None


def test_update_uri():
    uri = "mysql://DATABASE_IP:DATABASE_PORT/testing"
    query = "select * from INVOICE_HEADER where id > 0"
    conn = MySQLConn(uri=uri)
    input = MySQLSrc(conn, query)
    assert input.conn.uri == uri
    uri2 = "mysql://DATABASE_IP:3308/testing2"
    input.conn.uri = uri2
    assert input.conn.uri == uri2


def test_driver_uri_fails():
    uri = "mysql+driver://DATABASE_IP:DATABASE_PORT/testing"
    query = "select * from INVOICE_HEADER where id > :number"
    with pytest.raises(ConnectionConfigurationError) as e:
        conn = MySQLConn(uri=uri)
        MySQLSrc(conn, query)
    assert e.value.error_code == ErrorCode.CXCE2


def test_transactional_default_true():
    uri = "mysql://DATABASE_IP:DATABASE_PORT/testing"
    query = "select * from INVOICE_HEADER"
    conn = MySQLConn(uri=uri)
    input = MySQLSrc(conn, query)
    assert input.transactional is True


def test_transactional_false():
    uri = "mysql://DATABASE_IP:DATABASE_PORT/testing"
    query = "select * from INVOICE_HEADER"
    conn = MySQLConn(uri=uri)
    input = MySQLSrc(conn, query, transactional=False)
    assert input.transactional is False


def test_transactional_invalid_type_raises():
    uri = "mysql://DATABASE_IP:DATABASE_PORT/testing"
    query = "select * from INVOICE_HEADER"
    conn = MySQLConn(uri=uri)
    with pytest.raises(SourceConfigurationError) as e:
        MySQLSrc(conn, query, transactional=1)
    assert e.value.error_code == ErrorCode.SOCE49


def test_chunk_size_default():
    uri = "mysql://DATABASE_IP:DATABASE_PORT/testing"
    query = "select * from INVOICE_HEADER"
    conn = MySQLConn(uri=uri)
    input = MySQLSrc(conn, query)
    assert input.chunk_size == 100000


def test_chunk_size_custom():
    uri = "mysql://DATABASE_IP:DATABASE_PORT/testing"
    query = "select * from INVOICE_HEADER"
    conn = MySQLConn(uri=uri)
    input = MySQLSrc(conn, query, chunk_size=5000)
    assert input.chunk_size == 5000


def test_chunk_size_invalid_type_raises():
    uri = "mysql://DATABASE_IP:DATABASE_PORT/testing"
    query = "select * from INVOICE_HEADER"
    conn = MySQLConn(uri=uri)
    with pytest.raises(SourceConfigurationError) as e:
        MySQLSrc(conn, query, chunk_size="not_an_int")
    assert e.value.error_code == ErrorCode.SOCE50


def test_chunk_size_invalid_value_raises():
    uri = "mysql://DATABASE_IP:DATABASE_PORT/testing"
    query = "select * from INVOICE_HEADER"
    conn = MySQLConn(uri=uri)
    with pytest.raises(SourceConfigurationError) as e:
        MySQLSrc(conn, query, chunk_size=-2)
    assert e.value.error_code == ErrorCode.SOCE51


def test_loader_param_polars_sqlalchemy():
    uri = "mysql://DATABASE_IP:DATABASE_PORT/testing"
    query = "select * from INVOICE_HEADER"
    conn = MySQLConn(uri=uri)
    input = MySQLSrc(conn, query, loader="polars_sqlalchemy")
    assert input.loader == "polars_sqlalchemy"


def test_loader_param_pandas():
    uri = "mysql://DATABASE_IP:DATABASE_PORT/testing"
    query = "select * from INVOICE_HEADER"
    conn = MySQLConn(uri=uri)
    input = MySQLSrc(conn, query, loader="pandas")
    assert input.loader == "pandas"


def test_update_loader_param():
    uri = "mysql://DATABASE_IP:DATABASE_PORT/testing"
    query = "select * from INVOICE_HEADER"
    conn = MySQLConn(uri=uri)
    input = MySQLSrc(conn, query, loader="pandas")
    input.loader = "polars_sqlalchemy"
    assert input.loader == "polars_sqlalchemy"


def test_loader_param_invalid_raises_type_error():
    uri = "mysql://DATABASE_IP:DATABASE_PORT/testing"
    query = "select * from INVOICE_HEADER"
    conn = MySQLConn(uri=uri)
    with pytest.raises(SourceConfigurationError) as e:
        MySQLSrc(conn, query, loader="sqlalchemy")
    assert e.value.error_code == ErrorCode.SOCE52


def test_schema_overrides_accepts_dict_of_datatype():
    uri = "mysql://DATABASE_IP:DATABASE_PORT/testing"
    query = "select * from INVOICE_HEADER"
    conn = MySQLConn(uri=uri)
    schema_overrides = {"col1": Float64, "col2": Int64}
    src = MySQLSrc(conn, query, schema_overrides=schema_overrides)
    assert src.schema_overrides == [schema_overrides]


def test_schema_overrides_accepts_list_of_dicts():
    uri = "mysql://DATABASE_IP:DATABASE_PORT/testing"
    query = ["select * from INVOICE_HEADER", "select * from INVOICE_ITEM"]
    conn = MySQLConn(uri=uri)
    schema_overrides = [{"col1": Float64}, {"col2": Int64}]
    src = MySQLSrc(conn, query, schema_overrides=schema_overrides)
    assert src.schema_overrides == schema_overrides


def test_schema_overrides_none_is_allowed():
    uri = "mysql://DATABASE_IP:DATABASE_PORT/testing"
    query = "select * from INVOICE_HEADER"
    conn = MySQLConn(uri=uri)
    src = MySQLSrc(conn, query, schema_overrides=None)
    assert src.schema_overrides is None


def test_schema_overrides_wrong_type_raises():
    uri = "mysql://DATABASE_IP:DATABASE_PORT/testing"
    query = "select * from INVOICE_HEADER"
    conn = MySQLConn(uri=uri)
    with pytest.raises(SourceConfigurationError) as e:
        MySQLSrc(conn, query, schema_overrides="not_a_dict")
    assert e.value.error_code == ErrorCode.SOCE53


def test_schema_overrides_list_with_non_dict_raises():
    uri = "mysql://DATABASE_IP:DATABASE_PORT/testing"
    query = ["select * from INVOICE_HEADER", "select * from INVOICE_ITEM"]
    conn = MySQLConn(uri=uri)
    schema_overrides = [{"col1": Float64}, "not_a_dict"]
    with pytest.raises(SourceConfigurationError) as e:
        MySQLSrc(conn, query, schema_overrides=schema_overrides)
    assert e.value.error_code == ErrorCode.SOCE54


def test_schema_overrides_dict_with_non_str_key_raises():
    uri = "mysql://DATABASE_IP:DATABASE_PORT/testing"
    query = "select * from INVOICE_HEADER"
    conn = MySQLConn(uri=uri)
    schema_overrides = {42: Float64}
    with pytest.raises(SourceConfigurationError) as e:
        MySQLSrc(conn, query, schema_overrides=schema_overrides)
    assert e.value.error_code == ErrorCode.SOCE55


def test_schema_overrides_dict_with_non_datatype_value_raises():
    uri = "mysql://DATABASE_IP:DATABASE_PORT/testing"
    query = "select * from INVOICE_HEADER"
    conn = MySQLConn(uri=uri)
    schema_overrides = {"col1": float}
    with pytest.raises(SourceConfigurationError) as e:
        MySQLSrc(conn, query, schema_overrides=schema_overrides)
    assert e.value.error_code == ErrorCode.SOCE56


def test_schema_overrides_list_length_mismatch_raises():
    uri = "mysql://DATABASE_IP:DATABASE_PORT/testing"
    query = ["select * from INVOICE_HEADER", "select * from INVOICE_ITEM"]
    conn = MySQLConn(uri=uri)
    schema_overrides = [{"col1": Float64}]
    with pytest.raises(SourceConfigurationError) as e:
        MySQLSrc(conn, query, schema_overrides=schema_overrides)
    assert e.value.error_code == ErrorCode.SOCE57


def test_schema_overrides_dict_with_multiple_queries_raises():
    uri = "mysql://DATABASE_IP:DATABASE_PORT/testing"
    query = ["select * from INVOICE_HEADER", "select * from INVOICE_ITEM"]
    conn = MySQLConn(uri=uri)
    schema_overrides = {"col1": Float64}
    with pytest.raises(SourceConfigurationError) as e:
        MySQLSrc(conn, query, schema_overrides=schema_overrides)
    assert e.value.error_code == ErrorCode.SOCE58


def test_wrong_type_initial_values_key_raises_error():
    uri = "mysql://DATABASE_IP:DATABASE_PORT/testing"
    query = "select * from INVOICE_HEADER where id > :number"
    initial_values = {42: "42"}
    conn = MySQLConn(uri=uri)
    with pytest.raises(SourceConfigurationError) as e:
        MySQLSrc(conn, query, initial_values=initial_values)
    assert e.value.error_code == ErrorCode.SOCE40
