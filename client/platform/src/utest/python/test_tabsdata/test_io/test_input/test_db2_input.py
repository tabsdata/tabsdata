#
# Copyright 2026 Tabsdata Inc.
#

import pytest

from tabsdata._credentials import S3AccessKeyCredentials, UserPasswordCredentials
from tabsdata._io.connections.db2_connection import Db2Conn
from tabsdata._io.inputs.sql.db2_input import Db2Src
from tabsdata._io.plugin import SourcePlugin
from tabsdata.exceptions import (
    ConnectionConfigurationError,
    ErrorCode,
    ParameterValidationError,
    SourceConfigurationError,
)
from tabsdata.tableframe.dtypes import Float64, Int64

pytestmark = pytest.mark.db2

def test_all_correct_query_list():
    uri = "db2://DATABASE_IP:DATABASE_PORT/testing"
    query = [
        "select * from INVOICE_HEADER where id > 0",
        "select * from INVOICE_ITEM where id > 0",
    ]
    credentials = UserPasswordCredentials("admin", "admin")
    conn = Db2Conn(uri=uri, credentials=credentials)
    input = Db2Src(conn, query)
    assert input.conn == conn
    assert input.queries == query
    assert input.initial_values == {}
    assert isinstance(input, Db2Src)
    assert isinstance(input, SourcePlugin)
    assert input.__repr__()


def test_all_correct_query_string():
    uri = "db2://DATABASE_IP:DATABASE_PORT/testing"
    query = "select * from INVOICE_HEADER where id > 0"
    credentials = UserPasswordCredentials("admin", "admin")
    conn = Db2Conn(uri=uri, credentials=credentials)
    input = Db2Src(conn, query)
    assert input.conn == conn
    assert input.queries == [query]
    assert input.initial_values == {}
    assert isinstance(input, Db2Src)
    assert isinstance(input, SourcePlugin)
    assert input.__repr__()


def test_all_correct_query_string_no_credentials():
    uri = "db2://DATABASE_IP:DATABASE_PORT/testing"
    query = "select * from INVOICE_HEADER where id > 0"
    conn = Db2Conn(uri=uri)
    input = Db2Src(conn, query)
    assert input.conn == conn
    assert input.queries == [query]
    assert input.initial_values == {}
    assert isinstance(input, Db2Src)
    assert isinstance(input, SourcePlugin)
    assert input.__repr__()


def test_uri_upper():
    uri = "db2://DATABASE_IP:DATABASE_PORT/testing".upper()
    query = "select * from INVOICE_HEADER where id > 0"
    conn = Db2Conn(uri=uri)
    input = Db2Src(conn, query)
    assert input.conn.uri == uri


def test_wrong_scheme_raises_value_error():
    uri = "wrongscheme://path/to/query"
    query = "select * from INVOICE_HEADER where id > 0"
    with pytest.raises(ConnectionConfigurationError) as e:
        conn = Db2Conn(uri=uri)
        Db2Src(conn, query)
    assert e.value.error_code == ErrorCode.CXCE9


def test_query_list():
    uri = "db2://DATABASE_IP:DATABASE_PORT/testing"
    query = [
        "select * from INVOICE_HEADER where id > 0",
        "select * from INVOICE_ITEM where id > 0",
    ]
    conn = Db2Conn(uri=uri)
    input = Db2Src(conn, query)
    assert input.queries == query


def test_query_wrong_type_raises_type_error():
    uri = "db2://DATABASE_IP:DATABASE_PORT/testing"
    query = 42
    conn = Db2Conn(uri=uri)
    with pytest.raises(ParameterValidationError) as e:
        Db2Src(conn, query)
    assert e.value.error_code == ErrorCode.SOCE111


def test_query_list_wrong_type_raises_type_error():
    uri = "db2://DATABASE_IP:DATABASE_PORT/testing"
    query = [
        "select * from INVOICE_HEADER where id > 0",
        "select * from INVOICE_ITEM where id > 0",
        42,
    ]
    conn = Db2Conn(uri=uri)
    with pytest.raises(ParameterValidationError) as e:
        Db2Src(conn, query)
    assert e.value.error_code == ErrorCode.SOCE111


def test_wrong_credentials_type_raises_error():
    uri = "db2://DATABASE_IP:DATABASE_PORT/testing"
    query = "select * from INVOICE_HEADER where id > 0"
    credentials = S3AccessKeyCredentials("access_key_id", "secret_access_key")
    with pytest.raises(ParameterValidationError) as e:
        conn = Db2Conn(uri=uri, credentials=credentials)
        Db2Src(conn, query)
    assert e.value.error_code == ErrorCode.DECE108


def test_empty_uri():
    uri = ""
    query = "select * from INVOICE_HEADER where id > 0"
    with pytest.raises(ConnectionConfigurationError) as e:
        conn = Db2Conn(uri=uri)
        Db2Src(conn, query)
    assert e.value.error_code == ErrorCode.CXCE9


def test_enforce_connection_params_default_true():
    uri = "db2://DATABASE_IP:DATABASE_PORT/testing"
    conn = Db2Conn(uri=uri)
    assert conn.enforce_connection_params is True


def test_enforce_connection_params_setter():
    uri = "db2://DATABASE_IP:DATABASE_PORT/testing"
    conn = Db2Conn(uri=uri, enforce_connection_params=False)
    assert conn.enforce_connection_params is False


def test_enforce_connection_params_invalid_type_raises():
    uri = "db2://DATABASE_IP:DATABASE_PORT/testing"
    with pytest.raises(ParameterValidationError) as e:
        Db2Conn(uri=uri, enforce_connection_params="not_bool")
    assert e.value.error_code == ErrorCode.DECE108


def test_cx_src_configs_db2_default_and_setter():
    uri = "db2://DATABASE_IP:DATABASE_PORT/testing"
    conn = Db2Conn(uri=uri)
    assert conn.cx_src_configs_db2 == {}
    conn = Db2Conn(uri=uri, cx_src_configs_db2={"foo": "bar"})
    assert conn.cx_src_configs_db2 == {"foo": "bar"}


def test_cx_src_configs_db2_invalid_type_raises():
    uri = "db2://DATABASE_IP:DATABASE_PORT/testing"
    conn = Db2Conn(uri=uri)
    with pytest.raises(ParameterValidationError) as e:
        Db2Conn(uri=uri, cx_src_configs_db2="not_a_dict")
    assert e.value.error_code == ErrorCode.DECE108


def test_cx_dst_configs_db2_default_and_setter():
    uri = "db2://DATABASE_IP:DATABASE_PORT/testing"
    conn = Db2Conn(uri=uri)
    assert conn.cx_dst_configs_db2 == {}
    conn = Db2Conn(uri=uri, cx_dst_configs_db2={"baz": 123})
    assert conn.cx_dst_configs_db2 == {"baz": 123}


def test_cx_dst_configs_db2_invalid_type_raises():
    uri = "db2://DATABASE_IP:DATABASE_PORT/testing"
    conn = Db2Conn(uri=uri)
    with pytest.raises(ParameterValidationError) as e:
        Db2Conn(uri=uri, cx_dst_configs_db2="not_a_dict")
    assert e.value.error_code == ErrorCode.DECE108


def test_db2conn_eq_true_and_false():
    uri = "db2://DATABASE_IP:DATABASE_PORT/testing"
    creds = UserPasswordCredentials("user", "pass")
    conn1 = Db2Conn(
        uri=uri,
        credentials=creds,
        cx_src_configs_db2={"foo": "bar"},
        cx_dst_configs_db2={"baz": 123},
    )
    conn2 = Db2Conn(
        uri=uri,
        credentials=creds,
        cx_src_configs_db2={"foo": "bar"},
        cx_dst_configs_db2={"baz": 123},
    )
    conn3 = Db2Conn(
        uri=uri,
        credentials=UserPasswordCredentials("user", "pass1"),
        enforce_connection_params=False,
        cx_src_configs_db2={"foo": "bar"},
        cx_dst_configs_db2={"baz": 1234},
    )
    assert conn1 == conn2
    assert not (conn1 == conn3)
    assert conn1 != "not_a_db2conn"


def test_all_correct_initial_values():
    uri = "db2://DATABASE_IP:DATABASE_PORT/testing"
    query = [
        "select * from INVOICE_HEADER where id > :number",
        "select * from INVOICE_ITEM where id > :number",
    ]
    credentials = UserPasswordCredentials("admin", "admin")
    initial_values = {
        "number": "0",
    }
    conn = Db2Conn(uri=uri, credentials=credentials)
    input = Db2Src(conn, query, initial_values=initial_values)
    assert input.conn == conn
    assert input.queries == query
    assert input.initial_values == initial_values
    assert isinstance(input, Db2Src)
    assert isinstance(input, SourcePlugin)
    assert input.__repr__()


def test_wrong_type_initial_values_raises_type_error():
    uri = "db2://DATABASE_IP:DATABASE_PORT/testing"
    query = "select * from INVOICE_HEADER where id > :number"
    initial_values = 42
    conn = Db2Conn(uri=uri)
    with pytest.raises(ParameterValidationError) as e:
        Db2Src(conn, query, initial_values=initial_values)
    assert e.value.error_code == ErrorCode.SOCE111


def test_none_initial_values_converted_empty_dict():
    uri = "db2://DATABASE_IP:DATABASE_PORT/testing"
    query = "select * from INVOICE_HEADER where id > 0"
    initial_values = None
    conn = Db2Conn(uri=uri)
    input = Db2Src(conn, query, initial_values=initial_values)
    assert input.initial_values == {}


def test_update_initial_values():
    uri = "db2://DATABASE_IP:DATABASE_PORT/testing"
    query = "select * from INVOICE_HEADER where id > :number"
    credentials = UserPasswordCredentials("admin", "admin")
    initial_values = {
        "number": "0",
    }
    conn = Db2Conn(uri=uri, credentials=credentials)
    input = Db2Src(conn, query, initial_values=initial_values)
    assert input.initial_values == initial_values
    initial_values["number"] = "0"
    assert input.initial_values == initial_values
    input.initial_values = {"number": "2"}
    assert input.initial_values == {"number": "2"}


def test_transactional_default_true():
    uri = "db2://DATABASE_IP:DATABASE_PORT/testing"
    query = "select * from INVOICE_HEADER"
    conn = Db2Conn(uri=uri)
    input = Db2Src(conn, query)
    assert input.transactional is True


def test_transactional_false():
    uri = "db2://DATABASE_IP:DATABASE_PORT/testing"
    query = "select * from INVOICE_HEADER"
    conn = Db2Conn(uri=uri)
    input = Db2Src(conn, query, transactional=False)
    assert input.transactional is False


def test_transactional_invalid_type_raises():
    uri = "db2://DATABASE_IP:DATABASE_PORT/testing"
    query = "select * from INVOICE_HEADER"
    conn = Db2Conn(uri=uri)
    with pytest.raises(ParameterValidationError) as e:
        Db2Src(conn, query, transactional=3)
    assert e.value.error_code == ErrorCode.SOCE111


def test_chunk_size_default():
    uri = "db2://DATABASE_IP:DATABASE_PORT/testing"
    query = "select * from INVOICE_HEADER"
    conn = Db2Conn(uri=uri)
    input = Db2Src(conn, query)
    assert input.chunk_size == 100000


def test_chunk_size_custom():
    uri = "db2://DATABASE_IP:DATABASE_PORT/testing"
    query = "select * from INVOICE_HEADER"
    conn = Db2Conn(uri=uri)
    input = Db2Src(conn, query, chunk_size=5000)
    assert input.chunk_size == 5000


def test_chunk_size_invalid_type_raises():
    uri = "db2://DATABASE_IP:DATABASE_PORT/testing"
    query = "select * from INVOICE_HEADER"
    conn = Db2Conn(uri=uri)
    with pytest.raises(ParameterValidationError) as e:
        Db2Src(conn, query, chunk_size="not_an_int")
    assert e.value.error_code == ErrorCode.SOCE111


def test_chunk_size_invalid_value_raises():
    uri = "db2://DATABASE_IP:DATABASE_PORT/testing"
    query = "select * from INVOICE_HEADER"
    conn = Db2Conn(uri=uri)
    with pytest.raises(ParameterValidationError) as e:
        Db2Src(conn, query, chunk_size=-2)
    assert e.value.error_code == ErrorCode.SOCE111


def test_loader_param_polars_sqlalchemy():
    uri = "db2://DATABASE_IP:DATABASE_PORT/testing"
    query = "select * from INVOICE_HEADER"
    conn = Db2Conn(uri=uri)
    input = Db2Src(conn, query, loader="polars_sqlalchemy")
    assert input.loader == "polars_sqlalchemy"


def test_loader_param_pandas():
    uri = "db2://DATABASE_IP:DATABASE_PORT/testing"
    query = "select * from INVOICE_HEADER"
    conn = Db2Conn(uri=uri)
    input = Db2Src(conn, query, loader="pandas")
    assert input.loader == "pandas"


def test_loader_param_invalid_raises_type_error():
    uri = "db2://DATABASE_IP:DATABASE_PORT/testing"
    query = "select * from INVOICE_HEADER"
    conn = Db2Conn(uri=uri)
    with pytest.raises(ParameterValidationError) as e:
        Db2Src(conn, query, loader="sqlalchemy")
    assert e.value.error_code == ErrorCode.SOCE111


def test_schema_overrides_accepts_dict_of_datatype():
    uri = "db2://DATABASE_IP:DATABASE_PORT/testing"
    query = "select * from INVOICE_HEADER"
    conn = Db2Conn(uri=uri)
    schema_overrides = {"col1": Float64(), "col2": Int64()}
    src = Db2Src(conn, query, schema_overrides=schema_overrides)
    assert src.schema_overrides == [schema_overrides]


def test_schema_overrides_accepts_list_of_dicts():
    uri = "db2://DATABASE_IP:DATABASE_PORT/testing"
    query = ["select * from INVOICE_HEADER", "select * from INVOICE_ITEM"]
    conn = Db2Conn(uri=uri)
    schema_overrides = [{"col1": Float64()}, {"col2": Int64()}]
    src = Db2Src(conn, query, schema_overrides=schema_overrides)
    assert src.schema_overrides == schema_overrides


def test_schema_overrides_none_is_allowed():
    uri = "db2://DATABASE_IP:DATABASE_PORT/testing"
    query = "select * from INVOICE_HEADER"
    conn = Db2Conn(uri=uri)
    src = Db2Src(conn, query, schema_overrides=None)
    assert src.schema_overrides is None


def test_schema_overrides_wrong_type_raises():
    uri = "db2://DATABASE_IP:DATABASE_PORT/testing"
    query = "select * from INVOICE_HEADER"
    conn = Db2Conn(uri=uri)
    with pytest.raises(ParameterValidationError) as e:
        Db2Src(conn, query, schema_overrides="not_a_dict")
    assert e.value.error_code == ErrorCode.SOCE111


def test_schema_overrides_list_with_non_dict_raises():
    uri = "db2://DATABASE_IP:DATABASE_PORT/testing"
    query = ["select * from INVOICE_HEADER", "select * from INVOICE_ITEM"]
    conn = Db2Conn(uri=uri)
    schema_overrides = [{"col1": Float64}, "not_a_dict"]
    with pytest.raises(ParameterValidationError) as e:
        Db2Src(conn, query, schema_overrides=schema_overrides)
    assert e.value.error_code == ErrorCode.SOCE111


def test_schema_overrides_dict_with_non_str_key_raises():
    uri = "db2://DATABASE_IP:DATABASE_PORT/testing"
    query = "select * from INVOICE_HEADER"
    conn = Db2Conn(uri=uri)
    schema_overrides = {42: Float64}
    with pytest.raises(ParameterValidationError) as e:
        Db2Src(conn, query, schema_overrides=schema_overrides)
    assert e.value.error_code == ErrorCode.SOCE111


def test_schema_overrides_dict_with_non_datatype_value_raises():
    uri = "db2://DATABASE_IP:DATABASE_PORT/testing"
    query = "select * from INVOICE_HEADER"
    conn = Db2Conn(uri=uri)
    schema_overrides = {"col1": float}
    with pytest.raises(ParameterValidationError) as e:
        Db2Src(conn, query, schema_overrides=schema_overrides)
    assert e.value.error_code == ErrorCode.SOCE111


def test_schema_overrides_list_length_mismatch_raises():
    uri = "db2://DATABASE_IP:DATABASE_PORT/testing"
    query = ["select * from INVOICE_HEADER", "select * from INVOICE_ITEM"]
    conn = Db2Conn(uri=uri)
    schema_overrides = [{"col1": Float64()}]
    with pytest.raises(SourceConfigurationError) as e:
        Db2Src(conn, query, schema_overrides=schema_overrides)
    assert e.value.error_code == ErrorCode.SOCE57


def test_schema_overrides_dict_with_multiple_queries_raises():
    uri = "db2://DATABASE_IP:DATABASE_PORT/testing"
    query = ["select * from INVOICE_HEADER", "select * from INVOICE_ITEM"]
    conn = Db2Conn(uri=uri)
    schema_overrides = {"col1": Float64()}
    with pytest.raises(SourceConfigurationError) as e:
        Db2Src(conn, query, schema_overrides=schema_overrides)
    assert e.value.error_code == ErrorCode.SOCE58


def test_wrong_type_initial_values_key_raises_error():
    uri = "db2://DATABASE_IP:DATABASE_PORT/testing"
    query = "select * from INVOICE_HEADER where id > :number"
    initial_values = {42: "42"}
    conn = Db2Conn(uri=uri)
    with pytest.raises(ParameterValidationError) as e:
        Db2Src(conn, query, initial_values=initial_values)
    assert e.value.error_code == ErrorCode.SOCE111
