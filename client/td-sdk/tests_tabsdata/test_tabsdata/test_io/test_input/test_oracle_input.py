#
# Copyright 2024 Tabs Data Inc.
#

import pytest

from tabsdata.credentials import S3AccessKeyCredentials, UserPasswordCredentials
from tabsdata.exceptions import ErrorCode, InputConfigurationError
from tabsdata.io.input import Input, OracleSource, build_input

QUERY_KEY = OracleSource.QUERY_KEY
URI_KEY = OracleSource.URI_KEY


def test_all_correct_query_list():
    uri = "oracle://DATABASE_IP:DATABASE_PORT/testing"
    query = [
        "select * from INVOICE_HEADER where id > 0",
        "select * from INVOICE_ITEM where id > 0",
    ]
    credentials = UserPasswordCredentials("admin", "admin")
    input = OracleSource(uri, query, credentials=credentials)
    assert input.uri == uri
    assert input.query == query
    assert input.credentials == credentials
    assert input.initial_values == {}
    assert isinstance(input, OracleSource)
    assert isinstance(input, Input)
    expected_dict = {
        OracleSource.IDENTIFIER: {
            URI_KEY: uri,
            QUERY_KEY: query,
            OracleSource.CREDENTIALS_KEY: credentials.to_dict(),
            OracleSource.INITIAL_VALUES_KEY: {},
        }
    }
    assert input.to_dict() == expected_dict
    assert input.__repr__()
    assert isinstance(build_input(input.to_dict()), OracleSource)


def test_identifier_string_unchanged():
    uri = "oracle://DATABASE_IP:DATABASE_PORT/testing"
    query = [
        "select * from INVOICE_HEADER where id > 0",
        "select * from INVOICE_ITEM where id > 0",
    ]
    input = OracleSource(uri, query)
    expected_dict = {
        "oracle-input": {
            URI_KEY: uri,
            QUERY_KEY: query,
            OracleSource.CREDENTIALS_KEY: None,
            OracleSource.INITIAL_VALUES_KEY: {},
        }
    }
    assert input.to_dict() == expected_dict
    assert input.__repr__()
    assert isinstance(build_input(input.to_dict()), OracleSource)


def test_all_correct_query_string():
    uri = "oracle://DATABASE_IP:DATABASE_PORT/testing"
    query = "select * from INVOICE_HEADER where id > 0"
    credentials = UserPasswordCredentials("admin", "admin")
    input = OracleSource(uri, query, credentials=credentials)
    assert input.uri == uri
    assert input.query == query
    assert input.credentials == credentials
    assert input.initial_values == {}
    assert isinstance(input, OracleSource)
    assert isinstance(input, Input)
    expected_dict = {
        OracleSource.IDENTIFIER: {
            URI_KEY: uri,
            QUERY_KEY: query,
            OracleSource.CREDENTIALS_KEY: credentials.to_dict(),
            OracleSource.INITIAL_VALUES_KEY: {},
        }
    }
    assert input.to_dict() == expected_dict
    assert input.__repr__()
    assert isinstance(build_input(input.to_dict()), OracleSource)


def test_same_input_eq():
    uri = "oracle://DATABASE_IP:DATABASE_PORT/testing"
    query = "select * from INVOICE_HEADER where id > 0"
    credentials = UserPasswordCredentials("admin", "admin")
    input = OracleSource(uri, query, credentials=credentials)
    input2 = OracleSource(uri, query, credentials=credentials)
    assert input == input2


def test_different_input_not_eq():
    uri = "oracle://DATABASE_IP:DATABASE_PORT/testing"
    query = "select * from INVOICE_HEADER where id > 0"
    credentials = UserPasswordCredentials("admin", "admin")
    input = OracleSource(uri, query, credentials=credentials)
    uri2 = "oracle://DATABASE_IP:3308/testing"
    input2 = OracleSource(uri2, query, credentials=credentials)
    assert input != input2


def test_input_not_eq_dict():
    uri = "oracle://DATABASE_IP:DATABASE_PORT/testing"
    query = "select * from INVOICE_HEADER where id > 0"
    credentials = UserPasswordCredentials("admin", "admin")
    input = OracleSource(uri, query, credentials=credentials)
    assert input.to_dict() != input


def test_all_correct_query_string_no_credentials():
    uri = "oracle://DATABASE_IP:DATABASE_PORT/testing"
    query = "select * from INVOICE_HEADER where id > 0"
    input = OracleSource(uri, query)
    assert input.uri == uri
    assert input.query == query
    assert input.credentials is None
    assert isinstance(input, OracleSource)
    assert isinstance(input, Input)
    expected_dict = {
        OracleSource.IDENTIFIER: {
            URI_KEY: uri,
            QUERY_KEY: query,
            OracleSource.CREDENTIALS_KEY: None,
            OracleSource.INITIAL_VALUES_KEY: {},
        }
    }
    assert input.to_dict() == expected_dict
    assert input.__repr__()
    assert isinstance(build_input(input.to_dict()), OracleSource)


def test_wrong_scheme_raises_value_error():
    uri = "wrongscheme://path/to/query"
    query = "select * from INVOICE_HEADER where id > 0"
    with pytest.raises(InputConfigurationError) as e:
        OracleSource(uri, query)
    assert e.value.error_code == ErrorCode.ICE2


def test_query_list():
    uri = "oracle://DATABASE_IP:DATABASE_PORT/testing"
    query = [
        "select * from INVOICE_HEADER where id > 0",
        "select * from INVOICE_ITEM where id > 0",
    ]
    input = OracleSource(uri, query)
    assert input.query == query


def test_query_wrong_type_raises_type_error():
    uri = "oracle://DATABASE_IP:DATABASE_PORT/testing"
    query = 42
    with pytest.raises(InputConfigurationError) as e:
        OracleSource(uri, query)
    assert e.value.error_code == ErrorCode.ICE38


def test_query_list_wrong_type_raises_type_error():
    uri = "oracle://DATABASE_IP:DATABASE_PORT/testing"
    query = query = [
        "select * from INVOICE_HEADER where id > 0",
        "select * from INVOICE_ITEM where id > 0",
        42,
    ]
    with pytest.raises(InputConfigurationError) as e:
        OracleSource(uri, query)
    assert e.value.error_code == ErrorCode.ICE38


def test_wrong_credentials_type_raises_error():
    uri = "oracle://DATABASE_IP:DATABASE_PORT/testing"
    query = "select * from INVOICE_HEADER where id > 0"
    credentials = S3AccessKeyCredentials("access_key_id", "secret_access_key")
    with pytest.raises(InputConfigurationError) as e:
        OracleSource(uri, query, credentials=credentials)
    assert e.value.error_code == ErrorCode.ICE39


def test_empty_uri():
    uri = ""
    query = "select * from INVOICE_HEADER where id > 0"
    with pytest.raises(InputConfigurationError) as e:
        OracleSource(uri, query)
    assert e.value.error_code == ErrorCode.ICE2


def test_all_correct_initial_values():
    uri = "oracle://DATABASE_IP:DATABASE_PORT/testing"
    query = [
        "select * from INVOICE_HEADER where id > :number",
        "select * from INVOICE_ITEM where id > :number",
    ]
    credentials = UserPasswordCredentials("admin", "admin")
    initial_values = {
        "number": "0",
    }
    input = OracleSource(
        uri, query, credentials=credentials, initial_values=initial_values
    )
    assert input.uri == uri
    assert input.query == query
    assert input.credentials == credentials
    assert input.initial_values == initial_values
    assert isinstance(input, OracleSource)
    assert isinstance(input, Input)
    expected_dict = {
        OracleSource.IDENTIFIER: {
            URI_KEY: uri,
            QUERY_KEY: query,
            OracleSource.CREDENTIALS_KEY: credentials.to_dict(),
            OracleSource.INITIAL_VALUES_KEY: initial_values,
        }
    }
    assert input.to_dict() == expected_dict
    assert input.__repr__()
    assert isinstance(build_input(input.to_dict()), OracleSource)


def test_wrong_type_initial_values_raises_type_error():
    uri = "oracle://DATABASE_IP:DATABASE_PORT/testing"
    query = "select * from INVOICE_HEADER where id > :number"
    initial_values = 42
    with pytest.raises(InputConfigurationError) as e:
        OracleSource(uri, query, initial_values=initial_values)
    assert e.value.error_code == ErrorCode.ICE37


def test_none_initial_values_converted_empty_dict():
    uri = "oracle://DATABASE_IP:DATABASE_PORT/testing"
    query = "select * from INVOICE_HEADER where id > 0"
    initial_values = None
    input = OracleSource(uri, query, initial_values=initial_values)
    assert input.initial_values == {}


def test_update_initial_values():
    uri = "oracle://DATABASE_IP:DATABASE_PORT/testing"
    query = "select * from INVOICE_HEADER where id > :number"
    credentials = UserPasswordCredentials("admin", "admin")
    initial_values = {
        "number": "0",
    }
    input = OracleSource(
        uri, query, credentials=credentials, initial_values=initial_values
    )
    assert input.initial_values == initial_values
    initial_values["number"] = "0"
    assert input.initial_values == initial_values
    input.initial_values = {"number": "2"}
    assert input.initial_values == {"number": "2"}


def test_update_query():
    uri = "oracle://DATABASE_IP:DATABASE_PORT/testing"
    query = "select * from INVOICE_HEADER where id > :number"
    credentials = UserPasswordCredentials("admin", "admin")
    input = OracleSource(uri, query, credentials=credentials)
    assert input.query == query
    query = "select * from ITEMS_HEADER where id > :number"
    input.query = query
    assert input.query == query


def test_update_credentials():
    uri = "oracle://DATABASE_IP:DATABASE_PORT/testing"
    query = "select * from INVOICE_HEADER where id > 0"
    credentials = UserPasswordCredentials("admin", "admin")
    input = OracleSource(uri, query, credentials=credentials)
    assert input.credentials == credentials
    credentials = UserPasswordCredentials("admin2", "admin2")
    input.credentials = credentials
    assert input.credentials == credentials
    input.credentials = {}
    assert input.credentials is None


def test_update_uri():
    uri = "oracle://DATABASE_IP:DATABASE_PORT/testing"
    query = "select * from INVOICE_HEADER where id > 0"
    input = OracleSource(uri, query)
    assert input.uri == uri
    uri = "oracle://DATABASE_IP:3308/testing2"
    input.uri = uri
    assert input.uri == uri


def test_driver_uri_fails():
    uri = "oracle+driver://DATABASE_IP:DATABASE_PORT/testing"
    query = "select * from INVOICE_HEADER where id > :number"
    with pytest.raises(InputConfigurationError) as e:
        OracleSource(uri, query)
    assert e.value.error_code == ErrorCode.ICE2
