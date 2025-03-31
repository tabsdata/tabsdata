#
# Copyright 2024 Tabs Data Inc.
#

import pytest

from tabsdata.credentials import S3AccessKeyCredentials, UserPasswordCredentials
from tabsdata.exceptions import ErrorCode, InputConfigurationError
from tabsdata.io.input import Input, MySQLSource, build_input

QUERY_KEY = MySQLSource.QUERY_KEY
URI_KEY = MySQLSource.URI_KEY


def test_all_correct_query_list():
    uri = "mysql://DATABASE_IP:DATABASE_PORT/testing"
    query = [
        "select * from INVOICE_HEADER where id > 0",
        "select * from INVOICE_ITEM where id > 0",
    ]
    credentials = UserPasswordCredentials("admin", "admin")
    input = MySQLSource(uri, query, credentials=credentials)
    assert input.uri == uri
    assert input.query == query
    assert input.credentials == credentials
    assert input.initial_values == {}
    assert isinstance(input, MySQLSource)
    assert isinstance(input, Input)
    expected_dict = {
        MySQLSource.IDENTIFIER: {
            URI_KEY: uri,
            QUERY_KEY: query,
            MySQLSource.CREDENTIALS_KEY: credentials.to_dict(),
            MySQLSource.INITIAL_VALUES_KEY: {},
        }
    }
    assert input.to_dict() == expected_dict
    assert input.__repr__()
    assert isinstance(build_input(input.to_dict()), MySQLSource)


def test_identifier_string_unchanged():
    uri = "mysql://DATABASE_IP:DATABASE_PORT/testing"
    query = [
        "select * from INVOICE_HEADER where id > 0",
        "select * from INVOICE_ITEM where id > 0",
    ]
    input = MySQLSource(uri, query)
    expected_dict = {
        "mysql-input": {
            URI_KEY: uri,
            QUERY_KEY: query,
            MySQLSource.CREDENTIALS_KEY: None,
            MySQLSource.INITIAL_VALUES_KEY: {},
        }
    }
    assert input.to_dict() == expected_dict
    assert input.__repr__()
    assert isinstance(build_input(input.to_dict()), MySQLSource)


def test_all_correct_query_string():
    uri = "mysql://DATABASE_IP:DATABASE_PORT/testing"
    query = "select * from INVOICE_HEADER where id > 0"
    credentials = UserPasswordCredentials("admin", "admin")
    input = MySQLSource(uri, query, credentials=credentials)
    assert input.uri == uri
    assert input.query == query
    assert input.credentials == credentials
    assert input.initial_values == {}
    assert isinstance(input, MySQLSource)
    assert isinstance(input, Input)
    expected_dict = {
        MySQLSource.IDENTIFIER: {
            URI_KEY: uri,
            QUERY_KEY: query,
            MySQLSource.CREDENTIALS_KEY: credentials.to_dict(),
            MySQLSource.INITIAL_VALUES_KEY: {},
        }
    }
    assert input.to_dict() == expected_dict
    assert input.__repr__()
    assert isinstance(build_input(input.to_dict()), MySQLSource)


def test_same_input_eq():
    uri = "mysql://DATABASE_IP:DATABASE_PORT/testing"
    query = "select * from INVOICE_HEADER where id > 0"
    credentials = UserPasswordCredentials("admin", "admin")
    input = MySQLSource(uri, query, credentials=credentials)
    input2 = MySQLSource(uri, query, credentials=credentials)
    assert input == input2


def test_different_input_not_eq():
    uri = "mysql://DATABASE_IP:DATABASE_PORT/testing"
    query = "select * from INVOICE_HEADER where id > 0"
    credentials = UserPasswordCredentials("admin", "admin")
    input = MySQLSource(uri, query, credentials=credentials)
    uri2 = "mysql://DATABASE_IP:3308/testing"
    input2 = MySQLSource(uri2, query, credentials=credentials)
    assert input != input2


def test_input_not_eq_dict():
    uri = "mysql://DATABASE_IP:DATABASE_PORT/testing"
    query = "select * from INVOICE_HEADER where id > 0"
    credentials = UserPasswordCredentials("admin", "admin")
    input = MySQLSource(uri, query, credentials=credentials)
    assert input.to_dict() != input


def test_all_correct_query_string_no_credentials():
    uri = "mysql://DATABASE_IP:DATABASE_PORT/testing"
    query = "select * from INVOICE_HEADER where id > 0"
    input = MySQLSource(uri, query)
    assert input.uri == uri
    assert input.query == query
    assert input.credentials is None
    assert isinstance(input, MySQLSource)
    assert isinstance(input, Input)
    expected_dict = {
        MySQLSource.IDENTIFIER: {
            URI_KEY: uri,
            QUERY_KEY: query,
            MySQLSource.CREDENTIALS_KEY: None,
            MySQLSource.INITIAL_VALUES_KEY: {},
        }
    }
    assert input.to_dict() == expected_dict
    assert input.__repr__()
    assert isinstance(build_input(input.to_dict()), MySQLSource)


def test_wrong_scheme_raises_value_error():
    uri = "wrongscheme://path/to/query"
    query = "select * from INVOICE_HEADER where id > 0"
    with pytest.raises(InputConfigurationError) as e:
        MySQLSource(uri, query)
    assert e.value.error_code == ErrorCode.ICE2


def test_query_list():
    uri = "mysql://DATABASE_IP:DATABASE_PORT/testing"
    query = [
        "select * from INVOICE_HEADER where id > 0",
        "select * from INVOICE_ITEM where id > 0",
    ]
    input = MySQLSource(uri, query)
    assert input.query == query


def test_query_wrong_type_raises_type_error():
    uri = "mysql://DATABASE_IP:DATABASE_PORT/testing"
    query = 42
    with pytest.raises(InputConfigurationError) as e:
        MySQLSource(uri, query)
    assert e.value.error_code == ErrorCode.ICE19


def test_query_list_wrong_type_raises_type_error():
    uri = "mysql://DATABASE_IP:DATABASE_PORT/testing"
    query = query = [
        "select * from INVOICE_HEADER where id > 0",
        "select * from INVOICE_ITEM where id > 0",
        42,
    ]
    with pytest.raises(InputConfigurationError) as e:
        MySQLSource(uri, query)
    assert e.value.error_code == ErrorCode.ICE19


def test_wrong_credentials_type_raises_error():
    uri = "mysql://DATABASE_IP:DATABASE_PORT/testing"
    query = "select * from INVOICE_HEADER where id > 0"
    credentials = S3AccessKeyCredentials("access_key_id", "secret_access_key")
    with pytest.raises(InputConfigurationError) as e:
        MySQLSource(uri, query, credentials=credentials)
    assert e.value.error_code == ErrorCode.ICE22


def test_empty_uri():
    uri = ""
    query = "select * from INVOICE_HEADER where id > 0"
    with pytest.raises(InputConfigurationError) as e:
        MySQLSource(uri, query)
    assert e.value.error_code == ErrorCode.ICE2


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
    input = MySQLSource(
        uri, query, credentials=credentials, initial_values=initial_values
    )
    assert input.uri == uri
    assert input.query == query
    assert input.credentials == credentials
    assert input.initial_values == initial_values
    assert isinstance(input, MySQLSource)
    assert isinstance(input, Input)
    expected_dict = {
        MySQLSource.IDENTIFIER: {
            URI_KEY: uri,
            QUERY_KEY: query,
            MySQLSource.CREDENTIALS_KEY: credentials.to_dict(),
            MySQLSource.INITIAL_VALUES_KEY: initial_values,
        }
    }
    assert input.to_dict() == expected_dict
    assert input.__repr__()
    assert isinstance(build_input(input.to_dict()), MySQLSource)


def test_wrong_type_initial_values_raises_type_error():
    uri = "mysql://DATABASE_IP:DATABASE_PORT/testing"
    query = "select * from INVOICE_HEADER where id > :number"
    initial_values = 42
    with pytest.raises(InputConfigurationError) as e:
        MySQLSource(uri, query, initial_values=initial_values)
    assert e.value.error_code == ErrorCode.ICE12


def test_none_initial_values_converted_empty_dict():
    uri = "mysql://DATABASE_IP:DATABASE_PORT/testing"
    query = "select * from INVOICE_HEADER where id > 0"
    initial_values = None
    input = MySQLSource(uri, query, initial_values=initial_values)
    assert input.initial_values == {}


def test_update_initial_values():
    uri = "mysql://DATABASE_IP:DATABASE_PORT/testing"
    query = "select * from INVOICE_HEADER where id > :number"
    credentials = UserPasswordCredentials("admin", "admin")
    initial_values = {
        "number": "0",
    }
    input = MySQLSource(
        uri, query, credentials=credentials, initial_values=initial_values
    )
    assert input.initial_values == initial_values
    initial_values["number"] = "0"
    assert input.initial_values == initial_values
    input.initial_values = {"number": "2"}
    assert input.initial_values == {"number": "2"}


def test_update_query():
    uri = "mysql://DATABASE_IP:DATABASE_PORT/testing"
    query = "select * from INVOICE_HEADER where id > :number"
    credentials = UserPasswordCredentials("admin", "admin")
    input = MySQLSource(uri, query, credentials=credentials)
    assert input.query == query
    query = "select * from ITEMS_HEADER where id > :number"
    input.query = query
    assert input.query == query


def test_update_credentials():
    uri = "mysql://DATABASE_IP:DATABASE_PORT/testing"
    query = "select * from INVOICE_HEADER where id > 0"
    credentials = UserPasswordCredentials("admin", "admin")
    input = MySQLSource(uri, query, credentials=credentials)
    assert input.credentials == credentials
    credentials = UserPasswordCredentials("admin2", "admin2")
    input.credentials = credentials
    assert input.credentials == credentials
    input.credentials = {}
    assert input.credentials is None


def test_update_uri():
    uri = "mysql://DATABASE_IP:DATABASE_PORT/testing"
    query = "select * from INVOICE_HEADER where id > 0"
    input = MySQLSource(uri, query)
    assert input.uri == uri
    uri = "mysql://DATABASE_IP:3308/testing2"
    input.uri = uri
    assert input.uri == uri


def test_driver_uri_fails():
    uri = "mysql+driver://DATABASE_IP:DATABASE_PORT/testing"
    query = "select * from INVOICE_HEADER where id > :number"
    with pytest.raises(InputConfigurationError) as e:
        MySQLSource(uri, query)
    assert e.value.error_code == ErrorCode.ICE2
