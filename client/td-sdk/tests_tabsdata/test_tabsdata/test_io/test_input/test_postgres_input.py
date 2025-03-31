#
# Copyright 2024 Tabs Data Inc.
#

import pytest

from tabsdata.credentials import S3AccessKeyCredentials, UserPasswordCredentials
from tabsdata.exceptions import ErrorCode, InputConfigurationError
from tabsdata.io.input import Input, PostgresSource, build_input

QUERY_KEY = PostgresSource.QUERY_KEY
URI_KEY = PostgresSource.URI_KEY


def test_all_correct_query_list():
    uri = "postgres://DATABASE_IP:DATABASE_PORT/testing"
    query = [
        "select * from INVOICE_HEADER where id > 0",
        "select * from INVOICE_ITEM where id > 0",
    ]
    credentials = UserPasswordCredentials("admin", "admin")
    input = PostgresSource(uri, query, credentials=credentials)
    assert input.uri == uri
    assert input.query == query
    assert input.credentials == credentials
    assert input.initial_values == {}
    assert isinstance(input, PostgresSource)
    assert isinstance(input, Input)
    expected_dict = {
        PostgresSource.IDENTIFIER: {
            URI_KEY: uri,
            QUERY_KEY: query,
            PostgresSource.CREDENTIALS_KEY: credentials.to_dict(),
            PostgresSource.INITIAL_VALUES_KEY: {},
        }
    }
    assert input.to_dict() == expected_dict
    assert input.__repr__()
    assert isinstance(build_input(input.to_dict()), PostgresSource)


def test_all_correct_query_list_postgresql():
    uri = "postgresql://DATABASE_IP:DATABASE_PORT/testing"
    query = [
        "select * from INVOICE_HEADER where id > 0",
        "select * from INVOICE_ITEM where id > 0",
    ]
    credentials = UserPasswordCredentials("admin", "admin")
    input = PostgresSource(uri, query, credentials=credentials)
    assert input.uri == uri
    assert input.query == query
    assert input.credentials == credentials
    assert input.initial_values == {}
    assert isinstance(input, PostgresSource)
    assert isinstance(input, Input)
    expected_dict = {
        PostgresSource.IDENTIFIER: {
            URI_KEY: uri,
            QUERY_KEY: query,
            PostgresSource.CREDENTIALS_KEY: credentials.to_dict(),
            PostgresSource.INITIAL_VALUES_KEY: {},
        }
    }
    assert input.to_dict() == expected_dict
    assert input.__repr__()
    assert isinstance(build_input(input.to_dict()), PostgresSource)


def test_identifier_string_unchanged():
    uri = "postgres://DATABASE_IP:DATABASE_PORT/testing"
    query = [
        "select * from INVOICE_HEADER where id > 0",
        "select * from INVOICE_ITEM where id > 0",
    ]
    input = PostgresSource(uri, query)
    expected_dict = {
        "postgres-input": {
            URI_KEY: uri,
            QUERY_KEY: query,
            PostgresSource.CREDENTIALS_KEY: None,
            PostgresSource.INITIAL_VALUES_KEY: {},
        }
    }
    assert input.to_dict() == expected_dict
    assert input.__repr__()
    assert isinstance(build_input(input.to_dict()), PostgresSource)


def test_all_correct_query_string():
    uri = "postgres://DATABASE_IP:DATABASE_PORT/testing"
    query = "select * from INVOICE_HEADER where id > 0"
    credentials = UserPasswordCredentials("admin", "admin")
    input = PostgresSource(uri, query, credentials=credentials)
    assert input.uri == uri
    assert input.query == query
    assert input.credentials == credentials
    assert input.initial_values == {}
    assert isinstance(input, PostgresSource)
    assert isinstance(input, Input)
    expected_dict = {
        PostgresSource.IDENTIFIER: {
            URI_KEY: uri,
            QUERY_KEY: query,
            PostgresSource.CREDENTIALS_KEY: credentials.to_dict(),
            PostgresSource.INITIAL_VALUES_KEY: {},
        }
    }
    assert input.to_dict() == expected_dict
    assert input.__repr__()
    assert isinstance(build_input(input.to_dict()), PostgresSource)


def test_same_input_eq():
    uri = "postgres://DATABASE_IP:DATABASE_PORT/testing"
    query = "select * from INVOICE_HEADER where id > 0"
    credentials = UserPasswordCredentials("admin", "admin")
    input = PostgresSource(uri, query, credentials=credentials)
    input2 = PostgresSource(uri, query, credentials=credentials)
    assert input == input2


def test_different_input_not_eq():
    uri = "postgres://DATABASE_IP:DATABASE_PORT/testing"
    query = "select * from INVOICE_HEADER where id > 0"
    credentials = UserPasswordCredentials("admin", "admin")
    input = PostgresSource(uri, query, credentials=credentials)
    uri2 = "postgres://DATABASE_IP:3308/testing"
    input2 = PostgresSource(uri2, query, credentials=credentials)
    assert input != input2


def test_input_not_eq_dict():
    uri = "postgres://DATABASE_IP:DATABASE_PORT/testing"
    query = "select * from INVOICE_HEADER where id > 0"
    credentials = UserPasswordCredentials("admin", "admin")
    input = PostgresSource(uri, query, credentials=credentials)
    assert input.to_dict() != input


def test_all_correct_query_string_no_credentials():
    uri = "postgres://DATABASE_IP:DATABASE_PORT/testing"
    query = "select * from INVOICE_HEADER where id > 0"
    input = PostgresSource(uri, query)
    assert input.uri == uri
    assert input.query == query
    assert input.credentials is None
    assert isinstance(input, PostgresSource)
    assert isinstance(input, Input)
    expected_dict = {
        PostgresSource.IDENTIFIER: {
            URI_KEY: uri,
            QUERY_KEY: query,
            PostgresSource.CREDENTIALS_KEY: None,
            PostgresSource.INITIAL_VALUES_KEY: {},
        }
    }
    assert input.to_dict() == expected_dict
    assert input.__repr__()
    assert isinstance(build_input(input.to_dict()), PostgresSource)


def test_wrong_scheme_raises_value_error():
    uri = "wrongscheme://path/to/query"
    query = "select * from INVOICE_HEADER where id > 0"
    with pytest.raises(InputConfigurationError) as e:
        PostgresSource(uri, query)
    assert e.value.error_code == ErrorCode.ICE2


def test_query_list():
    uri = "postgres://DATABASE_IP:DATABASE_PORT/testing"
    query = [
        "select * from INVOICE_HEADER where id > 0",
        "select * from INVOICE_ITEM where id > 0",
    ]
    input = PostgresSource(uri, query)
    assert input.query == query


def test_query_wrong_type_raises_type_error():
    uri = "postgres://DATABASE_IP:DATABASE_PORT/testing"
    query = 42
    with pytest.raises(InputConfigurationError) as e:
        PostgresSource(uri, query)
    assert e.value.error_code == ErrorCode.ICE32


def test_query_list_wrong_type_raises_type_error():
    uri = "postgres://DATABASE_IP:DATABASE_PORT/testing"
    query = query = [
        "select * from INVOICE_HEADER where id > 0",
        "select * from INVOICE_ITEM where id > 0",
        42,
    ]
    with pytest.raises(InputConfigurationError) as e:
        PostgresSource(uri, query)
    assert e.value.error_code == ErrorCode.ICE32


def test_wrong_credentials_type_raises_error():
    uri = "postgres://DATABASE_IP:DATABASE_PORT/testing"
    query = "select * from INVOICE_HEADER where id > 0"
    credentials = S3AccessKeyCredentials("access_key_id", "secret_access_key")
    with pytest.raises(InputConfigurationError) as e:
        PostgresSource(uri, query, credentials=credentials)
    assert e.value.error_code == ErrorCode.ICE33


def test_empty_uri():
    uri = ""
    query = "select * from INVOICE_HEADER where id > 0"
    with pytest.raises(InputConfigurationError) as e:
        PostgresSource(uri, query)
    assert e.value.error_code == ErrorCode.ICE2


def test_all_correct_initial_values():
    uri = "postgres://DATABASE_IP:DATABASE_PORT/testing"
    query = [
        "select * from INVOICE_HEADER where id > :number",
        "select * from INVOICE_ITEM where id > :number",
    ]
    credentials = UserPasswordCredentials("admin", "admin")
    initial_values = {
        "number": "0",
    }
    input = PostgresSource(
        uri, query, credentials=credentials, initial_values=initial_values
    )
    assert input.uri == uri
    assert input.query == query
    assert input.credentials == credentials
    assert input.initial_values == initial_values
    assert isinstance(input, PostgresSource)
    assert isinstance(input, Input)
    expected_dict = {
        PostgresSource.IDENTIFIER: {
            URI_KEY: uri,
            QUERY_KEY: query,
            PostgresSource.CREDENTIALS_KEY: credentials.to_dict(),
            PostgresSource.INITIAL_VALUES_KEY: initial_values,
        }
    }
    assert input.to_dict() == expected_dict
    assert input.__repr__()
    assert isinstance(build_input(input.to_dict()), PostgresSource)


def test_wrong_type_initial_values_raises_type_error():
    uri = "postgres://DATABASE_IP:DATABASE_PORT/testing"
    query = "select * from INVOICE_HEADER where id > :number"
    initial_values = 42
    with pytest.raises(InputConfigurationError) as e:
        PostgresSource(uri, query, initial_values=initial_values)
    assert e.value.error_code == ErrorCode.ICE31


def test_none_initial_values_converted_empty_dict():
    uri = "postgres://DATABASE_IP:DATABASE_PORT/testing"
    query = "select * from INVOICE_HEADER where id > 0"
    initial_values = None
    input = PostgresSource(uri, query, initial_values=initial_values)
    assert input.initial_values == {}


def test_update_initial_values():
    uri = "postgres://DATABASE_IP:DATABASE_PORT/testing"
    query = "select * from INVOICE_HEADER where id > :number"
    credentials = UserPasswordCredentials("admin", "admin")
    initial_values = {
        "number": "0",
    }
    input = PostgresSource(
        uri, query, credentials=credentials, initial_values=initial_values
    )
    assert input.initial_values == initial_values
    initial_values["number"] = "0"
    assert input.initial_values == initial_values
    input.initial_values = {"number": "2"}
    assert input.initial_values == {"number": "2"}


def test_update_query():
    uri = "postgres://DATABASE_IP:DATABASE_PORT/testing"
    query = "select * from INVOICE_HEADER where id > :number"
    credentials = UserPasswordCredentials("admin", "admin")
    input = PostgresSource(uri, query, credentials=credentials)
    assert input.query == query
    query = "select * from ITEMS_HEADER where id > :number"
    input.query = query
    assert input.query == query


def test_update_credentials():
    uri = "postgres://DATABASE_IP:DATABASE_PORT/testing"
    query = "select * from INVOICE_HEADER where id > 0"
    credentials = UserPasswordCredentials("admin", "admin")
    input = PostgresSource(uri, query, credentials=credentials)
    assert input.credentials == credentials
    credentials = UserPasswordCredentials("admin2", "admin2")
    input.credentials = credentials
    assert input.credentials == credentials
    input.credentials = {}
    assert input.credentials is None


def test_update_uri():
    uri = "postgres://DATABASE_IP:DATABASE_PORT/testing"
    query = "select * from INVOICE_HEADER where id > 0"
    input = PostgresSource(uri, query)
    assert input.uri == uri
    uri = "postgres://DATABASE_IP:3308/testing2"
    input.uri = uri
    assert input.uri == uri


def test_driver_uri_fails():
    uri = "postgres+driver://DATABASE_IP:DATABASE_PORT/testing"
    query = "select * from INVOICE_HEADER where id > :number"
    with pytest.raises(InputConfigurationError) as e:
        PostgresSource(uri, query)
    assert e.value.error_code == ErrorCode.ICE2


def test_driver_uri_fails_postgresql():
    uri = "postgresql+driver://DATABASE_IP:DATABASE_PORT/testing"
    query = "select * from INVOICE_HEADER where id > :number"
    with pytest.raises(InputConfigurationError) as e:
        PostgresSource(uri, query)
    assert e.value.error_code == ErrorCode.ICE2
