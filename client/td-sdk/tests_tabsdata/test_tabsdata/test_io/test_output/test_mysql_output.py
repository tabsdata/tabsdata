#
# Copyright 2024 Tabs Data Inc.
#

import pytest

from tabsdata.credentials import S3AccessKeyCredentials, UserPasswordCredentials
from tabsdata.exceptions import ErrorCode, OutputConfigurationError
from tabsdata.io.output import MySQLDestination, Output, build_output


def test_all_correct_destination_table_list():
    uri = "mysql://DATABASE_IP:DATABASE_PORT/testing"
    destination_table = ["headers_table", "invoices_table"]
    credentials = UserPasswordCredentials("admin", "admin")
    output = MySQLDestination(uri, destination_table, credentials=credentials)
    assert output.uri == uri
    assert output.destination_table == destination_table
    assert output.credentials == credentials
    assert isinstance(output, MySQLDestination)
    assert isinstance(output, Output)
    expected_dict = {
        MySQLDestination.IDENTIFIER: {
            MySQLDestination.URI_KEY: uri,
            MySQLDestination.DESTINATION_TABLE_KEY: destination_table,
            MySQLDestination.CREDENTIALS_KEY: credentials.to_dict(),
            MySQLDestination.IF_TABLE_EXISTS_KEY: "append",
        }
    }
    assert output.to_dict() == expected_dict
    assert output.__repr__()
    assert isinstance(build_output(output.to_dict()), MySQLDestination)


def test_identifier_string_unchanged():
    uri = "mysql://DATABASE_IP:DATABASE_PORT/testing"
    destination_table = "headers_table"
    output = MySQLDestination(uri, destination_table)
    expected_dict = {
        "mysql-output": {
            MySQLDestination.URI_KEY: uri,
            MySQLDestination.DESTINATION_TABLE_KEY: destination_table,
            MySQLDestination.CREDENTIALS_KEY: None,
            MySQLDestination.IF_TABLE_EXISTS_KEY: "append",
        }
    }
    assert output.to_dict() == expected_dict
    assert output.__repr__()
    assert isinstance(build_output(output.to_dict()), MySQLDestination)


def test_all_correct_destination_table_string():
    uri = "mysql://DATABASE_IP:DATABASE_PORT/testing"
    destination_table = "output_table"
    credentials = UserPasswordCredentials("admin", "admin")
    output = MySQLDestination(uri, destination_table, credentials=credentials)
    assert output.uri == uri
    assert output.destination_table == destination_table
    assert output.credentials == credentials
    assert isinstance(output, MySQLDestination)
    assert isinstance(output, Output)
    expected_dict = {
        MySQLDestination.IDENTIFIER: {
            MySQLDestination.URI_KEY: uri,
            MySQLDestination.DESTINATION_TABLE_KEY: destination_table,
            MySQLDestination.CREDENTIALS_KEY: credentials.to_dict(),
            MySQLDestination.IF_TABLE_EXISTS_KEY: "append",
        }
    }
    assert output.to_dict() == expected_dict
    assert output.__repr__()
    assert isinstance(build_output(output.to_dict()), MySQLDestination)


def test_same_input_eq():
    uri = "mysql://DATABASE_IP:DATABASE_PORT/testing"
    destination_table = "output_table"
    credentials = UserPasswordCredentials("admin", "admin")
    output = MySQLDestination(uri, destination_table, credentials=credentials)
    output2 = MySQLDestination(uri, destination_table, credentials=credentials)
    assert output == output2


def test_different_input_not_eq():
    uri = "mysql://DATABASE_IP:DATABASE_PORT/testing"
    destination_table = "output_table"
    credentials = UserPasswordCredentials("admin", "admin")
    output = MySQLDestination(uri, destination_table, credentials=credentials)
    uri2 = "mysql://DATABASE_IP:3308/testing"
    output2 = MySQLDestination(uri2, destination_table, credentials=credentials)
    assert output != output2


def test_input_not_eq_dict():
    uri = "mysql://DATABASE_IP:DATABASE_PORT/testing"
    destination_table = "output_table"
    credentials = UserPasswordCredentials("admin", "admin")
    output = MySQLDestination(uri, destination_table, credentials=credentials)
    assert output.to_dict() != output


def test_all_correct_destination_table_string_no_credentials():
    uri = "mysql://DATABASE_IP:DATABASE_PORT/testing"
    destination_table = "output_table"
    output = MySQLDestination(uri, destination_table)
    assert output.uri == uri
    assert output.destination_table == destination_table
    assert output.credentials is None
    assert isinstance(output, MySQLDestination)
    assert isinstance(output, Output)
    expected_dict = {
        MySQLDestination.IDENTIFIER: {
            MySQLDestination.URI_KEY: uri,
            MySQLDestination.DESTINATION_TABLE_KEY: destination_table,
            MySQLDestination.CREDENTIALS_KEY: None,
            MySQLDestination.IF_TABLE_EXISTS_KEY: "append",
        }
    }
    assert output.to_dict() == expected_dict
    assert output.__repr__()
    assert isinstance(build_output(output.to_dict()), MySQLDestination)


def test_wrong_scheme_raises_value_error():
    uri = "wrongscheme://path/to/destination_table"
    destination_table = "output_table"
    with pytest.raises(OutputConfigurationError) as e:
        MySQLDestination(uri, destination_table)
    assert e.value.error_code == ErrorCode.OCE2


def test_destination_table_list():
    uri = "mysql://DATABASE_IP:DATABASE_PORT/testing"
    destination_table = ["headers_table", "items_table"]
    output = MySQLDestination(uri, destination_table)
    assert output.destination_table == destination_table


def test_destination_table_wrong_type_raises_type_error():
    uri = "mysql://DATABASE_IP:DATABASE_PORT/testing"
    destination_table = 42
    with pytest.raises(OutputConfigurationError) as e:
        MySQLDestination(uri, destination_table)
    assert e.value.error_code == ErrorCode.OCE8


def test_wrong_credentials_type_raises_type_error():
    uri = "mysql://DATABASE_IP:DATABASE_PORT/testing"
    destination_table = "output_table"
    credentials = S3AccessKeyCredentials("admin", "admin")
    with pytest.raises(OutputConfigurationError) as e:
        MySQLDestination(uri, destination_table, credentials=credentials)
    assert e.value.error_code == ErrorCode.OCE9


def test_empty_uri():
    uri = ""
    destination_table = "output_table"
    with pytest.raises(OutputConfigurationError) as e:
        MySQLDestination(uri, destination_table)
    assert e.value.error_code == ErrorCode.OCE2


def test_update_destination_table():
    uri = "mysql://DATABASE_IP:DATABASE_PORT/testing"
    destination_table = "output_table"
    output = MySQLDestination(uri, destination_table)
    assert output.destination_table == destination_table
    output.destination_table = "new_output_table"
    assert output.destination_table == "new_output_table"
    output.destination_table = ["new_output_table", "new_output_table2"]
    assert output.destination_table == ["new_output_table", "new_output_table2"]


def test_update_configs():
    uri = "mysql://DATABASE_IP:DATABASE_PORT/testing"
    destination_table = "output_table"
    output = MySQLDestination(uri, destination_table)
    assert output.credentials is None
    output.credentials = UserPasswordCredentials("admin", "admin")
    assert output.credentials == UserPasswordCredentials("admin", "admin")


def test_update_uri():
    uri = "mysql://DATABASE_IP:DATABASE_PORT/testing"
    destination_table = "output_table"
    output = MySQLDestination(uri, destination_table)
    assert output.uri == uri
    output.uri = "mysql://DATABASE_IP:DATABASE_PORT/testing2"
    assert output.uri == "mysql://DATABASE_IP:DATABASE_PORT/testing2"


def test_update_if_table_exists():
    uri = "mysql://DATABASE_IP:DATABASE_PORT/testing"
    destination_table = "output_table"
    output = MySQLDestination(uri, destination_table, if_table_exists="replace")
    assert output.if_table_exists == "replace"
    output.if_table_exists = "append"
    assert output.if_table_exists == "append"


def test_if_table_exists_wrong_parameter_raises_exception():
    uri = "mysql://DATABASE_IP:DATABASE_PORT/testing"
    destination_table = "output_table"
    with pytest.raises(OutputConfigurationError) as e:
        MySQLDestination(uri, destination_table, if_table_exists="wrong")
    assert e.value.error_code == ErrorCode.OCE27
    with pytest.raises(OutputConfigurationError) as e:
        MySQLDestination(uri, destination_table, if_table_exists=42)
    assert e.value.error_code == ErrorCode.OCE27


def test_all_correct_driver():
    uri = "mysql+driver://DATABASE_IP:DATABASE_PORT/testing"
    destination_table = "output_table"
    credentials = UserPasswordCredentials("admin", "admin")
    output = MySQLDestination(uri, destination_table, credentials=credentials)
    assert output.uri == uri
    assert output.destination_table == destination_table
    assert output.credentials == credentials
    assert isinstance(output, MySQLDestination)
    assert isinstance(output, Output)
    expected_dict = {
        MySQLDestination.IDENTIFIER: {
            MySQLDestination.URI_KEY: uri,
            MySQLDestination.DESTINATION_TABLE_KEY: destination_table,
            MySQLDestination.CREDENTIALS_KEY: credentials.to_dict(),
            MySQLDestination.IF_TABLE_EXISTS_KEY: "append",
        }
    }
    assert output.to_dict() == expected_dict
    assert output.__repr__()
    assert isinstance(build_output(output.to_dict()), MySQLDestination)
