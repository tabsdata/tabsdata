#
# Copyright 2024 Tabs Data Inc.
#

import pytest

from tabsdata.credentials import S3AccessKeyCredentials, UserPasswordCredentials
from tabsdata.exceptions import ErrorCode, OutputConfigurationError
from tabsdata.tabsdatafunction import OracleDestination, Output, build_output


def test_all_correct_destination_table_list():
    uri = "oracle://DATABASE_IP:DATABASE_PORT/testing"
    destination_table = ["headers_table", "invoices_table"]
    credentials = UserPasswordCredentials("admin", "admin")
    output = OracleDestination(uri, destination_table, credentials=credentials)
    assert output.uri == uri
    assert output.destination_table == destination_table
    assert output.credentials == credentials
    assert output.host == "DATABASE_IP"
    assert output.port == "DATABASE_PORT"
    assert output.database == "testing"
    assert isinstance(output, OracleDestination)
    assert isinstance(output, Output)
    expected_dict = {
        OracleDestination.IDENTIFIER: {
            OracleDestination.URI_KEY: uri,
            OracleDestination.DESTINATION_TABLE_KEY: destination_table,
            OracleDestination.CREDENTIALS_KEY: credentials.to_dict(),
            OracleDestination.IF_TABLE_EXISTS_KEY: "append",
        }
    }
    assert output.to_dict() == expected_dict
    assert output.__repr__()
    assert isinstance(build_output(output.to_dict()), OracleDestination)


def test_identifier_string_unchanged():
    uri = "oracle://DATABASE_IP:DATABASE_PORT/testing"
    destination_table = "headers_table"
    output = OracleDestination(uri, destination_table)
    expected_dict = {
        "oracle-output": {
            OracleDestination.URI_KEY: uri,
            OracleDestination.DESTINATION_TABLE_KEY: destination_table,
            OracleDestination.CREDENTIALS_KEY: None,
            OracleDestination.IF_TABLE_EXISTS_KEY: "append",
        }
    }
    assert output.to_dict() == expected_dict
    assert output.__repr__()
    assert isinstance(build_output(output.to_dict()), OracleDestination)


def test_all_correct_destination_table_string():
    uri = "oracle://DATABASE_IP:DATABASE_PORT/testing"
    destination_table = "output_table"
    credentials = UserPasswordCredentials("admin", "admin")
    output = OracleDestination(uri, destination_table, credentials=credentials)
    assert output.uri == uri
    assert output.destination_table == destination_table
    assert output.credentials == credentials
    assert output.host == "DATABASE_IP"
    assert output.port == "DATABASE_PORT"
    assert output.database == "testing"
    assert isinstance(output, OracleDestination)
    assert isinstance(output, Output)
    expected_dict = {
        OracleDestination.IDENTIFIER: {
            OracleDestination.URI_KEY: uri,
            OracleDestination.DESTINATION_TABLE_KEY: destination_table,
            OracleDestination.CREDENTIALS_KEY: credentials.to_dict(),
            OracleDestination.IF_TABLE_EXISTS_KEY: "append",
        }
    }
    assert output.to_dict() == expected_dict
    assert output.__repr__()
    assert isinstance(build_output(output.to_dict()), OracleDestination)


def test_same_input_eq():
    uri = "oracle://DATABASE_IP:DATABASE_PORT/testing"
    destination_table = "output_table"
    credentials = UserPasswordCredentials("admin", "admin")
    output = OracleDestination(uri, destination_table, credentials=credentials)
    output2 = OracleDestination(uri, destination_table, credentials=credentials)
    assert output == output2


def test_different_input_not_eq():
    uri = "oracle://DATABASE_IP:DATABASE_PORT/testing"
    destination_table = "output_table"
    credentials = UserPasswordCredentials("admin", "admin")
    output = OracleDestination(uri, destination_table, credentials=credentials)
    uri2 = "oracle://DATABASE_IP:3308/testing"
    output2 = OracleDestination(uri2, destination_table, credentials=credentials)
    assert output != output2


def test_input_not_eq_dict():
    uri = "oracle://DATABASE_IP:DATABASE_PORT/testing"
    destination_table = "output_table"
    credentials = UserPasswordCredentials("admin", "admin")
    output = OracleDestination(uri, destination_table, credentials=credentials)
    assert output.to_dict() != output


def test_all_correct_destination_table_string_no_credentials():
    uri = "oracle://DATABASE_IP:DATABASE_PORT/testing"
    destination_table = "output_table"
    output = OracleDestination(uri, destination_table)
    assert output.uri == uri
    assert output.destination_table == destination_table
    assert output.credentials is None
    assert output.host == "DATABASE_IP"
    assert output.port == "DATABASE_PORT"
    assert output.database == "testing"
    assert isinstance(output, OracleDestination)
    assert isinstance(output, Output)
    expected_dict = {
        OracleDestination.IDENTIFIER: {
            OracleDestination.URI_KEY: uri,
            OracleDestination.DESTINATION_TABLE_KEY: destination_table,
            OracleDestination.CREDENTIALS_KEY: None,
            OracleDestination.IF_TABLE_EXISTS_KEY: "append",
        }
    }
    assert output.to_dict() == expected_dict
    assert output.__repr__()
    assert isinstance(build_output(output.to_dict()), OracleDestination)


def test_wrong_scheme_raises_value_error():
    uri = "wrongscheme://path/to/destination_table"
    destination_table = "output_table"
    with pytest.raises(OutputConfigurationError) as e:
        OracleDestination(uri, destination_table)
    assert e.value.error_code == ErrorCode.OCE2


def test_destination_table_list():
    uri = "oracle://DATABASE_IP:DATABASE_PORT/testing"
    destination_table = ["headers_table", "items_table"]
    output = OracleDestination(uri, destination_table)
    assert output.destination_table == destination_table


def test_destination_table_wrong_type_raises_type_error():
    uri = "oracle://DATABASE_IP:DATABASE_PORT/testing"
    destination_table = 42
    with pytest.raises(OutputConfigurationError) as e:
        OracleDestination(uri, destination_table)
    assert e.value.error_code == ErrorCode.OCE24


def test_wrong_credentials_type_raises_type_error():
    uri = "oracle://DATABASE_IP:DATABASE_PORT/testing"
    destination_table = "output_table"
    credentials = S3AccessKeyCredentials("admin", "admin")
    with pytest.raises(OutputConfigurationError) as e:
        OracleDestination(uri, destination_table, credentials=credentials)
    assert e.value.error_code == ErrorCode.OCE25


def test_empty_uri():
    uri = ""
    destination_table = "output_table"
    with pytest.raises(OutputConfigurationError) as e:
        OracleDestination(uri, destination_table)
    assert e.value.error_code == ErrorCode.OCE2


def test_update_destination_table():
    uri = "oracle://DATABASE_IP:DATABASE_PORT/testing"
    destination_table = "output_table"
    output = OracleDestination(uri, destination_table)
    assert output.destination_table == destination_table
    output.destination_table = "new_output_table"
    assert output.destination_table == "new_output_table"
    output.destination_table = ["new_output_table", "new_output_table2"]
    assert output.destination_table == ["new_output_table", "new_output_table2"]


def test_update_configs():
    uri = "oracle://DATABASE_IP:DATABASE_PORT/testing"
    destination_table = "output_table"
    output = OracleDestination(uri, destination_table)
    assert output.credentials is None
    output.credentials = UserPasswordCredentials("admin", "admin")
    assert output.credentials == UserPasswordCredentials("admin", "admin")


def test_update_uri():
    uri = "oracle://DATABASE_IP:DATABASE_PORT/testing"
    destination_table = "output_table"
    output = OracleDestination(uri, destination_table)
    assert output.uri == uri
    output.uri = "oracle://DATABASE_IP:DATABASE_PORT/testing2"
    assert output.uri == "oracle://DATABASE_IP:DATABASE_PORT/testing2"
    assert output.host == "DATABASE_IP"
    assert output.port == "DATABASE_PORT"
    assert output.database == "testing2"


def test_update_if_table_exists():
    uri = "oracle://DATABASE_IP:DATABASE_PORT/testing"
    destination_table = "output_table"
    output = OracleDestination(uri, destination_table, if_table_exists="replace")
    assert output.if_table_exists == "replace"
    output.if_table_exists = "append"
    assert output.if_table_exists == "append"


def test_if_table_exists_wrong_parameter_raises_exception():
    uri = "oracle://DATABASE_IP:DATABASE_PORT/testing"
    destination_table = "output_table"
    with pytest.raises(OutputConfigurationError) as e:
        OracleDestination(uri, destination_table, if_table_exists="wrong")
    assert e.value.error_code == ErrorCode.OCE28
    with pytest.raises(OutputConfigurationError) as e:
        OracleDestination(uri, destination_table, if_table_exists=42)
    assert e.value.error_code == ErrorCode.OCE28


def test_all_correct_driver():
    uri = "oracle+driver://DATABASE_IP:DATABASE_PORT/testing"
    destination_table = "output_table"
    credentials = UserPasswordCredentials("admin", "admin")
    output = OracleDestination(uri, destination_table, credentials=credentials)
    assert output.uri == uri
    assert output.destination_table == destination_table
    assert output.credentials == credentials
    assert output.host == "DATABASE_IP"
    assert output.port == "DATABASE_PORT"
    assert output.database == "testing"
    assert isinstance(output, OracleDestination)
    assert isinstance(output, Output)
    expected_dict = {
        OracleDestination.IDENTIFIER: {
            OracleDestination.URI_KEY: uri,
            OracleDestination.DESTINATION_TABLE_KEY: destination_table,
            OracleDestination.CREDENTIALS_KEY: credentials.to_dict(),
            OracleDestination.IF_TABLE_EXISTS_KEY: "append",
        }
    }
    assert output.to_dict() == expected_dict
    assert output.__repr__()
    assert isinstance(build_output(output.to_dict()), OracleDestination)
