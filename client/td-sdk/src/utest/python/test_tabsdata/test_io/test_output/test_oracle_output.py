#
# Copyright 2024 Tabs Data Inc.
#

import pytest

from tabsdata._credentials import S3AccessKeyCredentials, UserPasswordCredentials
from tabsdata._io.outputs.sql_outputs import OracleDestination
from tabsdata._io.plugin import DestinationPlugin
from tabsdata.exceptions import DestinationConfigurationError, ErrorCode

# noinspection PyUnresolvedReferences
from . import pytestmark  # noqa: F401


def test_all_correct_destination_table_list():
    uri = "oracle://DATABASE_IP:DATABASE_PORT/testing"
    destination_table = ["headers_table", "invoices_table"]
    credentials = UserPasswordCredentials("admin", "admin")
    output = OracleDestination(uri, destination_table, credentials=credentials)
    assert output.uri == uri
    assert output.destination_table == destination_table
    assert output.credentials == credentials
    assert isinstance(output, OracleDestination)
    assert isinstance(output, DestinationPlugin)
    assert output.__repr__()


def test_all_correct_destination_table_string():
    uri = "oracle://DATABASE_IP:DATABASE_PORT/testing"
    destination_table = "output_table"
    credentials = UserPasswordCredentials("admin", "admin")
    output = OracleDestination(uri, destination_table, credentials=credentials)
    assert output.uri == uri
    assert output.destination_table == destination_table
    assert output.credentials == credentials
    assert isinstance(output, OracleDestination)
    assert isinstance(output, DestinationPlugin)
    assert output.__repr__()


def test_uri_upper():
    uri = "oracle://DATABASE_IP:DATABASE_PORT/testing".upper()
    destination_table = "output_table"
    credentials = UserPasswordCredentials("admin", "admin")
    output = OracleDestination(uri, destination_table, credentials=credentials)
    assert output.uri == uri


def test_all_correct_destination_table_string_no_credentials():
    uri = "oracle://DATABASE_IP:DATABASE_PORT/testing"
    destination_table = "output_table"
    output = OracleDestination(uri, destination_table)
    assert output.uri == uri
    assert output.destination_table == destination_table
    assert output.credentials is None
    assert isinstance(output, OracleDestination)
    assert isinstance(output, DestinationPlugin)
    assert output.__repr__()


def test_wrong_scheme_raises_value_error():
    uri = "wrongscheme://path/to/destination_table"
    destination_table = "output_table"
    with pytest.raises(DestinationConfigurationError) as e:
        OracleDestination(uri, destination_table)
    assert e.value.error_code == ErrorCode.DECE2


def test_destination_table_list():
    uri = "oracle://DATABASE_IP:DATABASE_PORT/testing"
    destination_table = ["headers_table", "items_table"]
    output = OracleDestination(uri, destination_table)
    assert output.destination_table == destination_table


def test_destination_table_wrong_type_raises_type_error():
    uri = "oracle://DATABASE_IP:DATABASE_PORT/testing"
    destination_table = 42
    with pytest.raises(DestinationConfigurationError) as e:
        OracleDestination(uri, destination_table)
    assert e.value.error_code == ErrorCode.DECE24


def test_wrong_credentials_type_raises_type_error():
    uri = "oracle://DATABASE_IP:DATABASE_PORT/testing"
    destination_table = "output_table"
    credentials = S3AccessKeyCredentials("admin", "admin")
    with pytest.raises(DestinationConfigurationError) as e:
        OracleDestination(uri, destination_table, credentials=credentials)
    assert e.value.error_code == ErrorCode.DECE25


def test_empty_uri():
    uri = ""
    destination_table = "output_table"
    with pytest.raises(DestinationConfigurationError) as e:
        OracleDestination(uri, destination_table)
    assert e.value.error_code == ErrorCode.DECE2


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
    with pytest.raises(DestinationConfigurationError) as e:
        OracleDestination(uri, destination_table, if_table_exists="wrong")
    assert e.value.error_code == ErrorCode.DECE28
    with pytest.raises(DestinationConfigurationError) as e:
        OracleDestination(uri, destination_table, if_table_exists=42)
    assert e.value.error_code == ErrorCode.DECE28


def test_all_correct_driver():
    uri = "oracle+driver://DATABASE_IP:DATABASE_PORT/testing"
    destination_table = "output_table"
    credentials = UserPasswordCredentials("admin", "admin")
    output = OracleDestination(uri, destination_table, credentials=credentials)
    assert output.uri == uri
    assert output.destination_table == destination_table
    assert output.credentials == credentials
    assert isinstance(output, OracleDestination)
    assert isinstance(output, DestinationPlugin)
    assert output.__repr__()
