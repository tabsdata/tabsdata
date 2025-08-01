#
# Copyright 2024 Tabs Data Inc.
#

import pytest

from tabsdata._credentials import S3AccessKeyCredentials, UserPasswordCredentials
from tabsdata._io.outputs.sql_outputs import MariaDBDestination
from tabsdata._io.plugin import DestinationPlugin
from tabsdata.exceptions import ErrorCode, OutputConfigurationError


def test_all_correct_destination_table_list():
    uri = "mariadb://DATABASE_IP:DATABASE_PORT/testing"
    destination_table = ["headers_table", "invoices_table"]
    credentials = UserPasswordCredentials("admin", "admin")
    output = MariaDBDestination(uri, destination_table, credentials=credentials)
    assert output.uri == uri
    assert output.destination_table == destination_table
    assert output.credentials == credentials
    assert isinstance(output, MariaDBDestination)
    assert isinstance(output, DestinationPlugin)
    assert output.__repr__()


def test_all_correct_destination_table_string():
    uri = "mariadb://DATABASE_IP:DATABASE_PORT/testing"
    destination_table = "output_table"
    credentials = UserPasswordCredentials("admin", "admin")
    output = MariaDBDestination(uri, destination_table, credentials=credentials)
    assert output.uri == uri
    assert output.destination_table == destination_table
    assert output.credentials == credentials
    assert isinstance(output, MariaDBDestination)
    assert isinstance(output, DestinationPlugin)
    assert output.__repr__()


def test_all_correct_destination_table_string_no_credentials():
    uri = "mariadb://DATABASE_IP:DATABASE_PORT/testing"
    destination_table = "output_table"
    output = MariaDBDestination(uri, destination_table)
    assert output.uri == uri
    assert output.destination_table == destination_table
    assert output.credentials is None
    assert isinstance(output, MariaDBDestination)
    assert isinstance(output, DestinationPlugin)
    assert output.__repr__()


def test_wrong_scheme_raises_value_error():
    uri = "wrongscheme://path/to/destination_table"
    destination_table = "output_table"
    with pytest.raises(OutputConfigurationError) as e:
        MariaDBDestination(uri, destination_table)
    assert e.value.error_code == ErrorCode.OCE2


def test_destination_table_list():
    uri = "mariadb://DATABASE_IP:DATABASE_PORT/testing"
    destination_table = ["headers_table", "items_table"]
    output = MariaDBDestination(uri, destination_table)
    assert output.destination_table == destination_table


def test_destination_table_wrong_type_raises_type_error():
    uri = "mariadb://DATABASE_IP:DATABASE_PORT/testing"
    destination_table = 42
    with pytest.raises(OutputConfigurationError) as e:
        MariaDBDestination(uri, destination_table)
    assert e.value.error_code == ErrorCode.OCE22


def test_wrong_credentials_type_raises_type_error():
    uri = "mariadb://DATABASE_IP:DATABASE_PORT/testing"
    destination_table = "output_table"
    credentials = S3AccessKeyCredentials("admin", "admin")
    with pytest.raises(OutputConfigurationError) as e:
        MariaDBDestination(uri, destination_table, credentials=credentials)
    assert e.value.error_code == ErrorCode.OCE23


def test_empty_uri():
    uri = ""
    destination_table = "output_table"
    with pytest.raises(OutputConfigurationError) as e:
        MariaDBDestination(uri, destination_table)
    assert e.value.error_code == ErrorCode.OCE2


def test_update_destination_table():
    uri = "mariadb://DATABASE_IP:DATABASE_PORT/testing"
    destination_table = "output_table"
    output = MariaDBDestination(uri, destination_table)
    assert output.destination_table == destination_table
    output.destination_table = "new_output_table"
    assert output.destination_table == "new_output_table"
    output.destination_table = ["new_output_table", "new_output_table2"]
    assert output.destination_table == ["new_output_table", "new_output_table2"]


def test_update_configs():
    uri = "mariadb://DATABASE_IP:DATABASE_PORT/testing"
    destination_table = "output_table"
    output = MariaDBDestination(uri, destination_table)
    assert output.credentials is None
    output.credentials = UserPasswordCredentials("admin", "admin")
    assert output.credentials == UserPasswordCredentials("admin", "admin")


def test_update_uri():
    uri = "mariadb://DATABASE_IP:DATABASE_PORT/testing"
    destination_table = "output_table"
    output = MariaDBDestination(uri, destination_table)
    assert output.uri == uri
    output.uri = "mariadb://DATABASE_IP:DATABASE_PORT/testing2"
    assert output.uri == "mariadb://DATABASE_IP:DATABASE_PORT/testing2"


def test_update_if_table_exists():
    uri = "mariadb://DATABASE_IP:DATABASE_PORT/testing"
    destination_table = "output_table"
    output = MariaDBDestination(uri, destination_table, if_table_exists="replace")
    assert output.if_table_exists == "replace"
    output.if_table_exists = "append"
    assert output.if_table_exists == "append"


def test_if_table_exists_wrong_parameter_raises_exception():
    uri = "mariadb://DATABASE_IP:DATABASE_PORT/testing"
    destination_table = "output_table"
    with pytest.raises(OutputConfigurationError) as e:
        MariaDBDestination(uri, destination_table, if_table_exists="wrong")
    assert e.value.error_code == ErrorCode.OCE26
    with pytest.raises(OutputConfigurationError) as e:
        MariaDBDestination(uri, destination_table, if_table_exists=42)
    assert e.value.error_code == ErrorCode.OCE26


def test_all_correct_driver():
    uri = "mariadb+driver://DATABASE_IP:DATABASE_PORT/testing"
    destination_table = "output_table"
    credentials = UserPasswordCredentials("admin", "admin")
    output = MariaDBDestination(uri, destination_table, credentials=credentials)
    assert output.uri == uri
    assert output.destination_table == destination_table
    assert output.credentials == credentials
    assert isinstance(output, MariaDBDestination)
    assert isinstance(output, DestinationPlugin)
    assert output.__repr__()
