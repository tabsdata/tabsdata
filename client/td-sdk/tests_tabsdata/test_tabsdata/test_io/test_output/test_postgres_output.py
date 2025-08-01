#
# Copyright 2024 Tabs Data Inc.
#

import pytest

from tabsdata._credentials import S3AccessKeyCredentials, UserPasswordCredentials
from tabsdata._io.outputs.sql_outputs import PostgresDestination
from tabsdata._io.plugin import DestinationPlugin
from tabsdata.exceptions import DestinationConfigurationError, ErrorCode


def test_all_correct_destination_table_list():
    uri = "postgres://DATABASE_IP:DATABASE_PORT/testing"
    destination_table = ["headers_table", "invoices_table"]
    credentials = UserPasswordCredentials("admin", "admin")
    output = PostgresDestination(uri, destination_table, credentials=credentials)
    assert output.uri == uri
    assert output.destination_table == destination_table
    assert output.credentials == credentials
    assert isinstance(output, PostgresDestination)
    assert isinstance(output, DestinationPlugin)
    assert output.__repr__()


def test_all_correct_destination_table_list_postgresql():
    uri = "postgresql://DATABASE_IP:DATABASE_PORT/testing"
    destination_table = ["headers_table", "invoices_table"]
    credentials = UserPasswordCredentials("admin", "admin")
    output = PostgresDestination(uri, destination_table, credentials=credentials)
    assert output.uri == uri
    assert output.destination_table == destination_table
    assert output.credentials == credentials
    assert isinstance(output, PostgresDestination)
    assert isinstance(output, DestinationPlugin)
    assert output.__repr__()


def test_all_correct_destination_table_string():
    uri = "postgres://DATABASE_IP:DATABASE_PORT/testing"
    destination_table = "output_table"
    credentials = UserPasswordCredentials("admin", "admin")
    output = PostgresDestination(uri, destination_table, credentials=credentials)
    assert output.uri == uri
    assert output.destination_table == destination_table
    assert output.credentials == credentials
    assert isinstance(output, PostgresDestination)
    assert isinstance(output, DestinationPlugin)
    assert output.__repr__()


def test_all_correct_destination_table_string_no_credentials():
    uri = "postgres://DATABASE_IP:DATABASE_PORT/testing"
    destination_table = "output_table"
    output = PostgresDestination(uri, destination_table)
    assert output.uri == uri
    assert output.destination_table == destination_table
    assert output.credentials is None
    assert isinstance(output, PostgresDestination)
    assert isinstance(output, DestinationPlugin)
    assert output.__repr__()


def test_wrong_scheme_raises_value_error():
    uri = "wrongscheme://path/to/destination_table"
    destination_table = "output_table"
    with pytest.raises(DestinationConfigurationError) as e:
        PostgresDestination(uri, destination_table)
    assert e.value.error_code == ErrorCode.DECE2


def test_destination_table_list():
    uri = "postgres://DATABASE_IP:DATABASE_PORT/testing"
    destination_table = ["headers_table", "items_table"]
    output = PostgresDestination(uri, destination_table)
    assert output.destination_table == destination_table


def test_destination_table_wrong_type_raises_type_error():
    uri = "postgres://DATABASE_IP:DATABASE_PORT/testing"
    destination_table = 42
    with pytest.raises(DestinationConfigurationError) as e:
        PostgresDestination(uri, destination_table)
    assert e.value.error_code == ErrorCode.DECE20


def test_wrong_credentials_type_raises_type_error():
    uri = "postgres://DATABASE_IP:DATABASE_PORT/testing"
    destination_table = "output_table"
    credentials = S3AccessKeyCredentials("admin", "admin")
    with pytest.raises(DestinationConfigurationError) as e:
        PostgresDestination(uri, destination_table, credentials=credentials)
    assert e.value.error_code == ErrorCode.DECE21


def test_empty_uri():
    uri = ""
    destination_table = "output_table"
    with pytest.raises(DestinationConfigurationError) as e:
        PostgresDestination(uri, destination_table)
    assert e.value.error_code == ErrorCode.DECE2


def test_update_destination_table():
    uri = "postgres://DATABASE_IP:DATABASE_PORT/testing"
    destination_table = "output_table"
    output = PostgresDestination(uri, destination_table)
    assert output.destination_table == destination_table
    output.destination_table = "new_output_table"
    assert output.destination_table == "new_output_table"
    output.destination_table = ["new_output_table", "new_output_table2"]
    assert output.destination_table == ["new_output_table", "new_output_table2"]


def test_update_configs():
    uri = "postgres://DATABASE_IP:DATABASE_PORT/testing"
    destination_table = "output_table"
    output = PostgresDestination(uri, destination_table)
    assert output.credentials is None
    output.credentials = UserPasswordCredentials("admin", "admin")
    assert output.credentials == UserPasswordCredentials("admin", "admin")


def test_update_uri():
    uri = "postgres://DATABASE_IP:DATABASE_PORT/testing"
    destination_table = "output_table"
    output = PostgresDestination(uri, destination_table)
    assert output.uri == uri
    output.uri = "postgres://DATABASE_IP:DATABASE_PORT/testing2"
    assert output.uri == "postgres://DATABASE_IP:DATABASE_PORT/testing2"


def test_update_if_table_exists():
    uri = "postgres://DATABASE_IP:DATABASE_PORT/testing"
    destination_table = "output_table"
    output = PostgresDestination(uri, destination_table, if_table_exists="replace")
    assert output.if_table_exists == "replace"
    output.if_table_exists = "append"
    assert output.if_table_exists == "append"


def test_if_table_exists_wrong_parameter_raises_exception():
    uri = "postgres://DATABASE_IP:DATABASE_PORT/testing"
    destination_table = "output_table"
    with pytest.raises(DestinationConfigurationError) as e:
        PostgresDestination(uri, destination_table, if_table_exists="wrong")
    assert e.value.error_code == ErrorCode.DECE29
    with pytest.raises(DestinationConfigurationError) as e:
        PostgresDestination(uri, destination_table, if_table_exists=42)
    assert e.value.error_code == ErrorCode.DECE29


def test_all_correct_driver():
    uri = "postgres+driver://DATABASE_IP:DATABASE_PORT/testing"
    destination_table = "output_table"
    credentials = UserPasswordCredentials("admin", "admin")
    output = PostgresDestination(uri, destination_table, credentials=credentials)
    assert output.uri == uri
    assert output.destination_table == destination_table
    assert output.credentials == credentials
    assert isinstance(output, PostgresDestination)
    assert isinstance(output, DestinationPlugin)
    assert output.__repr__()


def test_all_correct_driver_postgresql():
    uri = "postgresql+driver://DATABASE_IP:DATABASE_PORT/testing"
    destination_table = "output_table"
    credentials = UserPasswordCredentials("admin", "admin")
    output = PostgresDestination(uri, destination_table, credentials=credentials)
    assert output.uri == uri
    assert output.destination_table == destination_table
    assert output.credentials == credentials
    assert isinstance(output, PostgresDestination)
    assert isinstance(output, DestinationPlugin)
    assert output.__repr__()
