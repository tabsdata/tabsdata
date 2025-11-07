#
# Copyright 2024 Tabs Data Inc.
#

import copy
from urllib.parse import urlparse

import pytest

from tabsdata import CSVFormat, ParquetFormat
from tabsdata._credentials import AzureAccountKeyCredentials, UserPasswordCredentials
from tabsdata._io.outputs.file_outputs import (
    FRAGMENT_INDEX_PLACEHOLDER,
    AzureDestination,
)
from tabsdata._io.plugin import DestinationPlugin
from tabsdata.exceptions import (
    DestinationConfigurationError,
    ErrorCode,
    FormatConfigurationError,
)
from tests_tabsdata.conftest import FORMAT_TYPE_TO_CONFIG

# noinspection PyUnresolvedReferences
from . import pytestmark  # noqa: F401

TEST_ACCOUNT_NAME = "test_account_name"
TEST_ACCOUNT_KEY = "test_account_key"
AZURE_CREDENTIALS = AzureAccountKeyCredentials(
    account_name=TEST_ACCOUNT_NAME,
    account_key=TEST_ACCOUNT_KEY,
)


def test_all_correct_implicit_format():
    uri = "az://path/to/data/data.csv"
    output = AzureDestination(uri, AZURE_CREDENTIALS)
    assert output.uri == uri
    assert isinstance(output.format, CSVFormat)
    assert isinstance(output, AzureDestination)
    assert isinstance(output, DestinationPlugin)
    assert output.credentials == AZURE_CREDENTIALS
    assert output.__repr__()


def test_uri_upper():
    uri = "az://path/to/data/data.csv".upper()
    output = AzureDestination(uri, AZURE_CREDENTIALS)
    assert output.uri == uri


def test_all_correct_uri_list():
    uri = ["az://path/to/data/data.csv", "az://path/to/data/data2.csv"]
    output = AzureDestination(uri, AZURE_CREDENTIALS)
    assert output.uri == uri
    assert isinstance(output.format, CSVFormat)
    assert isinstance(output, AzureDestination)
    assert isinstance(output, DestinationPlugin)
    assert output.credentials == AZURE_CREDENTIALS
    assert output.__repr__()


def test_uri_list_update_to_string():
    uri = [
        "az://path/to/data/invoice-headers.csv",
        "az://path/to/data/invoice-items-*.csv",
    ]
    output = AzureDestination(uri, AZURE_CREDENTIALS)
    assert output.uri == uri
    assert output._uri_list == uri
    output = AzureDestination(
        "az://path/to/data/invoice-headers.csv", AZURE_CREDENTIALS
    )
    assert output._uri_list == ["az://path/to/data/invoice-headers.csv"]


def test_parsed_uri_list():
    uri = [
        "az://path/to/data/invoice-headers.csv",
        "az://path/to/data/invoice-items-*.csv",
    ]
    output = AzureDestination(uri, AZURE_CREDENTIALS)
    assert output.uri == uri
    assert output._uri_list == uri
    assert output._parsed_uri_list == [urlparse(uri[0]), urlparse(uri[1])]
    uri = "az://path/to/data/invoice-headers.csv"
    output = AzureDestination(uri, AZURE_CREDENTIALS)
    assert output.uri == uri
    assert output._uri_list == [uri]
    assert output._parsed_uri_list == [urlparse(uri)]


def test_update_uri():
    uri = ["az://path/to/data/data.csv", "az://path/to/data/data2.csv"]
    output = AzureDestination(uri, AZURE_CREDENTIALS)
    assert output.uri == uri
    assert output._uri_list == uri
    assert output._parsed_uri_list == [urlparse(uri[0]), urlparse(uri[1])]
    uri2 = ["az://path/to/data/data.csv", "az://path/to/data/data3.csv"]
    output.uri = uri2
    assert output.uri == uri2
    assert output._uri_list == uri2
    assert output._parsed_uri_list == [urlparse(uri2[0]), urlparse(uri2[1])]


def test_update_uri_implicit_format():
    uri = ["az://path/to/data/data.csv", "az://path/to/data/data2.csv"]
    output = AzureDestination(uri, AZURE_CREDENTIALS)
    assert output.uri == uri
    assert isinstance(output.format, CSVFormat)
    uri2 = ["az://path/to/data/data.parquet", "az://path/to/data/data3.parquet"]
    output.uri = uri2
    assert output.uri == uri2
    assert isinstance(output.format, ParquetFormat)


def test_update_uri_explicit_format():
    uri = ["az://path/to/data/data.csv", "az://path/to/data/data2.csv"]
    output = AzureDestination(uri, AZURE_CREDENTIALS, format="csv")
    assert output.uri == uri
    assert isinstance(output.format, CSVFormat)
    uri2 = ["az://path/to/data/data.parquet", "az://path/to/data/data3.parquet"]
    output.uri = uri2
    assert output.uri == uri2
    assert isinstance(output.format, CSVFormat)


def test_uri_path_mixed_format():
    uri = ["az://path/to/data/data.csv", "az://path/to/data/data2.csv"]
    output = AzureDestination(uri, AZURE_CREDENTIALS)
    assert output.uri == uri
    assert isinstance(output.format, CSVFormat)
    assert output._format is None
    uri2 = ["az://path/to/data/data.parquet", "az://path/to/data/data3.parquet"]
    output.uri = uri2
    assert output.uri == uri2
    assert isinstance(output.format, ParquetFormat)
    assert output._format is None
    output.format = CSVFormat()
    assert output.format == CSVFormat()
    assert output._format == CSVFormat()
    output.uri = uri2
    assert output.uri == uri2
    assert isinstance(output.format, CSVFormat)
    assert output._format == CSVFormat()
    output.format = ParquetFormat()
    assert isinstance(output.format, ParquetFormat)
    assert output._format == ParquetFormat()


def test_update_format():
    uri = ["az://path/to/data/data.csv", "az://path/to/data/data2.csv"]
    output = AzureDestination(uri, AZURE_CREDENTIALS)
    assert output.format == CSVFormat()
    format = CSVFormat(separator=";", input_has_header=False)
    output.format = format
    assert output.format == format


def test_update_credentials():
    uri = ["az://path/to/data/data.csv", "az://path/to/data/data2.csv"]
    output = AzureDestination(uri, AZURE_CREDENTIALS)
    assert output.credentials == AZURE_CREDENTIALS
    credentials = AzureAccountKeyCredentials(
        account_name="new_account_name",
        account_key="new_account_key",
    )
    output.credentials = credentials
    assert output.credentials == credentials


def test_wrong_type_credentials_raises_error():
    uri = ["az://path/to/data/data.csv", "az://path/to/data/data2.csv"]
    credentials = UserPasswordCredentials("username", "password")
    with pytest.raises(DestinationConfigurationError) as e:
        AzureDestination(uri, credentials)
    assert e.value.error_code == ErrorCode.DECE16


def test_different_output_not_eq():
    uri = ["az://path/to/data/data.csv", "az://path/to/data/data2.csv"]
    output = AzureDestination(uri, AZURE_CREDENTIALS)
    uri2 = ["az://path/to/data/data.csv", "az://path/to/data/data3.csv"]
    output2 = AzureDestination(uri2, AZURE_CREDENTIALS)
    assert output != output2


def test_output_not_eq_dict():
    uri = ["az://path/to/data/data.csv", "az://path/to/data/data2.csv"]
    output = AzureDestination(uri, AZURE_CREDENTIALS)
    assert output._to_dict() != output


def test_all_correct_explicit_format():
    uri = "az://path/to/data/data"
    format = "csv"
    output = AzureDestination(uri, AZURE_CREDENTIALS, format=format)
    assert output.uri == uri
    assert isinstance(output.format, CSVFormat)
    assert isinstance(output, AzureDestination)
    assert isinstance(output, DestinationPlugin)


def test_wrong_scheme_raises_value_error():
    uri = "wrongscheme://path/to/data/data.csv"
    with pytest.raises(DestinationConfigurationError) as e:
        AzureDestination(uri, AZURE_CREDENTIALS)
    assert e.value.error_code == ErrorCode.DECE15


def test_empty_scheme_raises_value_error():
    uri = "path/to/data/data.csv"
    with pytest.raises(DestinationConfigurationError) as e:
        AzureDestination(uri, AZURE_CREDENTIALS)
    assert e.value.error_code == ErrorCode.DECE15


def test_list_of_integers_raises_exception():
    uri = [1, 2, "hi"]
    with pytest.raises(DestinationConfigurationError) as e:
        AzureDestination(uri, AZURE_CREDENTIALS)
    assert e.value.error_code == ErrorCode.DECE14


def test_uri_list():
    uri = [
        "az://path/to/data/invoice-headers.csv",
        "az://path/to/data/invoice-items-*.csv",
    ]
    output = AzureDestination(uri, AZURE_CREDENTIALS)
    assert output.uri == uri


def test_uri_wrong_type_raises_type_error():
    uri = 42
    with pytest.raises(DestinationConfigurationError) as e:
        AzureDestination(uri, AZURE_CREDENTIALS)
    assert e.value.error_code == ErrorCode.DECE14


def test_format_from_uri_list():
    uri = [
        "az://path/to/data/invoice-headers.csv",
        "az://path/to/data/invoice-items-*.csv",
    ]
    output = AzureDestination(uri, AZURE_CREDENTIALS)
    assert isinstance(output.format, CSVFormat)


def test_no_implicit_format_raises_value_error():
    uri = "az://path/to/data/data"
    with pytest.raises(FormatConfigurationError) as e:
        AzureDestination(uri, AZURE_CREDENTIALS)
    assert e.value.error_code == ErrorCode.FOCE6


def test_correct_format_object():
    uri = "az://path/to/data/data"
    format = CSVFormat(separator=".", input_has_header=False)
    expected_format = copy.deepcopy(FORMAT_TYPE_TO_CONFIG["csv"])
    expected_format["separator"] = "."
    expected_format["input_has_header"] = False

    output = AzureDestination(uri, AZURE_CREDENTIALS, format=format)
    assert output.format._to_dict()[CSVFormat.IDENTIFIER] == expected_format
    assert isinstance(output, AzureDestination)
    assert isinstance(output, DestinationPlugin)


def test_incorrect_data_format_raises_value_error():
    uri = "az://path/to/data/data.wrongformat"
    with pytest.raises(FormatConfigurationError) as e:
        AzureDestination(uri, AZURE_CREDENTIALS)
    assert e.value.error_code == ErrorCode.FOCE4


def test_wrong_implicit_format_raises_value_error():
    uri = "az://path/to/data/data.wrong"
    with pytest.raises(FormatConfigurationError) as e:
        AzureDestination(uri, AZURE_CREDENTIALS)
    assert e.value.error_code == ErrorCode.FOCE4


def test_wrong_explicit_format_raises_value_error():
    uri = "az://path/to/data/data.csv"
    format = "wrong"
    with pytest.raises(FormatConfigurationError) as e:
        AzureDestination(uri, AZURE_CREDENTIALS, format=format)
    assert e.value.error_code == ErrorCode.FOCE4


def test_empty_format():
    uri = "az://path/to/data/data"
    format = ""
    with pytest.raises(FormatConfigurationError) as e:
        AzureDestination(uri, AZURE_CREDENTIALS, format=format)
    assert e.value.error_code == ErrorCode.FOCE4


def test_wrong_type_format_raises_type_error():
    uri = "az://path/to/data/data.csv"
    format = 42
    with pytest.raises(FormatConfigurationError) as e:
        AzureDestination(uri, AZURE_CREDENTIALS, format=format)
    assert e.value.error_code == ErrorCode.FOCE5


def test_allow_fragments():
    uri = f"az://path/to/data/data_{FRAGMENT_INDEX_PLACEHOLDER}.csv"
    with pytest.raises(DestinationConfigurationError) as e:
        AzureDestination(uri, AZURE_CREDENTIALS)
    assert e.value.error_code == ErrorCode.DECE38
    uri = [
        "az://path/to/data/data",
        f"az://path/to/data/data_{FRAGMENT_INDEX_PLACEHOLDER}.csv",
    ]
    with pytest.raises(DestinationConfigurationError) as e:
        AzureDestination(uri, AZURE_CREDENTIALS)
    assert e.value.error_code == ErrorCode.DECE38
    uri = "az://path/to/data/data.csv"
    output = AzureDestination(uri, AZURE_CREDENTIALS)
    assert output.uri == uri
    assert not output.allow_fragments
