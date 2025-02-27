#
# Copyright 2024 Tabs Data Inc.
#

import copy
from urllib.parse import urlparse

import pytest

from tabsdata import CSVFormat, DirectSecret, ParquetFormat
from tabsdata.credentials import AzureAccountKeyCredentials, UserPasswordCredentials
from tabsdata.exceptions import (
    ErrorCode,
    FormatConfigurationError,
    OutputConfigurationError,
)
from tabsdata.tabsdatafunction import AzureDestination, Output, build_output
from tests.conftest import FORMAT_TYPE_TO_CONFIG

TEST_ACCOUNT_NAME = "test_account_name"
TEST_ACCOUNT_KEY = "test_account_key"
AZURE_CREDENTIALS = AzureAccountKeyCredentials(
    account_name=TEST_ACCOUNT_NAME,
    account_key=TEST_ACCOUNT_KEY,
)
CREDENTIALS_DICT = {
    AzureAccountKeyCredentials.IDENTIFIER: {
        AzureAccountKeyCredentials.ACCOUNT_NAME_KEY: (
            DirectSecret(TEST_ACCOUNT_NAME).to_dict()
        ),
        AzureAccountKeyCredentials.ACCOUNT_KEY_KEY: (
            DirectSecret(TEST_ACCOUNT_KEY).to_dict()
        ),
    }
}


def test_all_correct_implicit_format():
    uri = "az://path/to/data/data.csv"
    output = AzureDestination(uri, AZURE_CREDENTIALS)
    assert output.uri == uri
    assert isinstance(output.format, CSVFormat)
    assert isinstance(output, AzureDestination)
    assert isinstance(output, Output)
    assert output.credentials == AZURE_CREDENTIALS
    expected_dict = {
        AzureDestination.IDENTIFIER: {
            AzureDestination.URI_KEY: [uri],
            AzureDestination.FORMAT_KEY: {
                CSVFormat.IDENTIFIER: FORMAT_TYPE_TO_CONFIG["csv"]
            },
            AzureDestination.CREDENTIALS_KEY: CREDENTIALS_DICT,
        }
    }
    assert output.to_dict() == expected_dict
    assert isinstance(build_output(output.to_dict()), AzureDestination)
    assert output.__repr__()


def test_all_correct_uri_list():
    uri = ["az://path/to/data/data.csv", "az://path/to/data/data2.csv"]
    output = AzureDestination(uri, AZURE_CREDENTIALS)
    assert output.uri == uri
    assert isinstance(output.format, CSVFormat)
    assert isinstance(output, AzureDestination)
    assert isinstance(output, Output)
    assert output.credentials == AZURE_CREDENTIALS
    expected_dict = {
        AzureDestination.IDENTIFIER: {
            AzureDestination.URI_KEY: uri,
            AzureDestination.FORMAT_KEY: {
                CSVFormat.IDENTIFIER: FORMAT_TYPE_TO_CONFIG["csv"]
            },
            AzureDestination.CREDENTIALS_KEY: CREDENTIALS_DICT,
        }
    }
    assert output.to_dict() == expected_dict
    assert isinstance(build_output(output.to_dict()), AzureDestination)
    assert output.__repr__()


def test_same_output_eq():
    uri = ["az://path/to/data/data.csv", "az://path/to/data/data2.csv"]
    output = AzureDestination(uri, AZURE_CREDENTIALS)
    output2 = AzureDestination(uri, AZURE_CREDENTIALS)
    assert output == output2


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
    with pytest.raises(OutputConfigurationError) as e:
        AzureDestination(uri, credentials)
    assert e.value.error_code == ErrorCode.OCE16


def test_different_output_not_eq():
    uri = ["az://path/to/data/data.csv", "az://path/to/data/data2.csv"]
    output = AzureDestination(uri, AZURE_CREDENTIALS)
    uri2 = ["az://path/to/data/data.csv", "az://path/to/data/data3.csv"]
    output2 = AzureDestination(uri2, AZURE_CREDENTIALS)
    assert output != output2


def test_output_not_eq_dict():
    uri = ["az://path/to/data/data.csv", "az://path/to/data/data2.csv"]
    output = AzureDestination(uri, AZURE_CREDENTIALS)
    assert output.to_dict() != output


def test_all_correct_explicit_format():
    uri = "az://path/to/data/data"
    format = "csv"
    output = AzureDestination(uri, AZURE_CREDENTIALS, format=format)
    assert output.uri == uri
    assert isinstance(output.format, CSVFormat)
    assert isinstance(output, AzureDestination)
    assert isinstance(output, Output)
    expected_dict = {
        AzureDestination.IDENTIFIER: {
            AzureDestination.URI_KEY: [uri],
            AzureDestination.FORMAT_KEY: {
                CSVFormat.IDENTIFIER: FORMAT_TYPE_TO_CONFIG["csv"]
            },
            AzureDestination.CREDENTIALS_KEY: CREDENTIALS_DICT,
        }
    }
    assert output.to_dict() == expected_dict
    assert isinstance(build_output(output.to_dict()), AzureDestination)


def test_wrong_scheme_raises_value_error():
    uri = "wrongscheme://path/to/data/data.csv"
    with pytest.raises(OutputConfigurationError) as e:
        AzureDestination(uri, AZURE_CREDENTIALS)
    assert e.value.error_code == ErrorCode.OCE15


def test_empty_scheme_raises_value_error():
    uri = "path/to/data/data.csv"
    with pytest.raises(OutputConfigurationError) as e:
        AzureDestination(uri, AZURE_CREDENTIALS)
    assert e.value.error_code == ErrorCode.OCE15


def test_list_of_integers_raises_exception():
    uri = [1, 2, "hi"]
    with pytest.raises(OutputConfigurationError) as e:
        AzureDestination(uri, AZURE_CREDENTIALS)
    assert e.value.error_code == ErrorCode.OCE14


def test_uri_list():
    uri = [
        "az://path/to/data/invoice-headers.csv",
        "az://path/to/data/invoice-items-*.csv",
    ]
    output = AzureDestination(uri, AZURE_CREDENTIALS)
    assert output.uri == uri


def test_uri_wrong_type_raises_type_error():
    uri = 42
    with pytest.raises(OutputConfigurationError) as e:
        AzureDestination(uri, AZURE_CREDENTIALS)
    assert e.value.error_code == ErrorCode.OCE14


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
    assert output.format.to_dict()[CSVFormat.IDENTIFIER] == expected_format
    assert isinstance(output, AzureDestination)
    assert isinstance(output, Output)
    expected_dict = {
        AzureDestination.IDENTIFIER: {
            AzureDestination.URI_KEY: [uri],
            AzureDestination.FORMAT_KEY: {CSVFormat.IDENTIFIER: expected_format},
            AzureDestination.CREDENTIALS_KEY: CREDENTIALS_DICT,
        }
    }
    assert output.to_dict() == expected_dict
    assert isinstance(build_output(output.to_dict()), AzureDestination)


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


def test_build_output_wrong_type_raises_error():
    with pytest.raises(OutputConfigurationError) as e:
        build_output(42)
    assert e.value.error_code == ErrorCode.OCE7


def test_identifier_string_unchanged():
    uri = "az://path/to/data/data"
    format = "csv"
    output = AzureDestination(uri, AZURE_CREDENTIALS, format=format)
    expected_dict = {
        "azure-output": {
            AzureDestination.URI_KEY: [uri],
            AzureDestination.FORMAT_KEY: {
                CSVFormat.IDENTIFIER: FORMAT_TYPE_TO_CONFIG["csv"]
            },
            AzureDestination.CREDENTIALS_KEY: CREDENTIALS_DICT,
        }
    }
    assert output.to_dict() == expected_dict
    assert isinstance(build_output(output.to_dict()), AzureDestination)
