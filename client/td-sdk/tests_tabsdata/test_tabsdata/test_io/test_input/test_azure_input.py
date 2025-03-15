#
# Copyright 2024 Tabs Data Inc.
#

import copy
import datetime
from urllib.parse import urlparse

import pytest
from tests_tabsdata.conftest import FORMAT_TYPE_TO_CONFIG

from tabsdata import CSVFormat, DirectSecret, ParquetFormat
from tabsdata.credentials import AzureAccountKeyCredentials, UserPasswordCredentials
from tabsdata.exceptions import (
    ErrorCode,
    FormatConfigurationError,
    InputConfigurationError,
)
from tabsdata.io.input import AzureSource, Input, build_input

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
    input = AzureSource(uri, AZURE_CREDENTIALS)
    assert input.uri == uri
    assert isinstance(input.format, CSVFormat)
    assert isinstance(input, AzureSource)
    assert isinstance(input, Input)
    assert input.credentials == AZURE_CREDENTIALS
    expected_dict = {
        AzureSource.IDENTIFIER: {
            AzureSource.URI_KEY: [uri],
            AzureSource.FORMAT_KEY: {
                CSVFormat.IDENTIFIER: FORMAT_TYPE_TO_CONFIG["csv"]
            },
            AzureSource.CREDENTIALS_KEY: CREDENTIALS_DICT,
            "initial_last_modified": None,
        }
    }
    assert input.to_dict() == expected_dict
    assert isinstance(build_input(input.to_dict()), AzureSource)
    assert input.__repr__()


def test_all_correct_uri_list():
    uri = ["az://path/to/data/data.csv", "az://path/to/data/data2.csv"]
    input = AzureSource(uri, AZURE_CREDENTIALS)
    assert input.uri == uri
    assert isinstance(input.format, CSVFormat)
    assert isinstance(input, AzureSource)
    assert isinstance(input, Input)
    assert input.credentials == AZURE_CREDENTIALS
    expected_dict = {
        AzureSource.IDENTIFIER: {
            AzureSource.URI_KEY: uri,
            AzureSource.FORMAT_KEY: {
                CSVFormat.IDENTIFIER: FORMAT_TYPE_TO_CONFIG["csv"]
            },
            AzureSource.CREDENTIALS_KEY: CREDENTIALS_DICT,
            "initial_last_modified": None,
        }
    }
    assert input.to_dict() == expected_dict
    assert isinstance(build_input(input.to_dict()), AzureSource)
    assert input.__repr__()


def test_same_input_eq():
    uri = ["az://path/to/data/data.csv", "az://path/to/data/data2.csv"]
    input = AzureSource(uri, AZURE_CREDENTIALS)
    input2 = AzureSource(uri, AZURE_CREDENTIALS)
    assert input == input2


def test_uri_list_update_to_string():
    uri = [
        "az://path/to/data/invoice-headers.csv",
        "az://path/to/data/invoice-items-*.csv",
    ]
    input = AzureSource(uri, AZURE_CREDENTIALS)
    assert input.uri == uri
    assert input._uri_list == uri
    input = AzureSource("az://path/to/data/invoice-headers.csv", AZURE_CREDENTIALS)
    assert input._uri_list == ["az://path/to/data/invoice-headers.csv"]


def test_parsed_uri_list():
    uri = [
        "az://path/to/data/invoice-headers.csv",
        "az://path/to/data/invoice-items-*.csv",
    ]
    input = AzureSource(uri, AZURE_CREDENTIALS)
    assert input.uri == uri
    assert input._uri_list == uri
    assert input._parsed_uri_list == [urlparse(uri[0]), urlparse(uri[1])]
    uri = "az://path/to/data/invoice-headers.csv"
    input = AzureSource(uri, AZURE_CREDENTIALS)
    assert input.uri == uri
    assert input._uri_list == [uri]
    assert input._parsed_uri_list == [urlparse(uri)]


def test_update_uri():
    uri = ["az://path/to/data/data.csv", "az://path/to/data/data2.csv"]
    input = AzureSource(uri, AZURE_CREDENTIALS)
    assert input.uri == uri
    assert input._uri_list == uri
    assert input._parsed_uri_list == [urlparse(uri[0]), urlparse(uri[1])]
    uri2 = ["az://path/to/data/data.csv", "az://path/to/data/data3.csv"]
    input.uri = uri2
    assert input.uri == uri2
    assert input._uri_list == uri2
    assert input._parsed_uri_list == [urlparse(uri2[0]), urlparse(uri2[1])]


def test_update_uri_implicit_format():
    uri = ["az://path/to/data/data.csv", "az://path/to/data/data2.csv"]
    input = AzureSource(uri, AZURE_CREDENTIALS)
    assert input.uri == uri
    assert isinstance(input.format, CSVFormat)
    uri2 = ["az://path/to/data/data.parquet", "az://path/to/data/data3.parquet"]
    input.uri = uri2
    assert input.uri == uri2
    assert isinstance(input.format, ParquetFormat)


def test_update_uri_explicit_format():
    uri = ["az://path/to/data/data.csv", "az://path/to/data/data2.csv"]
    input = AzureSource(uri, AZURE_CREDENTIALS, format="csv")
    assert input.uri == uri
    assert isinstance(input.format, CSVFormat)
    uri2 = ["az://path/to/data/data.parquet", "az://path/to/data/data3.parquet"]
    input.uri = uri2
    assert input.uri == uri2
    assert isinstance(input.format, CSVFormat)


def test_uri_path_mixed_format():
    uri = ["az://path/to/data/data.csv", "az://path/to/data/data2.csv"]
    input = AzureSource(uri, AZURE_CREDENTIALS)
    assert input.uri == uri
    assert isinstance(input.format, CSVFormat)
    assert input._format is None
    uri2 = ["az://path/to/data/data.parquet", "az://path/to/data/data3.parquet"]
    input.uri = uri2
    assert input.uri == uri2
    assert isinstance(input.format, ParquetFormat)
    assert input._format is None
    input.format = CSVFormat()
    assert input.format == CSVFormat()
    assert input._format == CSVFormat()
    input.uri = uri2
    assert input.uri == uri2
    assert isinstance(input.format, CSVFormat)
    assert input._format == CSVFormat()
    input.format = ParquetFormat()
    assert isinstance(input.format, ParquetFormat)
    assert input._format == ParquetFormat()


def test_update_format():
    uri = ["az://path/to/data/data.csv", "az://path/to/data/data2.csv"]
    input = AzureSource(uri, AZURE_CREDENTIALS)
    assert input.format == CSVFormat()
    format = CSVFormat(separator=";", input_has_header=False)
    input.format = format
    assert input.format == format


def test_update_credentials():
    uri = ["az://path/to/data/data.csv", "az://path/to/data/data2.csv"]
    input = AzureSource(uri, AZURE_CREDENTIALS)
    assert input.credentials == AZURE_CREDENTIALS
    credentials = AzureAccountKeyCredentials(
        account_name="new_account_name",
        account_key="new_account_key",
    )
    input.credentials = credentials
    assert input.credentials == credentials


def test_wrong_type_credentials_raises_error():
    uri = ["az://path/to/data/data.csv", "az://path/to/data/data2.csv"]
    credentials = UserPasswordCredentials("username", "password")
    with pytest.raises(InputConfigurationError) as e:
        AzureSource(uri, credentials)
    assert e.value.error_code == ErrorCode.ICE30


def test_different_input_not_eq():
    uri = ["az://path/to/data/data.csv", "az://path/to/data/data2.csv"]
    input = AzureSource(uri, AZURE_CREDENTIALS)
    uri2 = ["az://path/to/data/data.csv", "az://path/to/data/data3.csv"]
    input2 = AzureSource(uri2, AZURE_CREDENTIALS)
    assert input != input2


def test_input_not_eq_dict():
    uri = ["az://path/to/data/data.csv", "az://path/to/data/data2.csv"]
    input = AzureSource(uri, AZURE_CREDENTIALS)
    assert input.to_dict() != input


def test_all_correct_explicit_format():
    uri = "az://path/to/data/data"
    format = "csv"
    input = AzureSource(uri, AZURE_CREDENTIALS, format=format)
    assert input.uri == uri
    assert isinstance(input.format, CSVFormat)
    assert isinstance(input, AzureSource)
    assert isinstance(input, Input)
    expected_dict = {
        AzureSource.IDENTIFIER: {
            AzureSource.URI_KEY: [uri],
            AzureSource.FORMAT_KEY: {
                CSVFormat.IDENTIFIER: FORMAT_TYPE_TO_CONFIG["csv"]
            },
            AzureSource.CREDENTIALS_KEY: CREDENTIALS_DICT,
            "initial_last_modified": None,
        }
    }
    assert input.to_dict() == expected_dict
    assert isinstance(build_input(input.to_dict()), AzureSource)


def test_wrong_scheme_raises_value_error():
    uri = "wrongscheme://path/to/data/data.csv"
    with pytest.raises(InputConfigurationError) as e:
        AzureSource(uri, AZURE_CREDENTIALS)
    assert e.value.error_code == ErrorCode.ICE29


def test_empty_scheme_raises_value_error():
    uri = "path/to/data/data.csv"
    with pytest.raises(InputConfigurationError) as e:
        AzureSource(uri, AZURE_CREDENTIALS)
    assert e.value.error_code == ErrorCode.ICE29


def test_list_of_integers_raises_exception():
    uri = [1, 2, "hi"]
    with pytest.raises(InputConfigurationError) as e:
        AzureSource(uri, AZURE_CREDENTIALS)
    assert e.value.error_code == ErrorCode.ICE28


def test_uri_list():
    uri = [
        "az://path/to/data/invoice-headers.csv",
        "az://path/to/data/invoice-items-*.csv",
    ]
    input = AzureSource(uri, AZURE_CREDENTIALS)
    assert input.uri == uri


def test_uri_wrong_type_raises_type_error():
    uri = 42
    with pytest.raises(InputConfigurationError) as e:
        AzureSource(uri, AZURE_CREDENTIALS)
    assert e.value.error_code == ErrorCode.ICE28


def test_format_from_uri_list():
    uri = [
        "az://path/to/data/invoice-headers.csv",
        "az://path/to/data/invoice-items-*.csv",
    ]
    input = AzureSource(uri, AZURE_CREDENTIALS)
    assert isinstance(input.format, CSVFormat)


def test_no_implicit_format_raises_value_error():
    uri = "az://path/to/data/data"
    with pytest.raises(FormatConfigurationError) as e:
        AzureSource(uri, AZURE_CREDENTIALS)
    assert e.value.error_code == ErrorCode.FOCE6


def test_correct_format_object():
    uri = "az://path/to/data/data"
    format = CSVFormat(separator=".", input_has_header=False)
    expected_format = copy.deepcopy(FORMAT_TYPE_TO_CONFIG["csv"])
    expected_format["separator"] = "."
    expected_format["input_has_header"] = False

    input = AzureSource(uri, AZURE_CREDENTIALS, format=format)
    assert input.format.to_dict()[CSVFormat.IDENTIFIER] == expected_format
    assert isinstance(input, AzureSource)
    assert isinstance(input, Input)
    expected_dict = {
        AzureSource.IDENTIFIER: {
            AzureSource.URI_KEY: [uri],
            AzureSource.FORMAT_KEY: {CSVFormat.IDENTIFIER: expected_format},
            AzureSource.CREDENTIALS_KEY: CREDENTIALS_DICT,
            "initial_last_modified": None,
        }
    }
    assert input.to_dict() == expected_dict
    assert isinstance(build_input(input.to_dict()), AzureSource)


def test_incorrect_data_format_raises_value_error():
    uri = "az://path/to/data/data.wrongformat"
    with pytest.raises(FormatConfigurationError) as e:
        AzureSource(uri, AZURE_CREDENTIALS)
    assert e.value.error_code == ErrorCode.FOCE4


def test_wrong_implicit_format_raises_value_error():
    uri = "az://path/to/data/data.wrong"
    with pytest.raises(FormatConfigurationError) as e:
        AzureSource(uri, AZURE_CREDENTIALS)
    assert e.value.error_code == ErrorCode.FOCE4


def test_wrong_explicit_format_raises_value_error():
    uri = "az://path/to/data/data.csv"
    format = "wrong"
    with pytest.raises(FormatConfigurationError) as e:
        AzureSource(uri, AZURE_CREDENTIALS, format=format)
    assert e.value.error_code == ErrorCode.FOCE4


def test_empty_format():
    uri = "az://path/to/data/data"
    format = ""
    with pytest.raises(FormatConfigurationError) as e:
        AzureSource(uri, AZURE_CREDENTIALS, format=format)
    assert e.value.error_code == ErrorCode.FOCE4


def test_wrong_type_format_raises_type_error():
    uri = "az://path/to/data/data.csv"
    format = 42
    with pytest.raises(FormatConfigurationError) as e:
        AzureSource(uri, AZURE_CREDENTIALS, format=format)
    assert e.value.error_code == ErrorCode.FOCE5


def test_initial_last_modified_none():
    uri = "az://path/to/data/data"
    format = "csv"
    input = AzureSource(
        uri, AZURE_CREDENTIALS, format=format, initial_last_modified=None
    )
    assert input.initial_last_modified is None
    assert isinstance(input, AzureSource)
    assert isinstance(input, Input)
    expected_dict = {
        AzureSource.IDENTIFIER: {
            AzureSource.URI_KEY: [uri],
            AzureSource.FORMAT_KEY: {
                CSVFormat.IDENTIFIER: FORMAT_TYPE_TO_CONFIG["csv"]
            },
            AzureSource.CREDENTIALS_KEY: CREDENTIALS_DICT,
            "initial_last_modified": None,
        }
    }
    assert input.to_dict() == expected_dict
    assert isinstance(build_input(input.to_dict()), AzureSource)


def test_initial_last_modified_valid_string():
    uri = "az://path/to/data/data"
    format = "csv"
    time = "2024-09-05T01:01:00.01"
    input = AzureSource(
        uri, AZURE_CREDENTIALS, format=format, initial_last_modified=time
    )
    assert input.initial_last_modified == datetime.datetime.fromisoformat(
        time
    ).isoformat(timespec="microseconds")
    assert isinstance(input, AzureSource)
    assert isinstance(input, Input)
    expected_dict = {
        AzureSource.IDENTIFIER: {
            AzureSource.URI_KEY: [uri],
            AzureSource.FORMAT_KEY: {
                CSVFormat.IDENTIFIER: FORMAT_TYPE_TO_CONFIG["csv"]
            },
            AzureSource.CREDENTIALS_KEY: CREDENTIALS_DICT,
            "initial_last_modified": (
                datetime.datetime.fromisoformat(time).isoformat(timespec="microseconds")
            ),
        }
    }
    assert input.to_dict() == expected_dict
    assert isinstance(build_input(input.to_dict()), AzureSource)


def test_initial_last_modified_invalid_string():
    uri = "az://path/to/data/data"
    format = "csv"
    time = "wrong_time"
    with pytest.raises(InputConfigurationError) as e:
        AzureSource(uri, AZURE_CREDENTIALS, format=format, initial_last_modified=time)
    assert e.value.error_code == ErrorCode.ICE5


def test_initial_last_modified_valid_datetime():
    uri = "az://path/to/data/data"
    format = "csv"
    time = datetime.datetime(2024, 9, 5, 1, 1, 0, 10000)
    input = AzureSource(
        uri, AZURE_CREDENTIALS, format=format, initial_last_modified=time
    )
    assert input.initial_last_modified == time.isoformat(timespec="microseconds")
    assert isinstance(input, AzureSource)
    assert isinstance(input, Input)
    expected_dict = {
        AzureSource.IDENTIFIER: {
            AzureSource.URI_KEY: [uri],
            AzureSource.FORMAT_KEY: {
                CSVFormat.IDENTIFIER: FORMAT_TYPE_TO_CONFIG["csv"]
            },
            AzureSource.CREDENTIALS_KEY: CREDENTIALS_DICT,
            "initial_last_modified": time.isoformat(timespec="microseconds"),
        }
    }
    assert input.to_dict() == expected_dict
    assert isinstance(build_input(input.to_dict()), AzureSource)


def test_initial_last_modified_invalid_type():
    uri = "az://path/to/data/data"
    format = "csv"
    time = 123
    with pytest.raises(InputConfigurationError) as e:
        AzureSource(uri, AZURE_CREDENTIALS, format=format, initial_last_modified=time)
    assert e.value.error_code == ErrorCode.ICE6


def test_build_input_wrong_type_raises_error():
    with pytest.raises(InputConfigurationError) as e:
        build_input(42)
    assert e.value.error_code == ErrorCode.ICE11


def test_identifier_string_unchanged():
    uri = "az://path/to/data/data"
    format = "csv"
    input = AzureSource(uri, AZURE_CREDENTIALS, format=format)
    expected_dict = {
        "azure-input": {
            AzureSource.URI_KEY: [uri],
            AzureSource.FORMAT_KEY: {
                CSVFormat.IDENTIFIER: FORMAT_TYPE_TO_CONFIG["csv"]
            },
            AzureSource.CREDENTIALS_KEY: CREDENTIALS_DICT,
            "initial_last_modified": None,
        }
    }
    assert input.to_dict() == expected_dict
    assert isinstance(build_input(input.to_dict()), AzureSource)
