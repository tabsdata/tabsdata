#
# Copyright 2024 Tabs Data Inc.
#

import copy
import datetime
from urllib.parse import urlparse

import pytest

from tabsdata import CSVFormat, ParquetFormat
from tabsdata._credentials import S3AccessKeyCredentials, UserPasswordCredentials
from tabsdata._io.inputs.file_inputs import S3Source
from tabsdata._io.plugin import SourcePlugin
from tabsdata.exceptions import (
    ErrorCode,
    FormatConfigurationError,
    SourceConfigurationError,
)
from tests_tabsdata.conftest import FORMAT_TYPE_TO_CONFIG

# noinspection PyUnresolvedReferences
from . import pytestmark  # noqa: F401

TEST_ACCESS_KEY_ID = "test_access_key_id"
TEST_SECRET_ACCESS_KEY = "test_secret_access_key"
S3_CREDENTIALS = S3AccessKeyCredentials(
    aws_access_key_id=TEST_ACCESS_KEY_ID,
    aws_secret_access_key=TEST_SECRET_ACCESS_KEY,
)


def test_all_correct_implicit_format():
    uri = "s3://path/to/data/data.csv"
    input = S3Source(uri, S3_CREDENTIALS)
    assert input.uri == uri
    assert isinstance(input.format, CSVFormat)
    assert isinstance(input, S3Source)
    assert isinstance(input, SourcePlugin)
    assert input.credentials == S3_CREDENTIALS
    assert input.__repr__()


def test_all_correct_uri_list():
    uri = ["s3://path/to/data/data.csv", "s3://path/to/data/data2.csv"]
    input = S3Source(uri, S3_CREDENTIALS)
    assert input.uri == uri
    assert isinstance(input.format, CSVFormat)
    assert isinstance(input, S3Source)
    assert isinstance(input, SourcePlugin)
    assert input.credentials == S3_CREDENTIALS
    assert input.__repr__()


def test_uri_list_update_to_string():
    uri = [
        "s3://path/to/data/invoice-headers.csv",
        "s3://path/to/data/invoice-items-*.csv",
    ]
    input = S3Source(uri, S3_CREDENTIALS)
    assert input.uri == uri
    assert input._uri_list == uri
    input = S3Source("s3://path/to/data/invoice-headers.csv", S3_CREDENTIALS)
    assert input._uri_list == ["s3://path/to/data/invoice-headers.csv"]


def test_parsed_uri_list():
    uri = [
        "s3://path/to/data/invoice-headers.csv",
        "s3://path/to/data/invoice-items-*.csv",
    ]
    input = S3Source(uri, S3_CREDENTIALS)
    assert input.uri == uri
    assert input._uri_list == uri
    assert input._parsed_uri_list == [urlparse(uri[0]), urlparse(uri[1])]
    uri = "s3://path/to/data/invoice-headers.csv"
    input = S3Source(uri, S3_CREDENTIALS)
    assert input.uri == uri
    assert input._uri_list == [uri]
    assert input._parsed_uri_list == [urlparse(uri)]


def test_update_uri():
    uri = ["s3://path/to/data/data.csv", "s3://path/to/data/data2.csv"]
    input = S3Source(uri, S3_CREDENTIALS)
    assert input.uri == uri
    assert input._uri_list == uri
    assert input._parsed_uri_list == [urlparse(uri[0]), urlparse(uri[1])]
    uri2 = ["s3://path/to/data/data.csv", "s3://path/to/data/data3.csv"]
    input.uri = uri2
    assert input.uri == uri2
    assert input._uri_list == uri2
    assert input._parsed_uri_list == [urlparse(uri2[0]), urlparse(uri2[1])]


def test_update_uri_implicit_format():
    uri = ["s3://path/to/data/data.csv", "s3://path/to/data/data2.csv"]
    input = S3Source(uri, S3_CREDENTIALS)
    assert input.uri == uri
    assert isinstance(input.format, CSVFormat)
    uri2 = ["s3://path/to/data/data.parquet", "s3://path/to/data/data3.parquet"]
    input.uri = uri2
    assert input.uri == uri2
    assert isinstance(input.format, ParquetFormat)


def test_update_uri_explicit_format():
    uri = ["s3://path/to/data/data.csv", "s3://path/to/data/data2.csv"]
    input = S3Source(uri, S3_CREDENTIALS, format="csv")
    assert input.uri == uri
    assert isinstance(input.format, CSVFormat)
    uri2 = ["s3://path/to/data/data.parquet", "s3://path/to/data/data3.parquet"]
    input.uri = uri2
    assert input.uri == uri2
    assert isinstance(input.format, CSVFormat)


def test_uri_path_mixed_format():
    uri = ["s3://path/to/data/data.csv", "s3://path/to/data/data2.csv"]
    input = S3Source(uri, S3_CREDENTIALS)
    assert input.uri == uri
    assert isinstance(input.format, CSVFormat)
    assert input._format is None
    uri2 = ["s3://path/to/data/data.parquet", "s3://path/to/data/data3.parquet"]
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
    uri = ["s3://path/to/data/data.csv", "s3://path/to/data/data2.csv"]
    input = S3Source(uri, S3_CREDENTIALS)
    assert input.format == CSVFormat()
    format = CSVFormat(separator=";", input_has_header=False)
    input.format = format
    assert input.format == format


def test_update_credentials():
    uri = ["s3://path/to/data/data.csv", "s3://path/to/data/data2.csv"]
    input = S3Source(uri, S3_CREDENTIALS)
    assert input.credentials == S3_CREDENTIALS
    credentials = S3AccessKeyCredentials(
        aws_access_key_id="new_access_key_id",
        aws_secret_access_key="new_secret_access_key",
    )
    input.credentials = credentials
    assert input.credentials == credentials


def test_wrong_type_credentials_raises_error():
    uri = ["s3://path/to/data/data.csv", "s3://path/to/data/data2.csv"]
    credentials = UserPasswordCredentials("username", "password")
    with pytest.raises(SourceConfigurationError) as e:
        S3Source(uri, credentials)
    assert e.value.error_code == ErrorCode.SOCE20


def test_different_input_not_eq():
    uri = ["s3://path/to/data/data.csv", "s3://path/to/data/data2.csv"]
    input = S3Source(uri, S3_CREDENTIALS)
    uri2 = ["s3://path/to/data/data.csv", "s3://path/to/data/data3.csv"]
    input2 = S3Source(uri2, S3_CREDENTIALS)
    assert input != input2


def test_input_not_eq_dict():
    uri = ["s3://path/to/data/data.csv", "s3://path/to/data/data2.csv"]
    input = S3Source(uri, S3_CREDENTIALS)
    assert input._to_dict() != input


def test_all_correct_explicit_format():
    uri = "s3://path/to/data/data"
    format = "csv"
    input = S3Source(uri, S3_CREDENTIALS, format=format)
    assert input.uri == uri
    assert isinstance(input.format, CSVFormat)
    assert isinstance(input, S3Source)
    assert isinstance(input, SourcePlugin)


def test_wrong_scheme_raises_value_error():
    uri = "wrongscheme://path/to/data/data.csv"
    with pytest.raises(SourceConfigurationError) as e:
        S3Source(uri, S3_CREDENTIALS)
    assert e.value.error_code == ErrorCode.SOCE17


def test_empty_scheme_raises_value_error():
    uri = "path/to/data/data.csv"
    with pytest.raises(SourceConfigurationError) as e:
        S3Source(uri, S3_CREDENTIALS)
    assert e.value.error_code == ErrorCode.SOCE17


def test_list_of_integers_raises_exception():
    uri = [1, 2, "hi"]
    with pytest.raises(SourceConfigurationError) as e:
        S3Source(uri, S3_CREDENTIALS)
    assert e.value.error_code == ErrorCode.SOCE16


def test_uri_list():
    uri = [
        "s3://path/to/data/invoice-headers.csv",
        "s3://path/to/data/invoice-items-*.csv",
    ]
    input = S3Source(uri, S3_CREDENTIALS)
    assert input.uri == uri


def test_uri_wrong_type_raises_type_error():
    uri = 42
    with pytest.raises(SourceConfigurationError) as e:
        S3Source(uri, S3_CREDENTIALS)
    assert e.value.error_code == ErrorCode.SOCE16


def test_format_from_uri_list():
    uri = [
        "s3://path/to/data/invoice-headers.csv",
        "s3://path/to/data/invoice-items-*.csv",
    ]
    input = S3Source(uri, S3_CREDENTIALS)
    assert isinstance(input.format, CSVFormat)


def test_no_implicit_format_raises_value_error():
    uri = "s3://path/to/data/data"
    with pytest.raises(FormatConfigurationError) as e:
        S3Source(uri, S3_CREDENTIALS)
    assert e.value.error_code == ErrorCode.FOCE6


def test_correct_format_object():
    uri = "s3://path/to/data/data"
    format = CSVFormat(separator=".", input_has_header=False)
    expected_format = copy.deepcopy(FORMAT_TYPE_TO_CONFIG["csv"])
    expected_format["separator"] = "."
    expected_format["input_has_header"] = False

    input = S3Source(uri, S3_CREDENTIALS, format=format)
    assert input.format._to_dict()[CSVFormat.IDENTIFIER] == expected_format
    assert isinstance(input, S3Source)
    assert isinstance(input, SourcePlugin)


def test_incorrect_data_format_raises_value_error():
    uri = "s3://path/to/data/data.wrongformat"
    with pytest.raises(FormatConfigurationError) as e:
        S3Source(uri, S3_CREDENTIALS)
    assert e.value.error_code == ErrorCode.FOCE4


def test_wrong_implicit_format_raises_value_error():
    uri = "s3://path/to/data/data.wrong"
    with pytest.raises(FormatConfigurationError) as e:
        S3Source(uri, S3_CREDENTIALS)
    assert e.value.error_code == ErrorCode.FOCE4


def test_wrong_explicit_format_raises_value_error():
    uri = "s3://path/to/data/data.csv"
    format = "wrong"
    with pytest.raises(FormatConfigurationError) as e:
        S3Source(uri, S3_CREDENTIALS, format=format)
    assert e.value.error_code == ErrorCode.FOCE4


def test_empty_format():
    uri = "s3://path/to/data/data"
    format = ""
    with pytest.raises(FormatConfigurationError) as e:
        S3Source(uri, S3_CREDENTIALS, format=format)
    assert e.value.error_code == ErrorCode.FOCE4


def test_wrong_type_format_raises_type_error():
    uri = "s3://path/to/data/data.csv"
    format = 42
    with pytest.raises(FormatConfigurationError) as e:
        S3Source(uri, S3_CREDENTIALS, format=format)
    assert e.value.error_code == ErrorCode.FOCE5


def test_initial_last_modified_none():
    uri = "s3://path/to/data/data"
    format = "csv"
    input = S3Source(uri, S3_CREDENTIALS, format=format, initial_last_modified=None)
    assert input.initial_last_modified is None
    assert isinstance(input, S3Source)
    assert isinstance(input, SourcePlugin)


def test_initial_last_modified_valid_string():
    uri = "s3://path/to/data/data"
    format = "csv"
    time = "2024-09-05T01:01:00.01Z"
    input = S3Source(uri, S3_CREDENTIALS, format=format, initial_last_modified=time)
    assert input.initial_last_modified == datetime.datetime.fromisoformat(
        time
    ).isoformat(timespec="microseconds")
    assert isinstance(input, S3Source)
    assert isinstance(input, SourcePlugin)


def test_initial_last_modified_valid_string_hour_timezone():
    uri = "s3://path/to/data/data"
    format = "csv"
    time = "2024-09-05T01:01:00.01+02:00"
    input = S3Source(uri, S3_CREDENTIALS, format=format, initial_last_modified=time)
    assert input.initial_last_modified == datetime.datetime.fromisoformat(
        time
    ).isoformat(timespec="microseconds")
    assert isinstance(input, S3Source)
    assert isinstance(input, SourcePlugin)


def test_initial_last_modified_invalid_string():
    uri = "s3://path/to/data/data"
    format = "csv"
    time = "wrong_time"
    with pytest.raises(SourceConfigurationError) as e:
        S3Source(uri, S3_CREDENTIALS, format=format, initial_last_modified=time)
    assert e.value.error_code == ErrorCode.SOCE5


def test_initial_last_modified_invalid_string_no_timezone():
    uri = "s3://path/to/data/data"
    format = "csv"
    time = "2024-09-05T01:01:00.01"
    with pytest.raises(SourceConfigurationError) as e:
        S3Source(uri, S3_CREDENTIALS, format=format, initial_last_modified=time)
    assert e.value.error_code == ErrorCode.SOCE41


def test_initial_last_modified_valid_datetime():
    uri = "s3://path/to/data/data"
    format = "csv"
    time = datetime.datetime(2024, 9, 5, 1, 1, 0, 10000, tzinfo=datetime.timezone.utc)
    input = S3Source(uri, S3_CREDENTIALS, format=format, initial_last_modified=time)
    assert input.initial_last_modified == time.isoformat(timespec="microseconds")
    assert isinstance(input, S3Source)
    assert isinstance(input, SourcePlugin)


def test_initial_last_modified_invalid_datetime():
    uri = "s3://path/to/data/data"
    format = "csv"
    time = datetime.datetime(2024, 9, 5, 1, 1, 0, 10000)
    with pytest.raises(SourceConfigurationError) as e:
        S3Source(uri, S3_CREDENTIALS, format=format, initial_last_modified=time)
    assert e.value.error_code == ErrorCode.SOCE41


def test_initial_last_modified_invalid_type():
    uri = "s3://path/to/data/data"
    format = "csv"
    time = 123
    with pytest.raises(SourceConfigurationError) as e:
        S3Source(uri, S3_CREDENTIALS, format=format, initial_last_modified=time)
    assert e.value.error_code == ErrorCode.SOCE6


def test_region():
    uri = "s3://path/to/data/data"
    format = "csv"
    region = "us-west-2"
    input = S3Source(uri, S3_CREDENTIALS, format=format, region=region)
    assert input.region == region
    assert isinstance(input, S3Source)
    assert isinstance(input, SourcePlugin)


def test_region_wrong_type_raises_error():
    uri = "s3://path/to/data/data.csv"
    region = 42
    with pytest.raises(SourceConfigurationError) as e:
        S3Source(uri, S3_CREDENTIALS, region=region)
    assert e.value.error_code == ErrorCode.SOCE26
