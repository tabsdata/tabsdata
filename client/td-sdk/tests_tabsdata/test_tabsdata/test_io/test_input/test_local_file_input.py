#
# Copyright 2024 Tabs Data Inc.
#

import copy
import datetime

import pytest
from tests_tabsdata.conftest import FORMAT_TYPE_TO_CONFIG

from tabsdata import CSVFormat, NDJSONFormat, ParquetFormat
from tabsdata._io.inputs.file_inputs import LocalFileSource
from tabsdata._io.plugin import SourcePlugin
from tabsdata.exceptions import (
    ErrorCode,
    FormatConfigurationError,
    InputConfigurationError,
)


def test_all_correct_single_parameter():
    path = "/path/to/data/data.csv"
    input = LocalFileSource(path)
    assert input.path == path
    assert isinstance(input.format, CSVFormat)


def test_all_correct_both_ndjson_extensions():
    path = "/path/to/data/data.ndjson"
    input = LocalFileSource(path)
    assert input.path == path
    assert isinstance(input.format, NDJSONFormat)
    input.path = "/path/to/data/data.jsonl"
    assert input.path == "/path/to/data/data.jsonl"
    assert isinstance(input.format, NDJSONFormat)


def test_all_correct_single_parameter_list():
    path = ["/path/to/data/data.csv", "/path/to/data/data2.csv"]
    input = LocalFileSource(path)
    assert input.path == path
    assert isinstance(input.format, CSVFormat)
    assert isinstance(input, LocalFileSource)
    assert isinstance(input, SourcePlugin)
    assert input.__repr__()


def test_all_correct_single_parameter_uri():
    path = "file:///path/to/data/data.csv"
    input = LocalFileSource(path)
    assert input.path == path
    assert isinstance(input.format, CSVFormat)
    assert isinstance(input, LocalFileSource)
    assert isinstance(input, SourcePlugin)
    assert input.__repr__()


def test_list_of_integers_raises_exception():
    uri = [1, 2, "hi"]
    with pytest.raises(InputConfigurationError) as e:
        LocalFileSource(uri)
    assert e.value.error_code == ErrorCode.ICE13


def test_all_correct_implicit_format():
    path = "file://path/to/data/data.csv"
    input = LocalFileSource(path)
    assert input.path == path
    assert isinstance(input.format, CSVFormat)
    assert isinstance(input, LocalFileSource)
    assert isinstance(input, SourcePlugin)
    assert input.__repr__()


def test_all_correct_explicit_format():
    path = "file://path/to/data/data"
    format = "csv"
    input = LocalFileSource(path, format=format)
    assert input.path == path
    assert isinstance(input.format, CSVFormat)
    assert isinstance(input, LocalFileSource)
    assert isinstance(input, SourcePlugin)


def test_wrong_scheme_raises_value_error():
    path = "wrongscheme://path/to/data/data.csv"
    with pytest.raises(InputConfigurationError) as e:
        LocalFileSource(path)
    assert e.value.error_code == ErrorCode.ICE14


def test_path_list():
    path = [
        "file://path/to/data/invoice-headers.csv",
        "file://path/to/data/invoice-items-*.csv",
    ]
    input = LocalFileSource(path)
    assert input.path == path


def test_path_wrong_type_raises_type_error():
    path = 42
    with pytest.raises(InputConfigurationError) as e:
        LocalFileSource(path)
    assert e.value.error_code == ErrorCode.ICE13


def test_format_from_path_list():
    path = [
        "file://path/to/data/invoice-headers.csv",
        "file://path/to/data/invoice-items-*.csv",
    ]
    input = LocalFileSource(path)
    assert isinstance(input.format, CSVFormat)


def test_correct_dict_format():
    path = "file://path/to/data/data"
    format = {CSVFormat.IDENTIFIER: {"separator": ".", "input_has_header": False}}
    expected_format = copy.deepcopy(FORMAT_TYPE_TO_CONFIG["csv"])
    expected_format["separator"] = "."
    expected_format["input_has_header"] = False

    input = LocalFileSource(path, format=format)
    assert isinstance(input.format, CSVFormat)
    assert input.format._to_dict() == {CSVFormat.IDENTIFIER: expected_format}
    assert isinstance(input, LocalFileSource)
    assert isinstance(input, SourcePlugin)


def test_incorrect_file_format_raises_error():
    path = "file://path/to/data/data.wrongformat"
    with pytest.raises(FormatConfigurationError) as e:
        LocalFileSource(path)
    assert e.value.error_code == ErrorCode.FOCE4


def test_missing_file_format_raises_error():
    path = "file://path/to/data/data"
    with pytest.raises(FormatConfigurationError) as e:
        LocalFileSource(path)
    assert e.value.error_code == ErrorCode.FOCE6


def test_wrong_implicit_format_raises_value_error():
    path = "file://path/to/data/data.wrong"
    with pytest.raises(FormatConfigurationError) as e:
        LocalFileSource(path)
    assert e.value.error_code == ErrorCode.FOCE4


def test_wrong_explicit_format_raises_value_error():
    path = "file://path/to/data/data.csv"
    format = "wrong"
    with pytest.raises(FormatConfigurationError) as e:
        LocalFileSource(path, format=format)
    assert e.value.error_code == ErrorCode.FOCE4


def test_empty_format():
    path = "file://path/to/data/data"
    format = ""
    with pytest.raises(FormatConfigurationError) as e:
        LocalFileSource(path, format=format)
    assert e.value.error_code == ErrorCode.FOCE4


def test_wrong_type_format_raises_type_error():
    path = "file://path/to/data/data.csv"
    format = 42
    with pytest.raises(FormatConfigurationError) as e:
        LocalFileSource(path, format=format)
    assert e.value.error_code == ErrorCode.FOCE5


def test_initial_last_modified_none():
    path = "file://path/to/data/data"
    format = "csv"
    input = LocalFileSource(path, format=format, initial_last_modified=None)
    assert input.initial_last_modified is None
    assert isinstance(input, LocalFileSource)
    assert isinstance(input, SourcePlugin)


def test_initial_last_modified_valid_string():
    path = "file://path/to/data/data"
    format = "csv"
    time = "2024-09-05T01:01:00.01Z"
    input = LocalFileSource(path, format=format, initial_last_modified=time)
    assert input.initial_last_modified == datetime.datetime.fromisoformat(
        time
    ).isoformat(timespec="microseconds")
    assert isinstance(input, LocalFileSource)
    assert isinstance(input, SourcePlugin)


def test_initial_last_modified_valid_string_plus_timezone():
    path = "file://path/to/data/data"
    format = "csv"
    time = "2024-09-05T01:01:00.01+12:37"
    input = LocalFileSource(path, format=format, initial_last_modified=time)
    assert input.initial_last_modified == datetime.datetime.fromisoformat(
        time
    ).isoformat(timespec="microseconds")
    assert isinstance(input, LocalFileSource)
    assert isinstance(input, SourcePlugin)


def test_different_input_not_eq():
    path = "file://path/to/data/data"
    format = "csv"
    time = "2024-09-05T01:01:00.01Z"
    input = LocalFileSource(path, format=format, initial_last_modified=time)
    input2 = LocalFileSource(
        path, format=format, initial_last_modified="2024-09-05T01:01:00.02Z"
    )
    assert input != input2


def test_input_not_eq_dict():
    path = "file://path/to/data/data"
    format = "csv"
    time = "2024-09-05T01:01:00.01Z"
    input = LocalFileSource(path, format=format, initial_last_modified=time)
    assert input._to_dict() != input


def test_initial_last_modified_invalid_string():
    path = "file://path/to/data/data"
    format = "csv"
    time = "wrong_time"
    with pytest.raises(InputConfigurationError) as e:
        LocalFileSource(path, format=format, initial_last_modified=time)
    assert e.value.error_code == ErrorCode.ICE5


def test_initial_last_modified_invalid_string_no_timezone():
    path = "file://path/to/data/data"
    format = "csv"
    time = "2024-09-05T01:01:00.01"
    with pytest.raises(InputConfigurationError) as e:
        LocalFileSource(path, format=format, initial_last_modified=time)
    assert e.value.error_code == ErrorCode.ICE41


def test_initial_last_modified_valid_datetime():
    path = "file://path/to/data/data"
    format = "csv"
    time = datetime.datetime(2024, 9, 5, 1, 1, 0, 10000, tzinfo=datetime.timezone.utc)
    input = LocalFileSource(path, format=format, initial_last_modified=time)
    assert input.initial_last_modified == time.isoformat(timespec="microseconds")
    assert isinstance(input, LocalFileSource)
    assert isinstance(input, SourcePlugin)


def test_initial_last_modified_invalid_datetime_no_timezone():
    path = "file://path/to/data/data"
    format = "csv"
    time = datetime.datetime(2024, 9, 5, 1, 1, 0, 10000)
    with pytest.raises(InputConfigurationError) as e:
        LocalFileSource(path, format=format, initial_last_modified=time)
    assert e.value.error_code == ErrorCode.ICE41


def test_initial_last_modified_invalid_type():
    path = "file://path/to/data/data"
    format = "csv"
    time = 123
    with pytest.raises(InputConfigurationError) as e:
        LocalFileSource(path, format=format, initial_last_modified=time)
    assert e.value.error_code == ErrorCode.ICE6


def test_update_path():
    path = "file://path/to/data/data.csv"
    input = LocalFileSource(path)
    assert input.path == path
    assert isinstance(input.format, CSVFormat)
    input.path = "file://path/to/data/new_data.csv"
    assert input.path == "file://path/to/data/new_data.csv"
    assert isinstance(input.format, CSVFormat)


def test_path_list_update_to_string():
    path = [
        "/path/to/data/invoice-headers.csv",
        "/path/to/data/invoice-items-*.csv",
    ]
    input = LocalFileSource(path)
    assert input.path == path
    assert input._path_list == path
    assert isinstance(input.format, CSVFormat)
    input = LocalFileSource("/path/to/data/invoice-headers.csv")
    assert input._path_list == ["/path/to/data/invoice-headers.csv"]
    assert isinstance(input.format, CSVFormat)


def test_update_path_and_derived():
    path = ["/path/to/data/data.csv", "/path/to/data/data2.csv"]
    input = LocalFileSource(path)
    assert input.path == path
    assert input._path_list == path
    assert isinstance(input.format, CSVFormat)
    path2 = ["/path/to/data/data.csv", "/path/to/data/data3.csv"]
    input.path = path2
    assert input.path == path2
    assert input._path_list == path2
    assert isinstance(input.format, CSVFormat)


def test_update_path_implicit_format():
    path = ["/path/to/data/data.csv", "/path/to/data/data2.csv"]
    input = LocalFileSource(path)
    assert input.path == path
    assert isinstance(input.format, CSVFormat)
    path2 = ["/path/to/data/data.parquet", "/path/to/data/data3.parquet"]
    input.path = path2
    assert input.path == path2
    assert isinstance(input.format, ParquetFormat)


def test_update_path_explicit_format():
    path = ["/path/to/data/data.csv", "/path/to/data/data2.csv"]
    input = LocalFileSource(path, format="csv")
    assert input.path == path
    assert isinstance(input.format, CSVFormat)
    path2 = ["/path/to/data/data.parquet", "/path/to/data/data3.parquet"]
    input.path = path2
    assert input.path == path2
    assert isinstance(input.format, CSVFormat)


def test_update_path_mixed_format():
    path = ["/path/to/data/data.csv", "/path/to/data/data2.csv"]
    input = LocalFileSource(path)
    assert input.path == path
    assert isinstance(input.format, CSVFormat)
    assert input._format is None
    path2 = ["/path/to/data/data.parquet", "/path/to/data/data3.parquet"]
    input.path = path2
    assert input.path == path2
    assert isinstance(input.format, ParquetFormat)
    assert input._format is None
    input.format = CSVFormat()
    assert input.format == CSVFormat()
    assert input._format == CSVFormat()
    input.path = path2
    assert input.path == path2
    assert isinstance(input.format, CSVFormat)
    assert input._format == CSVFormat()
    input.format = ParquetFormat()
    assert isinstance(input.format, ParquetFormat)
    assert input._format == ParquetFormat()


def test_update_format():
    path = "file://path/to/data/data.csv"
    format = CSVFormat()
    input = LocalFileSource(path, format=format)
    assert input.format == format
    input.format = CSVFormat(separator=";")
    assert input.format == CSVFormat(separator=";")
