#
# Copyright 2024 Tabs Data Inc.
#

import copy

import pytest
from tests_tabsdata.conftest import FORMAT_TYPE_TO_CONFIG

from tabsdata import CSVFormat, NDJSONFormat, ParquetFormat
from tabsdata._io.outputs.file_outputs import (
    FRAGMENT_INDEX_PLACEHOLDER,
    LocalFileDestination,
)
from tabsdata._io.plugin import DestinationPlugin
from tabsdata.exceptions import (
    ErrorCode,
    FormatConfigurationError,
    OutputConfigurationError,
)


def test_all_correct_single_parameter():
    path = "/path/to/data/data.csv"
    output = LocalFileDestination(path)
    assert output.path == path
    assert isinstance(output.format, CSVFormat)


def test_all_correct_both_ndjson_extensions():
    path = "/path/to/data/data.ndjson"
    output = LocalFileDestination(path)
    assert output.path == path
    assert isinstance(output.format, NDJSONFormat)
    output.path = "/path/to/data/data.jsonl"
    assert output.path == "/path/to/data/data.jsonl"
    assert isinstance(output.format, NDJSONFormat)


def test_all_correct_single_parameter_list():
    path = ["/path/to/data/data.csv", "/path/to/data/data2.csv"]
    output = LocalFileDestination(path)
    assert output.path == path
    assert isinstance(output.format, CSVFormat)
    assert isinstance(output, LocalFileDestination)
    assert isinstance(output, DestinationPlugin)
    assert output.__repr__()


def test_all_correct_single_parameter_uri():
    path = "file:///path/to/data/data.csv"
    output = LocalFileDestination(path)
    assert output.path == path
    assert isinstance(output.format, CSVFormat)
    assert isinstance(output, LocalFileDestination)
    assert isinstance(output, DestinationPlugin)
    assert output.__repr__()


def test_list_of_integers_raises_exception():
    uri = [1, 2, "hi"]
    with pytest.raises(OutputConfigurationError) as e:
        LocalFileDestination(uri)
    assert e.value.error_code == ErrorCode.OCE11


def test_all_correct_implicit_format():
    path = "file://path/to/data/data.csv"
    output = LocalFileDestination(path)
    assert output.path == path
    assert isinstance(output.format, CSVFormat)
    assert isinstance(output, LocalFileDestination)
    assert isinstance(output, DestinationPlugin)
    assert output.__repr__()


def test_all_correct_explicit_format():
    path = "file://path/to/data/data"
    format = "csv"
    output = LocalFileDestination(path, format=format)
    assert output.path == path
    assert isinstance(output.format, CSVFormat)
    assert isinstance(output, LocalFileDestination)
    assert isinstance(output, DestinationPlugin)


def test_wrong_scheme_raises_value_error():
    path = "wrongscheme://path/to/data/data.csv"
    with pytest.raises(OutputConfigurationError) as e:
        LocalFileDestination(path)
    assert e.value.error_code == ErrorCode.OCE12


def test_path_list():
    path = [
        "file://path/to/data/invoice-headers.csv",
        "file://path/to/data/invoice-items-*.csv",
    ]
    output = LocalFileDestination(path)
    assert output.path == path


def test_path_wrong_type_raises_type_error():
    path = 42
    with pytest.raises(OutputConfigurationError) as e:
        LocalFileDestination(path)
    assert e.value.error_code == ErrorCode.OCE11


def test_format_from_path_list():
    path = [
        "file://path/to/data/invoice-headers.csv",
        "file://path/to/data/invoice-items-*.csv",
    ]
    output = LocalFileDestination(path)
    assert isinstance(output.format, CSVFormat)


def test_correct_dict_format():
    path = "file://path/to/data/data"
    format = {CSVFormat.IDENTIFIER: {"separator": ".", "input_has_header": False}}
    expected_format = copy.deepcopy(FORMAT_TYPE_TO_CONFIG["csv"])
    expected_format["separator"] = "."
    expected_format["input_has_header"] = False

    output = LocalFileDestination(path, format=format)
    assert isinstance(output.format, CSVFormat)
    assert output.format._to_dict() == {CSVFormat.IDENTIFIER: expected_format}
    assert isinstance(output, LocalFileDestination)
    assert isinstance(output, DestinationPlugin)


def test_incorrect_file_format_raises_error():
    path = "file://path/to/data/data.wrongformat"
    with pytest.raises(FormatConfigurationError) as e:
        LocalFileDestination(path)
    assert e.value.error_code == ErrorCode.FOCE4


def test_missing_file_format_raises_error():
    path = "file://path/to/data/data"
    with pytest.raises(FormatConfigurationError) as e:
        LocalFileDestination(path)
    assert e.value.error_code == ErrorCode.FOCE6


def test_wrong_implicit_format_raises_value_error():
    path = "file://path/to/data/data.wrong"
    with pytest.raises(FormatConfigurationError) as e:
        LocalFileDestination(path)
    assert e.value.error_code == ErrorCode.FOCE4


def test_wrong_explicit_format_raises_value_error():
    path = "file://path/to/data/data.csv"
    format = "wrong"
    with pytest.raises(FormatConfigurationError) as e:
        LocalFileDestination(path, format=format)
    assert e.value.error_code == ErrorCode.FOCE4


def test_empty_format():
    path = "file://path/to/data/data"
    format = ""
    with pytest.raises(FormatConfigurationError) as e:
        LocalFileDestination(path, format=format)
    assert e.value.error_code == ErrorCode.FOCE4


def test_wrong_type_format_raises_type_error():
    path = "file://path/to/data/data.csv"
    format = 42
    with pytest.raises(FormatConfigurationError) as e:
        LocalFileDestination(path, format=format)
    assert e.value.error_code == ErrorCode.FOCE5


def test_update_path():
    path = "file://path/to/data/data.csv"
    output = LocalFileDestination(path)
    assert output.path == path
    assert isinstance(output.format, CSVFormat)
    output.path = "file://path/to/data/new_data.csv"
    assert output.path == "file://path/to/data/new_data.csv"
    assert isinstance(output.format, CSVFormat)


def test_path_list_update_to_string():
    path = [
        "/path/to/data/invoice-headers.csv",
        "/path/to/data/invoice-items-*.csv",
    ]
    output = LocalFileDestination(path)
    assert output.path == path
    assert output._path_list == path
    assert isinstance(output.format, CSVFormat)
    output = LocalFileDestination("/path/to/data/invoice-headers.csv")
    assert output._path_list == ["/path/to/data/invoice-headers.csv"]
    assert isinstance(output.format, CSVFormat)


def test_update_path_and_derived():
    path = ["/path/to/data/data.csv", "/path/to/data/data2.csv"]
    output = LocalFileDestination(path)
    assert output.path == path
    assert output._path_list == path
    assert isinstance(output.format, CSVFormat)
    path2 = ["/path/to/data/data.csv", "/path/to/data/data3.csv"]
    output.path = path2
    assert output.path == path2
    assert output._path_list == path2
    assert isinstance(output.format, CSVFormat)


def test_update_path_implicit_format():
    path = ["/path/to/data/data.csv", "/path/to/data/data2.csv"]
    output = LocalFileDestination(path)
    assert output.path == path
    assert isinstance(output.format, CSVFormat)
    path2 = ["/path/to/data/data.parquet", "/path/to/data/data3.parquet"]
    output.path = path2
    assert output.path == path2
    assert isinstance(output.format, ParquetFormat)


def test_update_path_explicit_format():
    path = ["/path/to/data/data.csv", "/path/to/data/data2.csv"]
    output = LocalFileDestination(path, format="csv")
    assert output.path == path
    assert isinstance(output.format, CSVFormat)
    path2 = ["/path/to/data/data.parquet", "/path/to/data/data3.parquet"]
    output.path = path2
    assert output.path == path2
    assert isinstance(output.format, CSVFormat)


def test_update_path_mixed_format():
    path = ["/path/to/data/data.csv", "/path/to/data/data2.csv"]
    output = LocalFileDestination(path)
    assert output.path == path
    assert isinstance(output.format, CSVFormat)
    assert output._format is None
    path2 = ["/path/to/data/data.parquet", "/path/to/data/data3.parquet"]
    output.path = path2
    assert output.path == path2
    assert isinstance(output.format, ParquetFormat)
    assert output._format is None
    output.format = CSVFormat()
    assert output.format == CSVFormat()
    assert output._format == CSVFormat()
    output.path = path2
    assert output.path == path2
    assert isinstance(output.format, CSVFormat)
    assert output._format == CSVFormat()
    output.format = ParquetFormat()
    assert isinstance(output.format, ParquetFormat)
    assert output._format == ParquetFormat()


def test_update_format():
    path = "file://path/to/data/data.csv"
    format = CSVFormat()
    output = LocalFileDestination(path, format=format)
    assert output.format == format
    output.format = CSVFormat(separator=";")
    assert output.format == CSVFormat(separator=";")


def test_allow_fragments():
    path = f"file://path/to/data/data_{FRAGMENT_INDEX_PLACEHOLDER}.csv"
    output = LocalFileDestination(path)
    assert output.path == path
    assert output.allow_fragments
