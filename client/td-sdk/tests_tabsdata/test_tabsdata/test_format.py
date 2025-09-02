#
# Copyright 2024 Tabs Data Inc.
#

import copy

import pytest

from tabsdata import AvroFormat, CSVFormat, LogFormat, NDJSONFormat, ParquetFormat
from tabsdata._format import build_file_format, get_implicit_format_from_list
from tabsdata.exceptions import ErrorCode, FormatConfigurationError
from tests_tabsdata.conftest import FORMAT_TYPE_TO_CONFIG


def test_csv_format_to_dict():
    csv_format = CSVFormat()
    assert csv_format._to_dict() == {CSVFormat.IDENTIFIER: FORMAT_TYPE_TO_CONFIG["csv"]}
    built_csv_format = build_file_format(csv_format._to_dict())
    assert isinstance(built_csv_format, CSVFormat)
    assert built_csv_format == csv_format


def test_csv_format_not_equal_to_dict():
    csv_format = CSVFormat()
    assert csv_format != csv_format._to_dict()


def test_csv_format_all_parameters():
    csv_format = CSVFormat(
        separator=";",
        quote_char="'",
        eol_char="\r\n",
        input_encoding="latin1",
        input_null_values=["NULL"],
        input_missing_is_null=True,
        input_truncate_ragged_lines=True,
        input_comment_prefix="#",
        input_try_parse_dates=True,
        input_decimal_comma=True,
        input_has_header=False,
        input_skip_rows=1,
        input_skip_rows_after_header=2,
        input_raise_if_empty=False,
        input_ignore_errors=True,
    )
    assert csv_format.separator == ";"
    assert csv_format.quote_char == "'"
    assert csv_format.eol_char == "\r\n"
    assert csv_format.input_encoding == "latin1"
    assert csv_format.input_null_values == ["NULL"]
    assert csv_format.input_missing_is_null is True
    assert csv_format.input_truncate_ragged_lines is True
    assert csv_format.input_comment_prefix == "#"
    assert csv_format.input_try_parse_dates is True
    assert csv_format.input_decimal_comma is True
    assert csv_format.input_has_header is False
    assert csv_format.input_skip_rows == 1
    assert csv_format.input_skip_rows_after_header == 2
    assert csv_format.input_raise_if_empty is False
    assert csv_format.input_ignore_errors is True
    assert csv_format.__repr__()
    assert build_file_format(csv_format._to_dict()) == csv_format


def test_csv_format_separator():
    csv_format = CSVFormat(input_skip_rows_after_header=3)
    assert csv_format.input_skip_rows_after_header == 3
    expected_dict = copy.deepcopy(FORMAT_TYPE_TO_CONFIG["csv"])
    expected_dict["input_skip_rows_after_header"] = 3
    assert csv_format._to_dict() == {
        CSVFormat.IDENTIFIER: expected_dict,
    }
    new_csv_format = build_file_format(csv_format._to_dict())
    assert isinstance(new_csv_format, CSVFormat)
    assert new_csv_format.input_skip_rows_after_header == 3


def test_csv_format_wrong_type_parameter_raises_exception():
    with pytest.raises(FormatConfigurationError) as e:
        CSVFormat(input_skip_rows_after_header=False)
    assert e.value.error_code == ErrorCode.FOCE3


def test_csv_format_wrong_type_string_parameter_raises_exception():
    with pytest.raises(FormatConfigurationError) as e:
        CSVFormat(input_encoding=42)
    assert e.value.error_code == ErrorCode.FOCE3


def test_csv_format_non_existent_parameter_raises_exception():
    with pytest.raises(TypeError):
        CSVFormat(non_existent_parameter=3)


def test_csv_format_wrong_identifier_raises_error():
    with pytest.raises(FormatConfigurationError) as e:
        build_file_format({"wrong_identifier": FORMAT_TYPE_TO_CONFIG["csv"]})
    assert e.value.error_code == ErrorCode.FOCE1


def test_csv_format_multiple_identifiers_raises_error():
    with pytest.raises(FormatConfigurationError) as e:
        build_file_format(
            {
                CSVFormat.IDENTIFIER: FORMAT_TYPE_TO_CONFIG["csv"],
                ParquetFormat.IDENTIFIER: FORMAT_TYPE_TO_CONFIG["parquet"],
            }
        )
    assert e.value.error_code == ErrorCode.FOCE1


def test_csv_format_wrong_identifier_value_raises_error():
    with pytest.raises(FormatConfigurationError) as e:
        build_file_format({CSVFormat.IDENTIFIER: 42})
    assert e.value.error_code == ErrorCode.FOCE2


def test_parquet_format_to_dict():
    parquet_format = ParquetFormat()
    assert parquet_format._to_dict() == {
        ParquetFormat.IDENTIFIER: FORMAT_TYPE_TO_CONFIG["parquet"]
    }
    assert isinstance(build_file_format(parquet_format._to_dict()), ParquetFormat)
    assert parquet_format.__repr__()


def test_parquet_format_all_parameters():
    parquet_format = ParquetFormat()
    assert parquet_format.__repr__()
    assert build_file_format(parquet_format._to_dict()) == parquet_format


def test_avro_format_to_dict():
    avro_format = AvroFormat()
    assert avro_format._to_dict() == {
        AvroFormat.IDENTIFIER: FORMAT_TYPE_TO_CONFIG["avro"]
    }
    assert isinstance(build_file_format(avro_format._to_dict()), AvroFormat)
    assert avro_format.__repr__()


def test_avro_format_all_parameters():
    avro_format = AvroFormat()
    assert avro_format.__repr__()
    assert build_file_format(avro_format._to_dict()) == avro_format


def test_ndjson_format_to_dict():
    json_format = NDJSONFormat()
    assert json_format._to_dict() == {
        NDJSONFormat.IDENTIFIER: FORMAT_TYPE_TO_CONFIG["json"]
    }
    assert isinstance(build_file_format(json_format._to_dict()), NDJSONFormat)
    assert json_format.__repr__()


def test_ndjson_format_all_parameters():
    json_format = NDJSONFormat()
    assert json_format.__repr__()
    assert build_file_format(json_format._to_dict()) == json_format


def test_ndjson_file_extensions():
    assert build_file_format("ndjson") == NDJSONFormat()
    assert build_file_format("jsonl") == NDJSONFormat()


def test_log_format_to_dict():
    log_format = LogFormat()
    assert log_format._to_dict() == {LogFormat.IDENTIFIER: FORMAT_TYPE_TO_CONFIG["log"]}
    assert isinstance(build_file_format(log_format._to_dict()), LogFormat)
    assert log_format.__repr__()


def test_log_format_all_parameters():
    log_format = LogFormat()
    assert log_format.__repr__()
    assert build_file_format(log_format._to_dict()) == log_format


def test_get_implicit_format_from_list_single_csv():
    formats = ["file:///path/to/file.csv"]
    implicit_format = get_implicit_format_from_list(formats)
    assert implicit_format == "csv"


def test_get_implicit_format_from_list_single_parquet():
    formats = ["file:///path/to/file.parquet"]
    implicit_format = get_implicit_format_from_list(formats)
    assert implicit_format == "parquet"


def test_get_implicit_format_from_list_single_log():
    formats = ["file:///path/to/file.log"]
    implicit_format = get_implicit_format_from_list(formats)
    assert implicit_format == "log"


def test_get_implicit_format_from_list_empty_list():
    formats = []
    implicit_format = get_implicit_format_from_list(formats)
    assert implicit_format is None


def test_get_implicit_format_from_list_invalid_format():
    formats = ["file:///path/to/file.invalid"]
    assert get_implicit_format_from_list(formats) == "invalid"
