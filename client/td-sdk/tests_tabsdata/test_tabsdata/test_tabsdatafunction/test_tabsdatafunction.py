#
# Copyright 2024 Tabs Data Inc.
#

import os

import pytest

from tabsdata._io.inputs.file_inputs import LocalFileSource
from tabsdata._io.output import MySQLDestination, build_output
from tabsdata._tabsdatafunction import TabsdataFunction
from tabsdata.exceptions import (
    ErrorCode,
    FunctionConfigurationError,
    OutputConfigurationError,
)


def dummy_function(number):
    return number


def test_all_correct_with_no_input_no_output():
    function = TabsdataFunction(dummy_function, "dummy_function_name")
    assert function(42) == 42
    assert function.original_function == dummy_function
    assert isinstance(function, TabsdataFunction)
    assert function.output is None
    assert function.original_file == os.path.basename(__file__)
    assert function.original_folder == os.path.dirname(os.path.abspath(__file__))


def test_wrong_input_type_error():
    with pytest.raises(FunctionConfigurationError) as e:
        TabsdataFunction(dummy_function, "dummy_function_name", input=42)
    assert e.value.error_code == ErrorCode.FCE7


def test_func_not_callable_type_error():
    with pytest.raises(FunctionConfigurationError) as e:
        TabsdataFunction(42, "dummy_function_name")
    assert e.value.error_code == ErrorCode.FCE1


def test_trigger_none_stored():
    function = TabsdataFunction(dummy_function, "dummy_function_name", trigger_by=None)
    assert function.trigger_by is None


def test_trigger_empty_list_stored():
    function = TabsdataFunction(dummy_function, "dummy_function_name", trigger_by=[])
    assert function.trigger_by == []


def test_trigger_string():
    trigger = "table_name"
    function = TabsdataFunction(
        dummy_function, "dummy_function_name", trigger_by=trigger
    )
    assert isinstance(function.trigger_by, list)
    assert function.trigger_by[0] == "table_name"


def test_trigger_by_collection_table():
    uri = "testing_collection/table_name"
    function = TabsdataFunction(dummy_function, "dummy_function_name", trigger_by=uri)
    assert function.trigger_by[0] == uri


def test_trigger_by_wrong_type_raises_error():
    with pytest.raises(FunctionConfigurationError) as e:
        TabsdataFunction(dummy_function, "dummy_function_name", trigger_by=42)
    assert e.value.error_code == ErrorCode.FCE2


def test_trigger_string_list_converted():
    trigger = ["table_name", "table_name2"]
    function = TabsdataFunction(
        dummy_function, "dummy_function_name", trigger_by=trigger
    )
    assert isinstance(function.trigger_by, list)
    assert function.trigger_by[0] == "table_name"
    assert function.trigger_by[1] == "table_name2"


def test_trigger_by_uri_list_collection_table():
    uri = "testing_collection/table_name"
    uri2 = "testing_collection/table_name2"
    function = TabsdataFunction(
        dummy_function, "dummy_function_name", trigger_by=[uri, uri2]
    )
    assert function.trigger_by[0] == uri
    assert function.trigger_by[1] == uri2


def test_trigger_by_wrong_type_list_raises_error():
    with pytest.raises(FunctionConfigurationError) as e:
        TabsdataFunction(
            dummy_function, "dummy_function_name", trigger_by=["table_name", 42]
        )
    assert e.value.error_code == ErrorCode.FCE2


def test_build_output_wrong_type_raises_exception():
    output = "wrong_type"
    with pytest.raises(OutputConfigurationError) as e:
        build_output(output)
    assert e.value.error_code == ErrorCode.OCE7


def test_build_output_wrong_identifier_dict_raises_exception():
    output = {
        "wrong_identifier": {
            "uri": "mysql://path/to/data",
        }
    }
    with pytest.raises(OutputConfigurationError) as e:
        build_output(output)
    assert e.value.error_code == ErrorCode.OCE3


def test_build_output_multiple_identifiers_dict_raises_exception():
    output = {
        MySQLDestination.IDENTIFIER: {
            "uri": "mysql://path/to/data",
        },
        "wrong_identifier": {
            "uri": "mysql://path/to/data",
        },
    }
    with pytest.raises(OutputConfigurationError) as e:
        build_output(output)
    assert e.value.error_code == ErrorCode.OCE3


def test_build_output_wrong_identifier_value_raises_exception():
    output = {MySQLDestination.IDENTIFIER: 42}
    with pytest.raises(OutputConfigurationError) as e:
        build_output(output)
    assert e.value.error_code == ErrorCode.OCE4


def test_importer_exporter_raises_error():
    input = LocalFileSource("file://path/to/data/data.csv")
    output = MySQLDestination("mysql://DATABASE_IP:DATABASE_PORT/testing", "table")
    with pytest.raises(FunctionConfigurationError) as e:
        TabsdataFunction(
            dummy_function, "dummy_function_name", input=input, output=output
        )
    assert e.value.error_code == ErrorCode.FCE5


def test_wrong_type_name_raises_error():
    input = LocalFileSource("file://path/to/data/data.csv")
    with pytest.raises(FunctionConfigurationError) as e:
        TabsdataFunction(dummy_function, name=42, input=input)
    assert e.value.error_code == ErrorCode.FCE6
