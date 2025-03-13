#
# Copyright 2024 Tabs Data Inc.
#

import os

import pytest

from tabsdata import CSVFormat
from tabsdata.exceptions import (
    ErrorCode,
    FunctionConfigurationError,
    InputConfigurationError,
    OutputConfigurationError,
)
from tabsdata.io.input import LocalFileSource, MySQLSource, build_input
from tabsdata.io.output import MySQLDestination, build_output
from tabsdata.tabsdatafunction import TabsdataFunction

QUERY_KEY = MySQLSource.QUERY_KEY
URI_KEY = MySQLSource.URI_KEY


def dummy_function(number):
    return number


def test_all_correct_input_dict():
    input = {
        LocalFileSource.IDENTIFIER: {
            LocalFileSource.PATH_KEY: "file://path/to/data/data.csv",
        },
    }

    function = TabsdataFunction(dummy_function, "dummy_function_name", input=input)
    assert function(42) == 42
    assert function.original_function == dummy_function
    assert isinstance(function.input, LocalFileSource)
    assert (
        function.input.path
        == input[LocalFileSource.IDENTIFIER][LocalFileSource.PATH_KEY]
    )
    assert isinstance(function.input.format, CSVFormat)
    assert isinstance(function, TabsdataFunction)
    assert function.output is None
    assert function.original_file == os.path.basename(__file__)
    assert function.original_folder == os.path.dirname(os.path.abspath(__file__))
    assert function == function
    assert function.__repr__()


def test_all_correct_mysql_input_dict():
    input = {
        MySQLSource.IDENTIFIER: {
            URI_KEY: "mysql://DATABASE_IP:DATABASE_PORT/testing",
            QUERY_KEY: [
                "select * from INVOICE_HEADER where id > 0",
                "select * from INVOICE_ITEM where id > 0",
            ],
        },
    }

    function = TabsdataFunction(dummy_function, "dummy_function_name", input=input)
    assert function(42) == 42
    assert function.original_function == dummy_function
    assert isinstance(function.input, MySQLSource)
    assert function.input.uri == input[MySQLSource.IDENTIFIER][URI_KEY]
    assert function.input.query == input[MySQLSource.IDENTIFIER][QUERY_KEY]
    assert function.input.database == "testing"
    assert function.input.host == "DATABASE_IP"
    assert function.input.port == "DATABASE_PORT"
    assert isinstance(function, TabsdataFunction)
    assert function.output is None
    assert function.original_file == os.path.basename(__file__)
    assert function.original_folder == os.path.dirname(os.path.abspath(__file__))


def test_no_import_files_dict_raises_exception():
    input = {LocalFileSource.IDENTIFIER: 42}

    with pytest.raises(InputConfigurationError) as e:
        TabsdataFunction(dummy_function, "dummy_function_name", input=input)
    assert e.value.error_code == ErrorCode.ICE8


def test_all_correct_with_mysql_input_object():
    input = {
        MySQLSource.IDENTIFIER: {
            URI_KEY: "mysql://DATABASE_IP:DATABASE_PORT/testing",
            QUERY_KEY: [
                "select * from INVOICE_HEADER where id > 0",
                "select * from INVOICE_ITEM where id > 0",
            ],
        },
    }
    input_object = build_input(input)
    function = TabsdataFunction(
        dummy_function, "dummy_function_name", input=input_object
    )
    assert function(42) == 42
    assert function.original_function == dummy_function
    assert isinstance(function.input, MySQLSource)
    assert function.input.uri == input[MySQLSource.IDENTIFIER][URI_KEY]
    assert function.input.query == input[MySQLSource.IDENTIFIER][QUERY_KEY]
    assert function.input.database == "testing"
    assert function.input.host == "DATABASE_IP"
    assert function.input.port == "DATABASE_PORT"
    assert isinstance(function, TabsdataFunction)
    assert function.output is None
    assert function.original_file == os.path.basename(__file__)
    assert function.original_folder == os.path.dirname(os.path.abspath(__file__))


def test_all_correct_with_input_object():
    input = {
        LocalFileSource.IDENTIFIER: {
            LocalFileSource.PATH_KEY: "file://path/to/data/data.csv",
        }
    }
    input_object = build_input(input)
    function = TabsdataFunction(
        dummy_function, "dummy_function_name", input=input_object
    )
    assert function(42) == 42
    assert function.original_function == dummy_function
    assert isinstance(function.input, LocalFileSource)
    assert (
        function.input.path
        == input[LocalFileSource.IDENTIFIER][LocalFileSource.PATH_KEY]
    )
    assert isinstance(function.input.format, CSVFormat)
    assert isinstance(function, TabsdataFunction)
    assert function.output is None
    assert function.original_file == os.path.basename(__file__)
    assert function.original_folder == os.path.dirname(os.path.abspath(__file__))


def test_all_correct_input_dict_format_object():
    input = {
        LocalFileSource.IDENTIFIER: {
            LocalFileSource.PATH_KEY: "file://path/to/data/data.csv",
            LocalFileSource.FORMAT_KEY: CSVFormat(
                separator=".", input_has_header=False
            ),
        },
    }

    function = TabsdataFunction(dummy_function, "dummy_function_name", input=input)
    assert function(42) == 42
    assert function.original_function == dummy_function
    assert isinstance(function.input, LocalFileSource)
    assert (
        function.input.path
        == input[LocalFileSource.IDENTIFIER][LocalFileSource.PATH_KEY]
    )
    assert isinstance(function.input.format, CSVFormat)
    assert function.input.format.input_has_header is False
    assert function.input.format.separator == "."
    assert isinstance(function, TabsdataFunction)
    assert function.output is None
    assert function.original_file == os.path.basename(__file__)
    assert function.original_folder == os.path.dirname(os.path.abspath(__file__))


def test_all_correct_with_input_object_format_dict():
    input = {
        LocalFileSource.IDENTIFIER: {
            LocalFileSource.PATH_KEY: "file://path/to/data/data.csv",
            LocalFileSource.FORMAT_KEY: CSVFormat(
                separator=".", input_has_header=False
            ),
        },
    }
    input_object = build_input(input)
    function = TabsdataFunction(
        dummy_function, "dummy_function_name", input=input_object
    )
    assert function(42) == 42
    assert function.original_function == dummy_function
    assert isinstance(function.input, LocalFileSource)
    assert (
        function.input.path
        == input[LocalFileSource.IDENTIFIER][LocalFileSource.PATH_KEY]
    )
    assert isinstance(function.input.format, CSVFormat)
    assert function.input.format.input_has_header is False
    assert function.input.format.separator == "."
    assert isinstance(function, TabsdataFunction)
    assert function.output is None
    assert function.original_file == os.path.basename(__file__)
    assert function.original_folder == os.path.dirname(os.path.abspath(__file__))


def test_all_correct_with_no_input_no_output():
    function = TabsdataFunction(dummy_function, "dummy_function_name")
    assert function(42) == 42
    assert function.original_function == dummy_function
    assert isinstance(function, TabsdataFunction)
    assert function.output is None
    assert function.original_file == os.path.basename(__file__)
    assert function.original_folder == os.path.dirname(os.path.abspath(__file__))


def test_wrong_input_type_error():
    with pytest.raises(InputConfigurationError) as e:
        TabsdataFunction(dummy_function, "dummy_function_name", input=42)
    assert e.value.error_code == ErrorCode.ICE11


def test_wrong_mysql_input_dict_no_data_error():
    input = {
        MySQLSource.IDENTIFIER: {
            URI_KEY: "mysql://DATABASE_IP:DATABASE_PORT/testing",
        }
    }
    with pytest.raises(TypeError):
        TabsdataFunction(dummy_function, "dummy_function_name", input=input)


def test_wrong_input_dict_no_identifier_error():
    input = {
        URI_KEY: "file://path/to/data",
        QUERY_KEY: "data.csv",
    }
    with pytest.raises(InputConfigurationError) as e:
        TabsdataFunction(dummy_function, "dummy_function_name", input=input)
    assert e.value.error_code == ErrorCode.ICE7


def test_wrong_input_dict_wrong_identifier_error():
    input = {
        "wrong-identifier": {
            URI_KEY: "file://path/to/data",
            QUERY_KEY: "data.csv",
        }
    }
    with pytest.raises(InputConfigurationError) as e:
        TabsdataFunction(dummy_function, "dummy_function_name", input=input)
    assert e.value.error_code == ErrorCode.ICE7


def test_wrong_input_dict_multiple_identifiers_error():
    input = {
        LocalFileSource.IDENTIFIER: {
            LocalFileSource.PATH_KEY: "file://path/to/data/data.csv",
        },
        MySQLSource.IDENTIFIER: {
            URI_KEY: "file://path/to/data",
            QUERY_KEY: "data.csv",
        },
    }
    with pytest.raises(InputConfigurationError) as e:
        TabsdataFunction(dummy_function, "dummy_function_name", input=input)
    assert e.value.error_code == ErrorCode.ICE7


def test_wrong_input_dict_no_path_error():
    input = {
        LocalFileSource.IDENTIFIER: {
            QUERY_KEY: "data.csv",
        }
    }
    with pytest.raises(TypeError):
        TabsdataFunction(dummy_function, "dummy_function_name", input=input)


def test_wrong_mysql_input_dict_no_uri_error():
    input = {
        MySQLSource.IDENTIFIER: {
            QUERY_KEY: {
                "headers": "select * from INVOICE_HEADER where id > 0",
                "items": "select * from INVOICE_ITEM where id > 0",
            },
        }
    }
    with pytest.raises(TypeError):
        TabsdataFunction(dummy_function, "dummy_function_name", input=input)


def test_wrong_input_dict_unsupported_scheme_error():
    input = {
        LocalFileSource.IDENTIFIER: {
            LocalFileSource.PATH_KEY: "wrongscheme://path/to/data/data.csv",
        }
    }
    with pytest.raises(InputConfigurationError) as e:
        TabsdataFunction(dummy_function, "dummy_function_name", input=input)
    assert e.value.error_code == ErrorCode.ICE14


def test_wrong_mysql_input_dict_unsupported_scheme_error():
    input = {
        MySQLSource.IDENTIFIER: {
            URI_KEY: "wrongscheme://path/to/data",
            QUERY_KEY: [
                "select * from INVOICE_HEADER where id > 0",
                "select * from INVOICE_ITEM where id > 0",
            ],
        }
    }
    with pytest.raises(InputConfigurationError) as e:
        TabsdataFunction(dummy_function, "dummy_function_name", input=input)
    assert e.value.error_code == ErrorCode.ICE2


def test_mysql_wrong_initial_values_type_raises_error():
    input = {
        MySQLSource.IDENTIFIER: {
            URI_KEY: "mysql://DATABASE_IP:DATABASE_PORT/testing",
            QUERY_KEY: [
                "select * from INVOICE_HEADER where id > :number",
                "select * from INVOICE_ITEM where id > :number",
            ],
            "initial_values": 42,
        },
    }
    with pytest.raises(InputConfigurationError) as e:
        TabsdataFunction(dummy_function, "dummy_function_name", input=input)
    assert e.value.error_code == ErrorCode.ICE12


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


def test_update_input():
    input = LocalFileSource("file://path/to/data/data.csv")
    function = TabsdataFunction(dummy_function, "dummy_function_name", input=input)
    assert isinstance(function.input, LocalFileSource)
    assert function.input == input

    input = {
        MySQLSource.IDENTIFIER: {
            URI_KEY: "mysql://DATABASE_IP:DATABASE_PORT/testing",
            QUERY_KEY: [
                "select * from INVOICE_HEADER where id > 0",
                "select * from INVOICE_ITEM where id > 0",
            ],
        },
    }
    function.input = input
    assert isinstance(function.input, MySQLSource)


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
