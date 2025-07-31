#
# Copyright 2024 Tabs Data Inc.
#

import pytest

from tabsdata._io.output import Output, TableOutput, build_output
from tabsdata.exceptions import ErrorCode, OutputConfigurationError


def test_all_correct_destination_table_list():
    destination_table = ["headers_table", "invoices_table"]
    output = TableOutput(destination_table)
    assert output.table == destination_table
    assert isinstance(output, TableOutput)
    assert isinstance(output, Output)
    expected_dict = {
        TableOutput.IDENTIFIER: {
            TableOutput.TABLE_KEY: destination_table,
        }
    }
    assert output._to_dict() == expected_dict
    assert output.__repr__()
    assert isinstance(build_output(output._to_dict()), TableOutput)


def test_identifier_string_unchanged():
    destination_table = "headers_table"
    output = TableOutput(destination_table)
    expected_dict = {
        "table-output": {
            TableOutput.TABLE_KEY: [destination_table],
        }
    }
    assert output._to_dict() == expected_dict
    assert output.__repr__()
    assert isinstance(build_output(output._to_dict()), TableOutput)


def test_all_correct_destination_table_string():
    destination_table = "output_table"
    output = TableOutput(destination_table)
    assert output.table == destination_table
    assert isinstance(output, TableOutput)
    assert isinstance(output, Output)
    expected_dict = {
        TableOutput.IDENTIFIER: {
            TableOutput.TABLE_KEY: [destination_table],
        }
    }
    assert output._to_dict() == expected_dict
    assert output.__repr__()
    assert isinstance(build_output(output._to_dict()), TableOutput)


def test_same_table_eq():
    destination_table = "output_table"
    output = TableOutput(destination_table)
    output2 = TableOutput(destination_table)
    assert output == output2


def test_different_table_not_eq():
    destination_table = "output_table"
    output = TableOutput(destination_table)
    table2 = "second_output_table"
    output2 = TableOutput(table2)
    assert output != output2


def test_input_not_eq_dict():
    destination_table = "output_table"
    output = TableOutput(destination_table)
    assert output._to_dict() != output


def test_table_wrong_type_raises_type_error():
    destination_table = 42
    with pytest.raises(OutputConfigurationError) as e:
        TableOutput(destination_table)
    assert e.value.error_code == ErrorCode.OCE10


def test_update_destination_table():
    destination_table = "output_table"
    output = TableOutput(destination_table)
    assert output.table == destination_table
    assert output._table_list == [destination_table]
    output.table = "new_output_table"
    assert output.table == "new_output_table"
    assert output._table_list == ["new_output_table"]
    output.table = ["new_output_table", "new_output_table2"]
    assert output.table == ["new_output_table", "new_output_table2"]
    assert output._table_list == ["new_output_table", "new_output_table2"]
