#
# Copyright 2024 Tabs Data Inc.
#

import pytest

from tabsdata._io.outputs.table_outputs import TableOutput
from tabsdata._io.plugin import DestinationPlugin
from tabsdata.exceptions import DestinationConfigurationError, ErrorCode


def test_all_correct_destination_table_list():
    destination_table = ["headers_table", "invoices_table"]
    output = TableOutput(destination_table)
    assert output.table == destination_table
    assert isinstance(output, TableOutput)
    assert isinstance(output, DestinationPlugin)
    assert output.__repr__()


def test_all_correct_destination_table_string():
    destination_table = "output_table"
    output = TableOutput(destination_table)
    assert output.table == destination_table
    assert isinstance(output, TableOutput)
    assert isinstance(output, DestinationPlugin)
    assert output.__repr__()


def test_table_wrong_type_raises_type_error():
    destination_table = 42
    with pytest.raises(DestinationConfigurationError) as e:
        TableOutput(destination_table)
    assert e.value.error_code == ErrorCode.DECE10


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
