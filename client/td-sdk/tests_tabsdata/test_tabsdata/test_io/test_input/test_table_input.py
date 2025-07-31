#
# Copyright 2024 Tabs Data Inc.
#

import pytest

from tabsdata.exceptions import TableURIConfigurationError, TabsDataException
from tabsdata.io.inputs.table_inputs import TableInput
from tabsdata.io.plugin import SourcePlugin


def test_all_correct_string():
    table = "collection/table@HEAD^"
    input = TableInput(table)
    assert input.table == table
    assert isinstance(input, TableInput)
    assert isinstance(input, SourcePlugin)
    assert input.__repr__()


def test_all_correct_string_no_dataset():
    table = "table@HEAD^"
    input = TableInput(table)
    assert input.table == table
    assert isinstance(input, TableInput)
    assert isinstance(input, SourcePlugin)
    assert input.__repr__()


def test_all_correct_string_no_version():
    table = "table"
    input = TableInput(table)
    assert input.table == table
    assert isinstance(input, TableInput)
    assert isinstance(input, SourcePlugin)
    assert input.__repr__()


def test_identifier_string_unchanged():
    table = "collection/table@HEAD^"
    input = TableInput(table)
    assert input.table == table
    assert isinstance(input, TableInput)
    assert isinstance(input, SourcePlugin)
    assert input.__repr__()


def test_all_correct_query_list():
    table = [
        "collection/table@HEAD^",
        "collection/table@HEAD^^^",
    ]
    input = TableInput(table)
    assert input.table == table
    assert isinstance(input, TableInput)
    assert isinstance(input, SourcePlugin)
    assert input.__repr__()


def test_different_input_not_eq():
    table = "collection/table@HEAD^"
    input = TableInput(table)
    table2 = "collection2/table@HEAD^"
    input2 = TableInput(table2)
    assert input != input2


def test_input_not_eq_dict():
    table = "collection/table@HEAD^"
    input = TableInput(table)
    assert input.to_dict() != input


def test_wrong_scheme_raises_value_error():
    table = "wrongscheme:///path/to/query"
    with pytest.raises(TableURIConfigurationError):
        TableInput(table)


def test_table_wrong_type_raises_type_error():
    table = 42
    with pytest.raises(TableURIConfigurationError):
        TableInput(table)


def test_empty_table():
    table = ""
    with pytest.raises(TableURIConfigurationError):
        TableInput(table)


def test_update_table():
    table = "collection/table@HEAD^"
    input = TableInput(table)
    assert input.table == table
    table2 = "collection2/table2@HEAD^"
    input.table = table2
    assert input.table == table2


def test_valid_table_dataset_table():
    table = "table"
    assert TableInput(table)


def test_valid_table_collection_dataset_table():
    table = "collection/table"
    assert TableInput(table)


def test_valid_table_collection_dataset_table_version():
    table = "collection/table@HEAD^"
    assert TableInput(table)


def test_invalid_table_no_table():
    table = "collection/"
    with pytest.raises(TableURIConfigurationError):
        TableInput(table)


def test_invalid_table_no_dataset():
    table = "collection//table"
    with pytest.raises(TableURIConfigurationError):
        TableInput(table)


def test_invalid_table_no_table_version():
    table = "table@"
    with pytest.raises(TableURIConfigurationError):
        TableInput(table)


def test_invalid_table_too_many_slashes():
    table = "//table"
    with pytest.raises(TableURIConfigurationError):
        TableInput(table)


def test_invalid_table_too_many_at():
    table = "table@HEAD^@"
    with pytest.raises(TableURIConfigurationError):
        TableInput(table)


def test_invalid_table_too_many_ranges():
    table = "table@HEAD~7..HEAD~5...HEAD"
    with pytest.raises(TableURIConfigurationError):
        TableInput(table)


def test_rust_valid_tables():
    valid_tables = [
        "table",
        "collection/table",
        "table@HEAD",
        "collection/table@HEAD",
        "collection/table@HEAD^",
        "collection/table@HEAD~1",
        "collection/table@HEAD^^^^,HEAD^,HEAD",
        "collection/table@HEAD^^..HEAD",
    ]
    for table in valid_tables:
        assert TableInput(table)


def test_rust_invalid_tables():
    invalid_tables = [
        "collection/",
        "@HEAD",
        "collection/@HEAD",
        "/",
        "collection//",
        "collection/",
        "collection/table/",
        "dataset@head",
        "dataset@HEAD-1",
        "table@01234567890123456789012",
    ]
    for table in invalid_tables:
        with pytest.raises(TabsDataException):
            TableInput(table)
