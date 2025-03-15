#
# Copyright 2025 Tabs Data Inc.
#

import os
import time

import pytest
from tests_tabsdata.conftest import ABSOLUTE_TEST_FOLDER_LOCATION

from tabsdata.api.tabsdata_server import (
    Collection,
    Function,
    Table,
    convert_timestamp_to_string,
)


@pytest.mark.integration
def test_collection_class(tabsserver_connection):
    created_time = int(time.time())
    collection = Collection(
        connection=tabsserver_connection.connection,
        name="test_collection_class",
        created_on=created_time,
        created_by="test",
        description="test_collection_class_description",
        example_kwarg="example",
    )
    assert collection.name == "test_collection_class"
    assert collection.description == "test_collection_class_description"
    assert collection.created_on == created_time
    assert collection.created_on_string == convert_timestamp_to_string(created_time)
    assert collection.created_by == "test"
    assert collection.kwargs == {
        "example_kwarg": "example",
        "created_by": "test",
        "created_on": created_time,
    }
    assert collection.__repr__()
    assert collection.__str__()
    assert collection == collection
    assert collection != Collection(
        connection=tabsserver_connection.connection,
        name="test_collection_class_second_name",
        description="test_collection_class_description",
        id="test_collection_class_id",
        created_on=int(time.time()),
        created_by="test",
    )
    assert collection != "test"


@pytest.mark.integration
def test_collection_class_lazy_properties(tabsserver_connection):
    try:
        tabsserver_connection.collection_create(
            name="test_collection_class_lazy_properties",
            description="test_collection_class_lazy_properties",
        )
        example_collection = tabsserver_connection.collection_get(
            "test_collection_class_lazy_properties"
        )
        lazy_collection = Collection(
            tabsserver_connection.connection, example_collection.name
        )
        assert lazy_collection.name == example_collection.name
        assert lazy_collection.description == example_collection.description
        assert lazy_collection.created_on == example_collection.created_on
        assert lazy_collection.created_on_string == example_collection.created_on_string
        assert lazy_collection.created_by == example_collection.created_by
        assert lazy_collection._data
        assert lazy_collection.__repr__()
        assert lazy_collection.__str__()
    finally:
        tabsserver_connection.collection_delete(
            "test_collection_class_lazy_properties", raise_for_status=False
        )


@pytest.mark.integration
def test_collection_functions_property(tabsserver_connection):
    collection = Collection(
        tabsserver_connection.connection, "test_collection_function_property_collection"
    )
    try:
        collection.create()
        assert not collection.functions
        tabsserver_connection.function_create(
            collection_name="test_collection_function_property_collection",
            description="test_collection_function_property_collection_description",
            function_path=(
                f"{os.path.join(ABSOLUTE_TEST_FOLDER_LOCATION, "testing_resources",
                                "test_input_plugin", "example.py")}::input_plugin"
            ),
        )
        assert collection.functions
        assert isinstance(collection.functions, list)
        assert all(isinstance(function, Function) for function in collection.functions)
    finally:
        collection.delete(raise_for_status=False)


@pytest.mark.integration
@pytest.mark.slow
def test_collection_tables_property(
    testing_collection_with_table, tabsserver_connection
):
    collection = Collection(
        tabsserver_connection.connection, testing_collection_with_table
    )
    assert collection.tables
    assert isinstance(collection.tables, list)
    assert all(isinstance(table, Table) for table in collection.tables)
    collection_with_no_tables = Collection(
        tabsserver_connection.connection, "test_collection_tables_property_collection"
    )
    try:
        collection_with_no_tables.create()
        assert not collection_with_no_tables.tables
    finally:
        collection_with_no_tables.delete(raise_for_status=False)


@pytest.mark.integration
@pytest.mark.slow
def test_collection_get_tables(testing_collection_with_table, tabsserver_connection):
    collection = Collection(
        tabsserver_connection.connection, testing_collection_with_table
    )
    tables = collection.get_tables(offset=0, len=100)
    assert tables
    assert isinstance(tables, list)
    assert all(isinstance(table, Table) for table in tables)
    assert collection.get_table("output")
    with pytest.raises(ValueError):
        collection.get_table("doesnotexist")
    collection_with_no_tables = Collection(
        tabsserver_connection.connection, "test_collection_tables_property_collection"
    )
    try:
        collection_with_no_tables.create()
        assert not collection_with_no_tables.get_tables(offset=0, len=100)
    finally:
        collection_with_no_tables.delete(raise_for_status=False)


@pytest.mark.integration
def test_collection_delete(tabsserver_connection):
    collection = Collection(
        tabsserver_connection.connection, "test_collection_delete_collection"
    ).create()
    assert collection in tabsserver_connection.collections
    collection.delete()
    assert collection not in tabsserver_connection.collections


@pytest.mark.integration
def test_collection_update(tabsserver_connection):
    collection = Collection(
        tabsserver_connection.connection,
        "test_collection_update_collection",
        description="old_description",
    )
    try:
        collection.create()
        assert collection.description == "old_description"
        assert collection.name == "test_collection_update_collection"
        collection.update(
            name="test_collection_update_collection_new_name",
            description="new_description",
        )
        assert collection.description == "new_description"
        assert collection.name == "test_collection_update_collection_new_name"
    finally:
        collection.delete(raise_for_status=False)


@pytest.mark.integration
def test_collection_get_function(tabsserver_connection):
    collection = Collection(
        tabsserver_connection.connection, "test_collection_get_function_collection"
    )
    try:
        collection.create()
        assert not collection.functions
        tabsserver_connection.function_create(
            collection_name="test_collection_get_function_collection",
            description="test_collection_get_function_collection_description",
            function_path=(
                f"{os.path.join(ABSOLUTE_TEST_FOLDER_LOCATION, "testing_resources",
                                "test_input_plugin", "example.py")}::input_plugin"
            ),
        )
        function = collection.get_function("test_input_plugin")
        assert function
        assert isinstance(function, Function)
    finally:
        collection.delete(raise_for_status=False)


@pytest.mark.integration
def test_collection_create(tabsserver_connection):
    collection = Collection(
        tabsserver_connection.connection, "test_collection_create_collection"
    )
    try:
        collection.create()
        assert collection in tabsserver_connection.collections
    finally:
        collection.delete(raise_for_status=False)


@pytest.mark.integration
def test_collection_refresh(tabsserver_connection):
    collection = Collection(
        tabsserver_connection.connection,
        "test_collection_refresh_collection",
        description="old_description",
    )
    try:
        collection.create()
        assert collection in tabsserver_connection.collections
        assert collection.description == "old_description"
        tabsserver_connection.collection_update(
            "test_collection_refresh_collection", new_description="new_description"
        )
        assert collection.description == "old_description"
        collection.refresh()
        assert collection.description == "new_description"
    finally:
        collection.delete(raise_for_status=False)


@pytest.mark.integration
@pytest.mark.requires_internet
def test_collection_register_function(tabsserver_connection):
    collection = Collection(
        tabsserver_connection.connection, "test_collection_register_function_collection"
    )
    try:
        collection.create()
        collection.register_function(
            description="test_function_create_description",
            function_path=(
                f"{os.path.join(ABSOLUTE_TEST_FOLDER_LOCATION, "testing_resources",
                                "test_input_plugin", "example.py")}::input_plugin"
            ),
        )
        functions = tabsserver_connection.collection_list_functions(
            "test_collection_register_function_collection"
        )
        assert any(function.name == "test_input_plugin" for function in functions)
        functions = collection.functions
        assert any(function.name == "test_input_plugin" for function in functions)
    finally:
        tabsserver_connection.function_delete(
            "test_collection_register_function_collection",
            "test_input_plugin",
            raise_for_status=False,
        )
        collection.delete(raise_for_status=False)


@pytest.mark.integration
@pytest.mark.requires_internet
def test_collection_class_update_function(tabsserver_connection):
    collection = Collection(
        tabsserver_connection.connection,
        "test_collection_class_update_function_collection",
    )
    try:
        collection.create()
        collection.register_function(
            description="test_function_create_description",
            function_path=(
                f"{os.path.join(ABSOLUTE_TEST_FOLDER_LOCATION, "testing_resources",
                                "test_input_plugin", "example.py")}::input_plugin"
            ),
        )
        functions = tabsserver_connection.collection_list_functions(
            "test_collection_class_update_function_collection"
        )
        assert any(function.name == "test_input_plugin" for function in functions)
        functions = collection.functions
        assert any(function.name == "test_input_plugin" for function in functions)

        new_description = "test_collection_class_update_function_new_description"
        new_function_path = (
            f"{os.path.join(ABSOLUTE_TEST_FOLDER_LOCATION, 'testing_resources',
                            'test_input_file_csv_modified_format', 'example.py')}"
            "::input_file_csv_modified_format"
        )
        collection.update_function(
            "test_input_plugin",
            description=new_description,
            function_path=new_function_path,
        )
        functions = tabsserver_connection.collection_list_functions(
            "test_collection_class_update_function_collection"
        )
        assert len(functions) == 1
        assert functions[0].name == "input_file_csv_modified_format"
        functions = collection.functions
        assert len(functions) == 1
        assert functions[0].name == "input_file_csv_modified_format"

    finally:
        tabsserver_connection.function_delete(
            "test_collection_class_update_function_collection",
            "input_file_csv_modified_format",
        )
        collection.delete(raise_for_status=False)
