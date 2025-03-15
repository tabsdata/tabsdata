#
# Copyright 2025 Tabs Data Inc.
#

import os

import pytest
from tests_tabsdata.conftest import ABSOLUTE_TEST_FOLDER_LOCATION

from tabsdata.api.tabsdata_server import Collection, Function, Table


@pytest.mark.integration
@pytest.mark.slow
def test_function_class(tabsserver_connection, testing_collection_with_table):
    function = Function(
        connection=tabsserver_connection.connection,
        name="test",
        collection=testing_collection_with_table,
        id="test",
        trigger_with_names=["trigger"],
        tables=None,
        dependencies_with_names=["dependency"],
    )
    assert function.id == "test"
    assert function.name == "test"
    assert isinstance(function.collection, Collection)
    assert function.collection.name == testing_collection_with_table
    assert function.trigger_with_names == ["trigger"]
    assert function.dependencies_with_names == ["dependency"]
    assert function.__repr__()
    assert function.__str__()


@pytest.mark.integration
@pytest.mark.slow
def test_function_class_lazy_properties(
    tabsserver_connection, testing_collection_with_table
):
    api_function = tabsserver_connection.collection_get(
        testing_collection_with_table
    ).functions[0]
    function = Function(
        tabsserver_connection.connection,
        collection=testing_collection_with_table,
        name=api_function.name,
    )
    api_function.refresh()
    assert function.id == api_function.id
    assert function == api_function
    assert function.tables == api_function.tables
    assert function.tables
    assert all(isinstance(table, Table) for table in function.tables)
    table = function.tables[0]
    assert table.function == function
    assert function.get_table("output")
    with pytest.raises(ValueError):
        function.get_table("doesnotexist")
    assert table.collection == function.collection
    assert function.dependencies_with_names == api_function.dependencies_with_names
    assert function.trigger_with_names == api_function.trigger_with_names
    function.refresh()
    assert function.description == api_function.description
    assert function.created_on == api_function.created_on
    assert function.created_by == api_function.created_by
    assert function.created_on_string == api_function.created_on_string
    assert function.__repr__()
    assert function.__str__()
    second_function = Function(
        tabsserver_connection.connection,
        collection=Collection(
            tabsserver_connection.connection, testing_collection_with_table
        ),
        name=api_function.name,
    )
    assert second_function == function
    assert second_function.collection == function.collection


@pytest.mark.integration
@pytest.mark.requires_internet
def test_function_class_register(tabsserver_connection):
    collection = Collection(
        tabsserver_connection.connection, "test_function_class_register_collection"
    )
    try:
        collection.create()
        function = Function(
            tabsserver_connection.connection,
            collection=collection,
            name="test_function_class_register_function",
        )
        function.register(
            description="test_function_create_description",
            function_path=(
                f"{os.path.join(ABSOLUTE_TEST_FOLDER_LOCATION, "testing_resources",
                                "test_input_plugin", "example.py")}::input_plugin"
            ),
        )
        functions = tabsserver_connection.collection_list_functions(
            "test_function_class_register_collection"
        )
        assert any(
            function.name == "test_function_class_register_function"
            for function in functions
        )
        functions = collection.functions
        assert any(
            function.name == "test_function_class_register_function"
            for function in functions
        )
    finally:
        try:
            function.delete(raise_for_status=False)
        except Exception:
            pass
        collection.delete(raise_for_status=False)


@pytest.mark.integration
@pytest.mark.requires_internet
@pytest.mark.skip("Skipping until deleting functions is supported by the backend")
def test_function_class_delete(tabsserver_connection):
    collection = Collection(
        tabsserver_connection.connection, "test_function_class_delete_collection"
    )
    try:
        collection.create()
        function = Function(
            tabsserver_connection.connection,
            collection=collection,
            name="test_function_class_delete_function",
        )
        function.register(
            description="test_function_create_description",
            function_path=(
                f"{os.path.join(ABSOLUTE_TEST_FOLDER_LOCATION, "testing_resources",
                                "test_input_plugin", "example.py")}::input_plugin"
            ),
        )
        functions = tabsserver_connection.collection_list_functions(
            "test_function_class_delete_collection"
        )
        assert any(
            function.name == "test_function_class_delete_function"
            for function in functions
        )
        functions = collection.functions
        assert any(
            function.name == "test_function_class_delete_function"
            for function in functions
        )
        function.delete()
        functions = tabsserver_connection.collection_list_functions(
            "test_function_class_delete_collection"
        )
        assert not (
            any(
                function.name == "test_function_class_delete_function"
                for function in functions
            )
        )
        functions = collection.functions
        assert not (
            any(
                function.name == "test_function_class_delete_function"
                for function in functions
            )
        )
    finally:
        try:
            function.delete(raise_for_status=False)
        except Exception:
            pass
        collection.delete(raise_for_status=False)


@pytest.mark.integration
@pytest.mark.requires_internet
def test_function_class_update(tabsserver_connection):
    collection = Collection(
        tabsserver_connection.connection, "test_function_class_update_collection"
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
            "test_function_class_update_collection"
        )
        assert any(function.name == "test_input_plugin" for function in functions)
        functions = collection.functions
        assert any(function.name == "test_input_plugin" for function in functions)

        new_description = "test_function_class_update_new_description"
        new_function_path = (
            f"{os.path.join(ABSOLUTE_TEST_FOLDER_LOCATION, 'testing_resources',
                            'test_input_file_csv_modified_format', 'example.py')}"
            "::input_file_csv_modified_format"
        )
        function = collection.get_function("test_input_plugin")
        function.update(description=new_description, function_path=new_function_path)
        functions = tabsserver_connection.collection_list_functions(
            "test_function_class_update_collection"
        )
        assert len(functions) == 1
        assert functions[0].name == "input_file_csv_modified_format"
        functions = collection.functions
        assert len(functions) == 1
        assert functions[0].name == "input_file_csv_modified_format"
        assert function.description == new_description

    finally:
        try:
            function.delete(raise_for_status=False)
        except Exception:
            pass
        collection.delete(raise_for_status=False)


@pytest.mark.integration
@pytest.mark.requires_internet
def test_function_class_history(tabsserver_connection):
    collection = Collection(
        tabsserver_connection.connection, "test_function_class_history_collection"
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
            "test_function_class_history_collection"
        )
        assert any(function.name == "test_input_plugin" for function in functions)
        functions = collection.functions
        assert any(function.name == "test_input_plugin" for function in functions)

        new_description = "test_function_class_history_new_description"
        new_function_path = (
            f"{os.path.join(ABSOLUTE_TEST_FOLDER_LOCATION, 'testing_resources',
                            'test_input_file_csv_modified_format', 'example.py')}"
            "::input_file_csv_modified_format"
        )
        function = collection.get_function("test_input_plugin")
        function.update(description=new_description, function_path=new_function_path)
        functions = tabsserver_connection.collection_list_functions(
            "test_function_class_history_collection"
        )
        assert len(functions) == 1
        assert functions[0].name == "input_file_csv_modified_format"
        functions = collection.functions
        assert len(functions) == 1
        assert functions[0].name == "input_file_csv_modified_format"
        assert function.description == new_description
        assert function.history
        assert all(
            isinstance(historic_function, Function)
            for historic_function in function.history
        )
        assert all(historic_function.id for historic_function in function.history)
    finally:
        try:
            function.delete(raise_for_status=False)
        except Exception:
            pass
        collection.delete(raise_for_status=False)


@pytest.mark.integration
@pytest.mark.slow
def test_function_class_dataversions(
    tabsserver_connection, testing_collection_with_table
):
    function_name = tabsserver_connection.collection_list_functions(
        testing_collection_with_table
    )[0].name
    function = Function(
        tabsserver_connection.connection,
        collection=testing_collection_with_table,
        name=function_name,
    )
    data_versions = function.data_versions
    assert data_versions
    data_version = data_versions[0]
    assert data_version.function == function


@pytest.mark.integration
def test_function_class_get_dataversions(
    tabsserver_connection, testing_collection_with_table
):
    function_name = tabsserver_connection.collection_list_functions(
        testing_collection_with_table
    )[0].name
    function = Function(
        tabsserver_connection.connection,
        collection=testing_collection_with_table,
        name=function_name,
    )
    data_versions = function.get_dataversions(offset=0, len=100)
    assert data_versions
    data_version = data_versions[0]
    assert data_version.function == function


@pytest.mark.integration
def test_function_class_workers(tabsserver_connection, testing_collection_with_table):
    function_name = tabsserver_connection.collection_list_functions(
        testing_collection_with_table
    )[0].name
    function = Function(
        tabsserver_connection.connection,
        collection=testing_collection_with_table,
        name=function_name,
    )
    workers = function.workers
    assert workers
    worker = workers[0]
    assert worker.function == function
    assert worker.function.id == function.id
    assert worker.function.get_worker(worker.id) == worker


@pytest.mark.integration
def test_function_class_get_worker(
    tabsserver_connection, testing_collection_with_table
):
    function_name = tabsserver_connection.collection_list_functions(
        testing_collection_with_table
    )[0].name
    function = Function(
        tabsserver_connection.connection,
        collection=testing_collection_with_table,
        name=function_name,
    )
    workers = function.workers
    assert workers
    worker = function.get_worker(workers[0].id)
    assert worker.function == function
    assert worker.function.id == function.id
    assert worker.function.get_worker(worker.id) == worker
