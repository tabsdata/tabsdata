#
# Copyright 2025 Tabs Data Inc.
#

import os
import uuid
from http import HTTPStatus

import pytest

from tabsdata.api.tabsdata_server import (
    Collection,
    Function,
    FunctionRun,
    Table,
    Worker,
)
from tests_tabsdata.conftest import ABSOLUTE_TEST_FOLDER_LOCATION, LOCAL_PACKAGES_LIST

# noinspection PyUnresolvedReferences
from . import pytestmark  # noqa: F401


@pytest.mark.integration
@pytest.mark.slow
def test_function_class(tabsserver_connection, testing_collection_with_table):
    function = Function(
        connection=tabsserver_connection.connection,
        name="test",
        collection=testing_collection_with_table,
        id="test",
        triggers=["trigger"],
        tables=None,
        dependencies=["dependency"],
    )
    assert function.id == "test"
    assert function.name == "test"
    assert isinstance(function.collection, Collection)
    assert function.collection.name == testing_collection_with_table
    assert function.triggers == ["trigger"]
    assert function.dependencies == ["dependency"]
    assert function.__repr__()
    assert function.__str__()


@pytest.mark.integration
@pytest.mark.slow
def test_function_class_lazy_properties(
    tabsserver_connection, testing_collection_with_table
):
    api_function = tabsserver_connection.get_collection(
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
    assert function.dependencies == api_function.dependencies
    assert function.triggers == api_function.triggers
    function.refresh()
    assert function.description == api_function.description
    assert function.defined_on == api_function.defined_on
    assert function.defined_by == api_function.defined_by
    assert function.defined_on_str == api_function.defined_on_str
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
    collection.create()
    function = Function(
        tabsserver_connection.connection,
        collection=collection,
        name="test_function_class_register_function",
    )
    try:
        function.register(
            description="test_function_register_description",
            function_path=(
                f"{os.path.join(ABSOLUTE_TEST_FOLDER_LOCATION, "testing_resources",
                                "test_input_plugin", "example.py")}::input_plugin"
            ),
            local_packages=LOCAL_PACKAGES_LIST,
        )
        functions = tabsserver_connection.list_functions(
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
        function.delete(raise_for_status=False)
        collection.delete(raise_for_status=False)


@pytest.mark.integration
def test_function_class_delete(tabsserver_connection):
    collection = Collection(
        tabsserver_connection.connection, "test_function_class_delete_collection"
    )
    collection.create()
    function = Function(
        tabsserver_connection.connection,
        collection=collection,
        name="test_function_class_delete_function",
    )
    try:
        function.register(
            description="test_function_register_description",
            function_path=(
                f"{os.path.join(ABSOLUTE_TEST_FOLDER_LOCATION, "testing_resources",
                                "test_input_plugin", "example.py")}::input_plugin"
            ),
            local_packages=LOCAL_PACKAGES_LIST,
        )
        functions = tabsserver_connection.list_functions(
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
        functions = tabsserver_connection.list_functions(
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
        function.delete(raise_for_status=False)
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
            description="test_function_register_description",
            function_path=(
                f"{os.path.join(ABSOLUTE_TEST_FOLDER_LOCATION, "testing_resources",
                                "test_input_plugin", "example.py")}::input_plugin"
            ),
            local_packages=LOCAL_PACKAGES_LIST,
        )
        functions = tabsserver_connection.list_functions(
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
        functions = tabsserver_connection.list_functions(
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
            description="test_function_register_description",
            function_path=(
                f"{os.path.join(ABSOLUTE_TEST_FOLDER_LOCATION, "testing_resources",
                                "test_input_plugin", "example.py")}::input_plugin"
            ),
            local_packages=LOCAL_PACKAGES_LIST,
        )
        functions = tabsserver_connection.list_functions(
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
        functions = tabsserver_connection.list_functions(
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
def test_function_class_read_run(tabsserver_connection):
    collection = tabsserver_connection.create_collection(
        f"test_function_class_read_run_{uuid.uuid4().hex[:16]}"
    )
    file_path = os.path.join(
        ABSOLUTE_TEST_FOLDER_LOCATION,
        "testing_resources",
        "test_input_file_csv_string_format",
        "example.py",
    )
    function_path = file_path + "::input_file_csv_string_format"
    function = collection.register_function(
        function_path, local_packages=LOCAL_PACKAGES_LIST
    )
    plan = function.trigger(
        f"test_function_class_read_run_plan_{uuid.uuid4().hex[:16]}"
    )
    response = function.read_run(plan)
    assert HTTPStatus(response.status_code).is_success
    response = function.read_run(plan.id)
    assert HTTPStatus(response.status_code).is_success


@pytest.mark.integration
@pytest.mark.slow
def test_function_class_workers(tabsserver_connection, testing_collection_with_table):
    function_name = tabsserver_connection.list_functions(testing_collection_with_table)[
        0
    ].name
    function = Function(
        tabsserver_connection.connection,
        collection=testing_collection_with_table,
        name=function_name,
    )
    workers = function.workers
    assert workers
    assert isinstance(workers, list)
    worker = workers[0]
    assert worker.function == function
    assert worker.function.id == function.id
    assert worker.function.get_worker(worker.id) == worker
    assert isinstance(worker, Worker)


@pytest.mark.integration
@pytest.mark.slow
def test_function_class_runs(tabsserver_connection, testing_collection_with_table):
    function_name = tabsserver_connection.list_functions(testing_collection_with_table)[
        0
    ].name
    function = Function(
        tabsserver_connection.connection,
        collection=testing_collection_with_table,
        name=function_name,
    )
    runs = function.runs
    assert runs
    assert isinstance(runs, list)
    run = runs[0]
    assert run.function == function
    assert run.function.id == function.id
    assert isinstance(run, FunctionRun)


@pytest.mark.integration
@pytest.mark.slow
def test_function_class_get_worker(
    tabsserver_connection, testing_collection_with_table
):
    function_name = tabsserver_connection.list_functions(testing_collection_with_table)[
        0
    ].name
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
