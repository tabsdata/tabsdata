#
# Copyright 2024 Tabs Data Inc.
#

import datetime
import logging
import os
import time

import polars as pl
import pytest
from tests_tabsdata.conftest import ABSOLUTE_TEST_FOLDER_LOCATION, API_SERVER_URL

from tabsdata.api.api_server import BASE_API_URL, APIServerError
from tabsdata.api.tabsdata_server import (
    Collection,
    Commit,
    DataVersion,
    ExecutionPlan,
    Function,
    ServerStatus,
    Table,
    TabsdataServer,
    Transaction,
    User,
    Worker,
    convert_timestamp_to_string,
)

logger = logging.getLogger(__name__)
logger.setLevel(logging.DEBUG)


@pytest.mark.integration
@pytest.mark.requires_internet
def test_tabsdata_server_create():
    tabsdata_server = TabsdataServer(API_SERVER_URL, "admin", "tabsdata")
    real_url = f"http://{API_SERVER_URL}{BASE_API_URL}"
    assert tabsdata_server.connection.url == real_url


@pytest.mark.integration
@pytest.mark.requires_internet
def test_tabsdata_server_users_list(tabsserver_connection):
    users = tabsserver_connection.users
    assert isinstance(users, list)
    assert all(isinstance(user, User) for user in users)


@pytest.mark.integration
@pytest.mark.requires_internet
def test_tabsdata_server_user_create(tabsserver_connection):
    try:
        tabsserver_connection.user_create(
            name="test_tabsdata_server_user_create",
            password="test_tabsdata_server_user_create_password",
            full_name="Test User",
            email="test_tabsdata_server_user_create_email",
        )
        users = tabsserver_connection.users
        assert any(user.name == "test_tabsdata_server_user_create" for user in users)
    finally:
        tabsserver_connection.user_delete("test_tabsdata_server_user_create")


@pytest.mark.integration
@pytest.mark.requires_internet
def test_tabsdata_server_user_get(tabsserver_connection):
    try:
        tabsserver_connection.user_create(
            name="test_tabsdata_server_user_get",
            password="test_tabsdata_server_user_get_password",
            full_name="Test User",
            email="test_tabsdata_server_user_get_email",
        )
        users = tabsserver_connection.users
        assert any(user.name == "test_tabsdata_server_user_get" for user in users)
        user = tabsserver_connection.user_get("test_tabsdata_server_user_get")
        assert user.name == "test_tabsdata_server_user_get"
    finally:
        tabsserver_connection.user_delete("test_tabsdata_server_user_get")


@pytest.mark.integration
@pytest.mark.requires_internet
def test_tabsdata_server_user_update(tabsserver_connection):
    try:
        tabsserver_connection.user_create(
            name="test_tabsdata_server_user_update",
            password="test_tabsdata_server_user_update_password",
            full_name="Test User",
            email="test_tabsdata_server_user_update_email",
        )
        users = tabsserver_connection.users
        assert any(user.name == "test_tabsdata_server_user_update" for user in users)
        new_full_name = "test_tabsdata_server_user_update_new"
        new_email = "test_tabsdata_server_user_update_new_email"
        tabsserver_connection.user_update(
            "test_tabsdata_server_user_update",
            full_name=new_full_name,
            email=new_email,
            enabled=False,
        )
        user = tabsserver_connection.user_get("test_tabsdata_server_user_update")
        assert user.name == "test_tabsdata_server_user_update"
        assert user.full_name == new_full_name
        assert user.email == new_email
        assert user.enabled is False
    finally:
        tabsserver_connection.user_delete("test_tabsdata_server_user_update")


@pytest.mark.integration
@pytest.mark.requires_internet
def test_function_get(tabsserver_connection):
    try:
        tabsserver_connection.collection_create(
            name="test_function_get_collection",
            description="test_collection_description",
        )
        tabsserver_connection.function_create(
            collection_name="test_function_get_collection",
            description="test_function_get_description",
            function_path=(
                f"{os.path.join(ABSOLUTE_TEST_FOLDER_LOCATION, "testing_resources",
                                "test_input_plugin", "example.py")}::input_plugin"
            ),
        )
        function = tabsserver_connection.function_get(
            "test_function_get_collection", "test_input_plugin"
        )
        assert function.name == "test_input_plugin"
    finally:
        tabsserver_connection.function_delete(
            "test_function_get_collection", "test_input_plugin"
        )
        # TODO: Uncomment this line when the collection_delete method is implemented
        # tabsserver_connection.collection_delete("test_function_get_collection")


@pytest.mark.integration
@pytest.mark.requires_internet
def test_function_list_history(tabsserver_connection):
    try:
        tabsserver_connection.collection_create(
            name="test_function_list_history_collection",
            description="test_collection_description",
        )
        tabsserver_connection.function_create(
            collection_name="test_function_list_history_collection",
            description="test_function_list_history_description",
            function_path=(
                f"{os.path.join(ABSOLUTE_TEST_FOLDER_LOCATION, "testing_resources",
                                "test_input_plugin", "example.py")}::input_plugin"
            ),
        )
        functions = tabsserver_connection.function_list_history(
            "test_function_list_history_collection", "test_input_plugin"
        )
        assert isinstance(functions, list)
        assert all(isinstance(function, Function) for function in functions)
    finally:
        tabsserver_connection.function_delete(
            "test_function_list_history", "test_input_plugin"
        )
        # TODO: Uncomment this line when the collection_delete method is implemented
        # tabsserver_connection.collection_delete("test_function_get_collection")


@pytest.mark.integration
@pytest.mark.requires_internet
def test_function_trigger(tabsserver_connection):
    try:
        tabsserver_connection.collection_create(
            name="test_function_trigger_collection",
            description="test_collection_description",
        )
        tabsserver_connection.function_create(
            collection_name="test_function_trigger_collection",
            description="test_function_trigger_description",
            function_path=(
                f"{os.path.join(ABSOLUTE_TEST_FOLDER_LOCATION, "testing_resources",
                                "test_input_plugin", "example.py")}::input_plugin"
            ),
        )
        response = tabsserver_connection.function_trigger(
            "test_function_trigger_collection", "test_input_plugin"
        )
        assert response.status_code == 201
    finally:
        tabsserver_connection.function_delete(
            "test_function_trigger_collection", "test_input_plugin"
        )
        # TODO: Uncomment this line when the collection_delete method is implemented
        # tabsserver_connection.collection_delete("test_function_trigger_collection")


@pytest.mark.integration
@pytest.mark.requires_internet
def test_function_trigger_execution_plan_name(tabsserver_connection):
    try:
        tabsserver_connection.collection_create(
            name="test_function_trigger_execution_plan_name_collection",
            description="test_collection_description",
        )
        tabsserver_connection.function_create(
            collection_name="test_function_trigger_execution_plan_name_collection",
            description="test_function_trigger_execution_plan_name_description",
            function_path=(
                f"{os.path.join(ABSOLUTE_TEST_FOLDER_LOCATION, "testing_resources",
                                "test_input_plugin", "example.py")}::input_plugin"
            ),
        )
        response = tabsserver_connection.function_trigger(
            "test_function_trigger_execution_plan_name_collection",
            "test_input_plugin",
            execution_plan_name="test_execution_plan_name",
        )
        assert response.status_code == 201
        assert response.json()["data"]["name"] == "test_execution_plan_name"
    finally:
        tabsserver_connection.function_delete(
            "test_function_trigger_execution_plan_name_collection", "test_input_plugin"
        )
        # TODO: Uncomment this line when the collection_delete method is implemented
        # tabsserver_connection.collection_delete("test_function_trigger_execution_plan_name_collection")


@pytest.mark.integration
@pytest.mark.requires_internet
def test_function_create(tabsserver_connection):
    try:
        tabsserver_connection.collection_create(
            name="test_function_create_collection",
            description="test_collection_description",
        )
        tabsserver_connection.function_create(
            collection_name="test_function_create_collection",
            description="test_function_create_description",
            function_path=(
                f"{os.path.join(ABSOLUTE_TEST_FOLDER_LOCATION, "testing_resources",
                                "test_input_plugin", "example.py")}::input_plugin"
            ),
        )
        functions = tabsserver_connection.collection_list_functions(
            "test_function_create_collection"
        )
        assert any(function.name == "test_input_plugin" for function in functions)
    finally:
        tabsserver_connection.function_delete(
            "test_function_create_collection", "test_input_plugin"
        )
        # TODO: Uncomment this line when the collection_delete method is implemented
        # tabsserver_connection.collection_delete("test_function_create_collection")


@pytest.mark.integration
@pytest.mark.requires_internet
def test_function_update(tabsserver_connection):
    try:
        tabsserver_connection.collection_create(
            name="test_function_update_server_collection",
            description="test_collection_description",
        )
        tabsserver_connection.function_create(
            collection_name="test_function_update_server_collection",
            description="test_function_update_server_description",
            function_path=(
                f"{os.path.join(ABSOLUTE_TEST_FOLDER_LOCATION, "testing_resources",
                                "test_input_plugin", "example.py")}::input_plugin"
            ),
        )
        functions = tabsserver_connection.collection_list_functions(
            "test_function_update_server_collection"
        )
        assert any(function.name == "test_input_plugin" for function in functions)
        new_description = "test_function_update_server_new_description"
        new_function_path = (
            f"{os.path.join(ABSOLUTE_TEST_FOLDER_LOCATION, 'testing_resources',
                            'test_input_file_csv_modified_format', 'example.py')}"
            "::input_file_csv_modified_format"
        )
        tabsserver_connection.function_update(
            "test_function_update_server_collection",
            "test_input_plugin",
            description=new_description,
            function_path=new_function_path,
        )
        functions = tabsserver_connection.collection_list_functions(
            "test_function_update_server_collection"
        )
        assert len(functions) == 1
        assert functions[0].name == "input_file_csv_modified_format"

    finally:
        tabsserver_connection.function_delete(
            "test_function_update_server_collection",
            "input_file_csv_modified_format",
        )
        # TODO: Uncomment this line when the collection_delete method is implemented
        # tabsserver_connection.collection_delete("test_function_update_collection")


@pytest.mark.integration
@pytest.mark.requires_internet
@pytest.mark.skip(
    reason=(
        "Not working due to a bug in the backend. function delete is "
        "not working properly."
    )
)
def test_function_delete(tabsserver_connection):
    try:
        tabsserver_connection.collection_create(
            name="test_function_delete_collection",
            description="test_collection_description",
        )
        tabsserver_connection.function_create(
            collection_name="test_function_delete_collection",
            description="test_function_delete_description",
            function_path=(
                f"{os.path.join(ABSOLUTE_TEST_FOLDER_LOCATION, "testing_resources",
                                "test_input_plugin", "example.py")}::input_plugin"
            ),
        )
        functions = tabsserver_connection.collection_list_functions(
            "test_function_delete_collection"
        )
        assert any(function.name == "test_input_plugin" for function in functions)
        tabsserver_connection.function_delete(
            "test_function_create_collection", "test_input_plugin"
        )
        functions = tabsserver_connection.collection_list_functions(
            "test_function_delete_collection"
        )
        assert not any(function.name == "test_input_plugin" for function in functions)
    finally:
        tabsserver_connection.function_delete(
            "test_function_create_collection", "test_input_plugin"
        )
        # TODO: Uncomment this line when the collection_delete method is implemented
        # tabsserver_connection.collection_delete("test_function_create_collection")


@pytest.mark.integration
@pytest.mark.requires_internet
def test_tabsdata_server_collection_list(tabsserver_connection):
    collections = tabsserver_connection.collections
    assert isinstance(collections, list)
    assert all(isinstance(collection, Collection) for collection in collections)


@pytest.mark.integration
@pytest.mark.requires_internet
def test_tabsdata_server_collection_create(tabsserver_connection):
    try:
        tabsserver_connection.collection_create(
            name="test_tabsdata_server_collection_create",
            description="test_tabsdata_server_collection_create_description",
        )
        collections = tabsserver_connection.collections
        assert any(
            collection.name == "test_tabsdata_server_collection_create"
            for collection in collections
        )
    finally:
        tabsserver_connection.collection_delete(
            "test_tabsdata_server_collection_create"
        )


@pytest.mark.integration
@pytest.mark.requires_internet
def test_tabsdata_server_collection_get(tabsserver_connection):
    try:
        tabsserver_connection.collection_create(
            name="test_tabsdata_server_collection_get",
            description="test_tabsdata_server_collection_get_description",
        )
        collections = tabsserver_connection.collections
        assert any(
            collection.name == "test_tabsdata_server_collection_get"
            for collection in collections
        )
        collection = tabsserver_connection.collection_get(
            "test_tabsdata_server_collection_get"
        )
        assert collection.name == "test_tabsdata_server_collection_get"
    finally:
        tabsserver_connection.collection_delete("test_tabsdata_server_collection_get")


@pytest.mark.integration
@pytest.mark.requires_internet
def test_tabsdata_server_collection_update(tabsserver_connection):
    try:
        tabsserver_connection.collection_create(
            name="test_tabsdata_server_collection_update",
            description="test_tabsdata_server_collection_update_description",
        )
        collections = tabsserver_connection.collections
        assert any(
            collection.name == "test_tabsdata_server_collection_update"
            for collection in collections
        )
        new_description = "test_tabsdata_server_collection_update_new_description"
        tabsserver_connection.collection_update(
            "test_tabsdata_server_collection_update",
            new_description=new_description,
        )
        collection = tabsserver_connection.collection_get(
            "test_tabsdata_server_collection_update"
        )
        assert collection.name == "test_tabsdata_server_collection_update"
        assert collection.description == new_description
    finally:
        tabsserver_connection.collection_delete(
            "test_tabsdata_server_collection_update"
        )


@pytest.mark.integration
@pytest.mark.requires_internet
def test_tabsdata_server_status(tabsserver_connection):
    status = tabsserver_connection.status
    assert isinstance(status, ServerStatus)
    assert status.__repr__()
    assert status.__str__()
    assert isinstance(status.latency_as_nanos, int)
    assert isinstance(status.status, str)


def test_server_status_class():
    status = ServerStatus(status="test", latency_as_nanos=1)
    assert status.status == "test"
    assert status.latency_as_nanos == 1
    assert status.__repr__()
    assert status.__str__()
    assert status == status
    assert status != ServerStatus(status="test2", latency_as_nanos=1)
    assert status != "test"


def test_convert_timestamp_to_string():
    timestamp = 1732723413266
    assert convert_timestamp_to_string(timestamp) == "2024-11-27T16:03:33Z"


@pytest.mark.integration
@pytest.mark.requires_internet
def test_tabsdata_server_execution_plans_list(tabsserver_connection):
    execution_plans = tabsserver_connection.execution_plans
    assert isinstance(execution_plans, list)
    assert all(isinstance(user, ExecutionPlan) for user in execution_plans)


@pytest.mark.integration
@pytest.mark.requires_internet
def test_table_list(tabsserver_connection, testing_collection_with_table):
    tables = tabsserver_connection.table_list(
        collection_name=testing_collection_with_table,
    )
    assert tables
    assert isinstance(tables, list)
    assert all(isinstance(table, Table) for table in tables)


@pytest.mark.integration
@pytest.mark.requires_internet
def test_table_download(tabsserver_connection, tmp_path, testing_collection_with_table):
    destination_file = os.path.join(
        tmp_path, "test_table_download_collection_output.parquet"
    )
    tabsserver_connection.table_download(
        collection_name=testing_collection_with_table,
        table_name="output",
        destination_file=destination_file,
    )

    assert os.path.exists(destination_file)


@pytest.mark.integration
@pytest.mark.requires_internet
def test_table_sample(tabsserver_connection, testing_collection_with_table):
    table = tabsserver_connection.table_sample(
        collection_name=testing_collection_with_table,
        table_name="output",
    )
    assert isinstance(table, pl.DataFrame)


@pytest.mark.integration
@pytest.mark.requires_internet
def test_table_get_schema(tabsserver_connection, testing_collection_with_table):
    schema = tabsserver_connection.table_get_schema(
        collection_name=testing_collection_with_table,
        table_name="output",
    )
    assert schema


@pytest.mark.integration
@pytest.mark.requires_internet
def test_tabsdata_server_transaction_list(tabsserver_connection):
    transactions = tabsserver_connection.transactions
    assert isinstance(transactions, list)
    assert all(isinstance(transaction, Transaction) for transaction in transactions)


@pytest.mark.integration
@pytest.mark.requires_internet
def test_table_download_with_version(
    tabsserver_connection, tmp_path, testing_collection_with_table
):
    destination_file = os.path.join(
        tmp_path, "test_table_download_with_version_collection_output.parquet"
    )
    tabsserver_connection.table_download(
        collection_name=testing_collection_with_table,
        table_name="output",
        version="HEAD",
        destination_file=destination_file,
    )
    assert os.path.exists(destination_file)


@pytest.mark.integration
@pytest.mark.requires_internet
def test_table_sample_with_version(
    tabsserver_connection, testing_collection_with_table
):
    table = tabsserver_connection.table_sample(
        collection_name=testing_collection_with_table,
        table_name="output",
        version="HEAD",
    )
    assert isinstance(table, pl.DataFrame)


@pytest.mark.integration
@pytest.mark.requires_internet
def test_table_get_schema_with_version(
    tabsserver_connection, testing_collection_with_table
):
    schema = tabsserver_connection.table_get_schema(
        collection_name=testing_collection_with_table,
        table_name="output",
        version="HEAD",
    )
    assert schema


@pytest.mark.integration
@pytest.mark.requires_internet
def test_table_download_with_commit(
    tabsserver_connection, tmp_path, testing_collection_with_table
):
    destination_file = os.path.join(
        tmp_path, "test_table_download_with_commit_collection_output.parquet"
    )
    commit = tabsserver_connection.commits[0].id
    tabsserver_connection.table_download(
        collection_name=testing_collection_with_table,
        table_name="output",
        destination_file=destination_file,
        commit=commit,
    )
    assert os.path.exists(destination_file)


@pytest.mark.integration
@pytest.mark.requires_internet
def test_table_sample_with_commit(tabsserver_connection, testing_collection_with_table):
    commit = tabsserver_connection.commits[0].id
    table = tabsserver_connection.table_sample(
        collection_name=testing_collection_with_table,
        table_name="output",
        commit=commit,
    )
    assert isinstance(table, pl.DataFrame)


@pytest.mark.integration
@pytest.mark.requires_internet
def test_table_get_schema_with_commit(
    tabsserver_connection, testing_collection_with_table
):
    commit = tabsserver_connection.commits[0].id
    schema = tabsserver_connection.table_get_schema(
        collection_name=testing_collection_with_table,
        table_name="output",
        commit=commit,
    )
    assert schema


@pytest.mark.integration
@pytest.mark.requires_internet
def test_table_download_with_wrong_version(
    tabsserver_connection, tmp_path, testing_collection_with_table
):
    destination_file = os.path.join(
        tmp_path, "test_table_download_with_wrong_version_collection_output.parquet"
    )
    with pytest.raises(APIServerError):
        tabsserver_connection.table_download(
            collection_name=testing_collection_with_table,
            table_name="output",
            version="DOESNTEXIST",
            destination_file=destination_file,
        )


@pytest.mark.integration
@pytest.mark.requires_internet
def test_table_sample_with_wrong_version(
    tabsserver_connection, testing_collection_with_table
):
    with pytest.raises(APIServerError):
        tabsserver_connection.table_sample(
            collection_name=testing_collection_with_table,
            table_name="output",
            version="DOESNTEXIST",
        )


@pytest.mark.integration
@pytest.mark.requires_internet
def test_table_get_schema_with_wrong_version(
    tabsserver_connection, testing_collection_with_table
):
    with pytest.raises(APIServerError):
        tabsserver_connection.table_get_schema(
            collection_name=testing_collection_with_table,
            table_name="output",
            version="DOESNTEXIST",
        )


@pytest.mark.integration
@pytest.mark.requires_internet
def test_table_download_with_wrong_commit(
    tabsserver_connection, tmp_path, testing_collection_with_table
):
    destination_file = os.path.join(
        tmp_path, "test_table_download_with_wrong_commit_collection_output.parquet"
    )
    with pytest.raises(APIServerError):
        tabsserver_connection.table_download(
            collection_name=testing_collection_with_table,
            table_name="output",
            destination_file=destination_file,
            commit="DOESNTEXIST",
        )


@pytest.mark.integration
@pytest.mark.requires_internet
def test_table_sample_with_wrong_commit(
    tabsserver_connection, testing_collection_with_table
):
    with pytest.raises(APIServerError):
        tabsserver_connection.table_sample(
            collection_name=testing_collection_with_table,
            table_name="output",
            commit="DOESNTEXIST",
        )


@pytest.mark.integration
@pytest.mark.requires_internet
def test_table_get_schema_with_wrong_commit(
    tabsserver_connection, testing_collection_with_table
):
    with pytest.raises(APIServerError):
        tabsserver_connection.table_get_schema(
            collection_name=testing_collection_with_table,
            table_name="output",
            commit="DOESNTEXIST",
        )


@pytest.mark.integration
@pytest.mark.requires_internet
def test_table_download_with_time(
    tabsserver_connection, tmp_path, testing_collection_with_table
):
    destination_file = os.path.join(
        tmp_path, "test_table_download_with_time_collection_output.parquet"
    )
    current_time = datetime.datetime.now(datetime.UTC)
    formatted_time = current_time.strftime("%Y-%m-%dT%H:%M:%S.%f")[:-3] + "Z"
    tabsserver_connection.table_download(
        collection_name=testing_collection_with_table,
        table_name="output",
        destination_file=destination_file,
        time=formatted_time,
    )
    assert os.path.exists(destination_file)


@pytest.mark.integration
@pytest.mark.requires_internet
def test_table_sample_with_time(tabsserver_connection, testing_collection_with_table):
    current_time = datetime.datetime.now(datetime.UTC)
    formatted_time = current_time.strftime("%Y-%m-%dT%H:%M:%S.%f")[:-3] + "Z"
    table = tabsserver_connection.table_sample(
        collection_name=testing_collection_with_table,
        table_name="output",
        time=formatted_time,
    )
    assert isinstance(table, pl.DataFrame)


@pytest.mark.integration
@pytest.mark.requires_internet
def test_table_get_schema_with_time(
    tabsserver_connection, testing_collection_with_table
):
    current_time = datetime.datetime.now(datetime.UTC)
    formatted_time = current_time.strftime("%Y-%m-%dT%H:%M:%S.%f")[:-3] + "Z"
    schema = tabsserver_connection.table_get_schema(
        collection_name=testing_collection_with_table,
        table_name="output",
        time=formatted_time,
    )
    assert schema


@pytest.mark.integration
@pytest.mark.requires_internet
def test_table_download_with_wrong_time(
    tabsserver_connection, tmp_path, testing_collection_with_table
):
    destination_file = os.path.join(
        tmp_path, "test_table_download_with_wrong_time_collection_output.parquet"
    )
    with pytest.raises(APIServerError):
        tabsserver_connection.table_download(
            collection_name=testing_collection_with_table,
            table_name="output",
            destination_file=destination_file,
            time="DOESNTEXIST",
        )


@pytest.mark.integration
@pytest.mark.requires_internet
def test_table_sample_with_wrong_time(
    tabsserver_connection, testing_collection_with_table
):
    with pytest.raises(APIServerError):
        tabsserver_connection.table_sample(
            collection_name=testing_collection_with_table,
            table_name="output",
            time="DOESNTEXIST",
        )


@pytest.mark.integration
@pytest.mark.requires_internet
def test_table_get_schema_with_wrong_time(
    tabsserver_connection, testing_collection_with_table
):
    with pytest.raises(APIServerError):
        tabsserver_connection.table_get_schema(
            collection_name=testing_collection_with_table,
            table_name="output",
            time="DOESNTEXIST",
        )


@pytest.mark.integration
@pytest.mark.requires_internet
def test_table_download_with_all_options_fails(
    tabsserver_connection, tmp_path, testing_collection_with_table
):
    commit = tabsserver_connection.commits[0].id
    destination_file = os.path.join(
        tmp_path, "test_table_download_with_time_collection_output.parquet"
    )
    current_time = datetime.datetime.now(datetime.UTC)
    formatted_time = current_time.strftime("%Y-%m-%dT%H:%M:%S.%f")[:-3] + "Z"
    with pytest.raises(APIServerError):
        tabsserver_connection.table_download(
            collection_name=testing_collection_with_table,
            table_name="output",
            destination_file=destination_file,
            time=formatted_time,
            commit=commit,
            version="HEAD",
        )


@pytest.mark.integration
@pytest.mark.requires_internet
def test_table_sample_with_all_options_fails(
    tabsserver_connection, testing_collection_with_table
):
    commit = tabsserver_connection.commits[0].id
    current_time = datetime.datetime.now(datetime.UTC)
    formatted_time = current_time.strftime("%Y-%m-%dT%H:%M:%S.%f")[:-3] + "Z"
    with pytest.raises(APIServerError):
        tabsserver_connection.table_sample(
            collection_name=testing_collection_with_table,
            table_name="output",
            time=formatted_time,
            commit=commit,
            version="HEAD",
        )


@pytest.mark.integration
@pytest.mark.requires_internet
def test_table_get_schema_with_all_options_fails(
    tabsserver_connection, testing_collection_with_table
):
    commit = tabsserver_connection.commits[0].id
    current_time = datetime.datetime.now(datetime.UTC)
    formatted_time = current_time.strftime("%Y-%m-%dT%H:%M:%S.%f")[:-3] + "Z"
    with pytest.raises(APIServerError):
        tabsserver_connection.table_get_schema(
            collection_name=testing_collection_with_table,
            table_name="output",
            time=formatted_time,
            commit=commit,
            version="HEAD",
        )


@pytest.mark.integration
@pytest.mark.requires_internet
def test_tabsdata_server_commit_list(
    testing_collection_with_table, tabsserver_connection
):
    commits = tabsserver_connection.commits
    assert isinstance(commits, list)
    assert all(isinstance(commit, Commit) for commit in commits)


@pytest.mark.integration
@pytest.mark.requires_internet
def test_commit_class():
    time_triggered = int(time.time())
    commit = Commit(
        id="test_id",
        execution_plan_id="test_execution_plan_id",
        triggered_on=time_triggered,
        ended_on=time_triggered,
        started_on=time_triggered,
        transaction_id="test_transaction_id",
        example_kwarg="example",
    )
    assert commit.id == "test_id"
    assert commit.execution_plan_id == "test_execution_plan_id"
    assert commit.triggered_on == time_triggered
    assert isinstance(commit.triggered_on_str, str)
    assert commit.transaction_id == "test_transaction_id"
    assert commit.started_on == time_triggered
    assert isinstance(commit.started_on_str, str)
    assert commit.ended_on == time_triggered
    assert isinstance(commit.ended_on_str, str)
    assert commit.kwargs == {"example_kwarg": "example"}
    assert commit.__repr__()
    assert commit.__str__()


@pytest.mark.integration
@pytest.mark.requires_internet
def test_dataversion_list(tabsserver_connection, testing_collection_with_table):
    function_name = tabsserver_connection.collection_list_functions(
        testing_collection_with_table
    )[0].name
    data_versions = tabsserver_connection.dataversion_list(
        testing_collection_with_table, function_name
    )
    assert data_versions
    assert isinstance(data_versions, list)
    assert all(isinstance(version, DataVersion) for version in data_versions)


@pytest.mark.integration
@pytest.mark.requires_internet
def test_worker_message_get(tabsserver_connection, testing_collection_with_table):
    transaction_id = None
    for element in tabsserver_connection.transactions:
        if element.status in ("Failed", "Published"):
            transaction_id = element.id
            break
    logger.debug(f"Transactions: {tabsserver_connection.transactions}")
    logger.debug(f"Transaction ID: {transaction_id}")
    assert transaction_id
    function = tabsserver_connection.collection_list_functions(
        testing_collection_with_table
    )[0]
    messages = tabsserver_connection.worker_list(by_function_id=function.id)
    logger.debug(f"Messages: {messages}")
    assert messages
    assert isinstance(messages, list)
    assert all(isinstance(message, Worker) for message in messages)
    message_id = messages[0].id
    logger.debug(f"Message ID: {message_id}")
    # Flakiness should be fixed now, comment if it starts failing again
    log = tabsserver_connection.worker_log(message_id)
    assert isinstance(log, str)


@pytest.mark.integration
@pytest.mark.requires_internet
def test_worker_messages_list_by_execution_plan_id(
    tabsserver_connection, testing_collection_with_table
):
    execution_plan_id = tabsserver_connection.execution_plans[0].id
    messages = tabsserver_connection.worker_list(by_execution_plan_id=execution_plan_id)
    assert isinstance(messages, list)
    assert all(isinstance(message, Worker) for message in messages)


@pytest.mark.integration
@pytest.mark.requires_internet
def test_worker_messages_list_by_transaction_id(
    tabsserver_connection, testing_collection_with_table
):
    transaction_id = None
    for element in tabsserver_connection.transactions:
        if element.status in ("Failed", "Published"):
            transaction_id = element.id
            break
    logger.debug(f"Transactions: {tabsserver_connection.transactions}")
    logger.debug(f"Transaction ID: {transaction_id}")
    assert transaction_id
    messages = tabsserver_connection.worker_list(by_transaction_id=transaction_id)
    assert messages
    assert isinstance(messages, list)
    assert all(isinstance(message, Worker) for message in messages)


@pytest.mark.integration
@pytest.mark.requires_internet
def test_worker_messages_list_by_function_id(
    tabsserver_connection, testing_collection_with_table
):
    function_name = tabsserver_connection.collection_list_functions(
        testing_collection_with_table
    )[0].name
    function_id = tabsserver_connection.function_get(
        testing_collection_with_table, function_name
    ).id
    messages = tabsserver_connection.worker_list(by_function_id=function_id)
    assert messages
    assert isinstance(messages, list)
    assert all(isinstance(message, Worker) for message in messages)


@pytest.mark.integration
@pytest.mark.requires_internet
def test_worker_messages_list_by_data_version_id(
    tabsserver_connection, testing_collection_with_table
):
    function_name = tabsserver_connection.collection_list_functions(
        testing_collection_with_table
    )[0].name
    data_version = tabsserver_connection.dataversion_list(
        testing_collection_with_table, function_name
    )[0].id
    messages = tabsserver_connection.worker_list(by_data_version_id=data_version)
    assert messages
    assert isinstance(messages, list)
    assert all(isinstance(message, Worker) for message in messages)


@pytest.mark.integration
@pytest.mark.requires_internet
def test_worker_messages_list_by_all_options_fails(
    tabsserver_connection, testing_collection_with_table
):
    execution_plan_id = tabsserver_connection.execution_plans[0].id
    function_name = tabsserver_connection.collection_list_functions(
        testing_collection_with_table
    )[0].name
    data_version = tabsserver_connection.dataversion_list(
        testing_collection_with_table, function_name
    )[0].id
    function_name = tabsserver_connection.collection_list_functions(
        testing_collection_with_table
    )[0].name
    function_id = tabsserver_connection.function_get(
        testing_collection_with_table, function_name
    ).id
    transaction_id = None
    for element in tabsserver_connection.transactions:
        if element.status in ("Failed", "Published"):
            transaction_id = element.id
            break
    logger.debug(f"Transactions: {tabsserver_connection.transactions}")
    logger.debug(f"Transaction ID: {transaction_id}")
    assert transaction_id
    with pytest.raises(APIServerError):
        tabsserver_connection.worker_list(
            by_execution_plan_id=execution_plan_id,
            by_function_id=function_id,
            by_data_version_id=data_version,
            by_transaction_id=transaction_id,
        )


@pytest.mark.integration
@pytest.mark.requires_internet
def test_worker_messages_list_no_options_fails(
    tabsserver_connection, testing_collection_with_table
):
    with pytest.raises(APIServerError):
        tabsserver_connection.worker_list()


@pytest.mark.integration
@pytest.mark.requires_internet
def test_execution_plan_read(tabsserver_connection, testing_collection_with_table):
    execution_plans = tabsserver_connection.execution_plans
    assert execution_plans
    assert tabsserver_connection.execution_plan_read(execution_plans[0].id)
