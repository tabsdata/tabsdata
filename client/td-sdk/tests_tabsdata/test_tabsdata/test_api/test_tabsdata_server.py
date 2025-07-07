#
# Copyright 2024 Tabs Data Inc.
#

import datetime
import logging
import os
import time
import types
import uuid

import polars as pl
import pytest
from tests_tabsdata.conftest import (
    ABSOLUTE_TEST_FOLDER_LOCATION,
    APISERVER_URL,
    LOCAL_PACKAGES_LIST,
)

from tabsdata.api.apiserver import BASE_API_URL, APIServerError
from tabsdata.api.status_utils.transaction import TRANSACTION_FINAL_STATUSES
from tabsdata.api.tabsdata_server import (
    Collection,
    DataVersion,
    Execution,
    Function,
    FunctionRun,
    Role,
    ServerStatus,
    Table,
    TabsdataServer,
    Transaction,
    User,
    Worker,
    convert_timestamp_to_string,
    top_and_convert_to_timestamp,
)

# noinspection PyUnresolvedReferences
from . import pytestmark  # noqa: F401

logger = logging.getLogger(__name__)
logger.setLevel(logging.DEBUG)


@pytest.mark.integration
@pytest.mark.requires_internet
def test_tabsdata_server_create():
    tabsdata_server = TabsdataServer(APISERVER_URL, "admin", "tabsdata")
    real_url = f"http://{APISERVER_URL}{BASE_API_URL}"
    assert tabsdata_server.connection.url == real_url


@pytest.mark.integration
@pytest.mark.requires_internet
def test_tabsdata_server_auth_info(tabsserver_connection):
    assert tabsserver_connection.auth_info


@pytest.mark.integration
@pytest.mark.requires_internet
def test_tabsdata_server_password_change(tabsserver_connection):
    user_name = "test_tabsdata_server_password_change_user"
    password = "test_tabsdata_server_password_change_password"
    try:
        tabsserver_connection.create_user(user_name, password)
        assert tabsserver_connection.get_user(user_name).name == user_name
        new_password = "test_tabsdata_server_password_change_new_password"
        tabsserver_connection.password_change(user_name, password, new_password)
        # Re-authenticate with the new password
        assert TabsdataServer(APISERVER_URL, user_name, new_password)
        with pytest.raises(APIServerError):
            # Try to authenticate with the old password
            TabsdataServer(APISERVER_URL, user_name, password)
    finally:
        tabsserver_connection.delete_user(user_name, raise_for_status=False)


@pytest.mark.integration
@pytest.mark.requires_internet
def test_tabsdata_server_users_list(tabsserver_connection):
    users = tabsserver_connection.users
    assert isinstance(users, list)
    assert all(isinstance(user, User) for user in users)


@pytest.mark.integration
@pytest.mark.requires_internet
def test_tabsdata_server_create_user(tabsserver_connection):
    try:
        tabsserver_connection.create_user(
            name="test_tabsdata_server_user_create",
            password="test_tabsdata_server_user_create_password",
            full_name="Test User",
            email="test_tabsdata_server_user_create_email@tabsdata.com",
        )
        users = tabsserver_connection.users
        assert any(user.name == "test_tabsdata_server_user_create" for user in users)
    finally:
        tabsserver_connection.delete_user(
            "test_tabsdata_server_user_create", raise_for_status=False
        )


@pytest.mark.integration
@pytest.mark.requires_internet
@pytest.mark.slow
def test_tabsdata_server_list_users(tabsserver_connection):
    amount = 101
    try:
        for i in range(amount):
            tabsserver_connection.create_user(
                name=f"test_tabsdata_server_list_users_no_generator_{i}",
                password=f"test_tabsdata_server_list_users_no_generator_password_{i}",
                full_name=f"Test User {i}",
                email=(
                    "test_tabsdata_server_list_users_no_generator_email"
                    f"_{i}@tabsdata.com"
                ),
            )
        users = tabsserver_connection.list_users(
            filter="name:eq:test_tabsdata_server_list_users_no_generator_0"
        )
        assert len(users) == 1
        users = tabsserver_connection.list_users(
            filter="name:lk:test_tabsdata_server_list_users_no_generator_*"
        )
        assert len(users) == amount
    finally:
        for i in range(amount):
            tabsserver_connection.delete_user(
                f"test_tabsdata_server_list_users_no_generator_{i}",
                raise_for_status=False,
            )


@pytest.mark.integration
@pytest.mark.requires_internet
@pytest.mark.slow
def test_tabsdata_server_list_users_generator(tabsserver_connection):
    amount = 101
    try:
        for i in range(amount):
            tabsserver_connection.create_user(
                name=f"test_tabsdata_server_list_users_generator_{i}",
                password=f"test_tabsdata_server_list_users_generator_password_{i}",
                full_name=f"Test User {i}",
                email=(
                    f"test_tabsdata_server_list_users_generator_email_{i}@tabsdata.com"
                ),
            )
        users = tabsserver_connection.list_users_generator(
            filter="name:eq:test_tabsdata_server_list_users_generator_0"
        )
        assert isinstance(users, types.GeneratorType)
        materialized_users = list(users)
        assert len(materialized_users) == 1
        users = tabsserver_connection.list_users_generator(
            filter="name:lk:test_tabsdata_server_list_users_generator_*"
        )
        assert isinstance(users, types.GeneratorType)
        materialized_users = list(users)
        assert len(materialized_users) == amount
    finally:
        for i in range(amount):
            tabsserver_connection.delete_user(
                f"test_tabsdata_server_list_users_generator_{i}", raise_for_status=False
            )


@pytest.mark.integration
@pytest.mark.requires_internet
def test_tabsdata_server_get_user(tabsserver_connection):
    try:
        tabsserver_connection.create_user(
            name="test_tabsdata_server_user_get",
            password="test_tabsdata_server_user_get_password",
            full_name="Test User",
            email="test_tabsdata_server_user_get_email@tabsdata.com",
        )
        users = tabsserver_connection.users
        assert any(user.name == "test_tabsdata_server_user_get" for user in users)
        user = tabsserver_connection.get_user("test_tabsdata_server_user_get")
        assert user.name == "test_tabsdata_server_user_get"
    finally:
        tabsserver_connection.delete_user(
            "test_tabsdata_server_user_get", raise_for_status=False
        )


@pytest.mark.integration
@pytest.mark.requires_internet
def test_tabsdata_server_update_user(tabsserver_connection):
    try:
        tabsserver_connection.create_user(
            name="test_tabsdata_server_user_update",
            password="test_tabsdata_server_user_update_password",
            full_name="Test User",
            email="test_tabsdata_server_user_update_email@tabsdata.com",
        )
        users = tabsserver_connection.users
        assert any(user.name == "test_tabsdata_server_user_update" for user in users)
        new_full_name = "test_tabsdata_server_user_update_new"
        new_email = "test_tabsdata_server_user_update_new_email@tabsdata.com"
        tabsserver_connection.update_user(
            "test_tabsdata_server_user_update",
            full_name=new_full_name,
            email=new_email,
            enabled=False,
        )
        user = tabsserver_connection.get_user("test_tabsdata_server_user_update")
        assert user.name == "test_tabsdata_server_user_update"
        assert user.full_name == new_full_name
        assert user.email == new_email
        assert user.enabled is False
    finally:
        tabsserver_connection.delete_user(
            "test_tabsdata_server_user_update", raise_for_status=False
        )


@pytest.mark.integration
@pytest.mark.requires_internet
def test_function_get(tabsserver_connection):
    try:
        tabsserver_connection.create_collection(
            name="test_function_get_collection",
            description="test_collection_description",
        )
        tabsserver_connection.register_function(
            collection_name="test_function_get_collection",
            description="test_function_get_description",
            function_path=(
                f"{os.path.join(ABSOLUTE_TEST_FOLDER_LOCATION, "testing_resources",
                                "test_input_plugin", "example.py")}::input_plugin"
            ),
            local_packages=LOCAL_PACKAGES_LIST,
        )
        function = tabsserver_connection.function_get(
            "test_function_get_collection", "test_input_plugin"
        )
        assert function.name == "test_input_plugin"
    finally:
        tabsserver_connection.delete_function(
            "test_function_get_collection",
            "test_input_plugin",
            raise_for_status=False,
        )
        tabsserver_connection.delete_collection(
            "test_function_get_collection", raise_for_status=False
        )


@pytest.mark.integration
@pytest.mark.requires_internet
def test_function_list_history(tabsserver_connection):
    try:
        tabsserver_connection.create_collection(
            name="test_function_list_history_collection",
            description="test_collection_description",
        )
        tabsserver_connection.register_function(
            collection_name="test_function_list_history_collection",
            description="test_function_list_history_description",
            function_path=(
                f"{os.path.join(ABSOLUTE_TEST_FOLDER_LOCATION, "testing_resources",
                                "test_input_plugin", "example.py")}::input_plugin"
            ),
            local_packages=LOCAL_PACKAGES_LIST,
        )
        functions = tabsserver_connection.list_function_history(
            "test_function_list_history_collection", "test_input_plugin"
        )
        assert isinstance(functions, list)
        assert all(isinstance(function, Function) for function in functions)
    finally:
        tabsserver_connection.delete_function(
            "test_function_list_history",
            "test_input_plugin",
            raise_for_status=False,
        )
        tabsserver_connection.delete_collection(
            "test_function_list_history_collection", raise_for_status=False
        )


@pytest.mark.integration
@pytest.mark.requires_internet
def test_function_trigger(tabsserver_connection):
    try:
        tabsserver_connection.create_collection(
            name="test_function_trigger_collection",
            description="test_collection_description",
        )
        tabsserver_connection.register_function(
            collection_name="test_function_trigger_collection",
            description="test_function_trigger_description",
            function_path=(
                f"{os.path.join(ABSOLUTE_TEST_FOLDER_LOCATION, "testing_resources",
                                "test_input_plugin", "example.py")}::input_plugin"
            ),
            local_packages=LOCAL_PACKAGES_LIST,
        )
        response = tabsserver_connection.trigger_function(
            "test_function_trigger_collection", "test_input_plugin"
        )
        assert isinstance(response, Execution)
    finally:
        tabsserver_connection.delete_function(
            "test_function_trigger_collection",
            "test_input_plugin",
            raise_for_status=False,
        )
        tabsserver_connection.delete_collection(
            "test_function_trigger_collection", raise_for_status=False
        )


@pytest.mark.integration
@pytest.mark.requires_internet
def test_function_trigger_execution_name(tabsserver_connection):
    try:
        tabsserver_connection.create_collection(
            name="test_function_trigger_execution_name_collection",
            description="test_collection_description",
        )
        tabsserver_connection.register_function(
            collection_name="test_function_trigger_execution_name_collection",
            description="test_function_trigger_execution_name_description",
            function_path=(
                f"{os.path.join(ABSOLUTE_TEST_FOLDER_LOCATION, "testing_resources",
                                "test_input_plugin", "example.py")}::input_plugin"
            ),
            local_packages=LOCAL_PACKAGES_LIST,
        )
        execution = tabsserver_connection.trigger_function(
            "test_function_trigger_execution_name_collection",
            "test_input_plugin",
            execution_name="test_execution_name",
        )
        assert execution
        assert execution.name == "test_execution_name"
    finally:
        tabsserver_connection.delete_function(
            "test_function_trigger_execution_name_collection",
            "test_input_plugin",
            raise_for_status=False,
        )
        tabsserver_connection.delete_collection(
            "test_function_trigger_execution_name_collection", raise_for_status=False
        )


@pytest.mark.integration
@pytest.mark.requires_internet
def test_function_register(tabsserver_connection):
    try:
        tabsserver_connection.create_collection(
            name="test_function_register_collection",
            description="test_collection_description",
        )
        tabsserver_connection.register_function(
            collection_name="test_function_register_collection",
            description="test_function_register_description",
            function_path=(
                f"{os.path.join(ABSOLUTE_TEST_FOLDER_LOCATION, "testing_resources",
                                "test_input_plugin", "example.py")}::input_plugin"
            ),
            local_packages=LOCAL_PACKAGES_LIST,
        )
        functions = tabsserver_connection.list_functions(
            "test_function_register_collection"
        )
        assert any(function.name == "test_input_plugin" for function in functions)
    finally:
        tabsserver_connection.delete_function(
            "test_function_register_collection",
            "test_input_plugin",
            raise_for_status=False,
        )
        tabsserver_connection.delete_collection(
            "test_function_register_collection", raise_for_status=False
        )


@pytest.mark.integration
@pytest.mark.requires_internet
def test_function_update(tabsserver_connection):
    try:
        tabsserver_connection.create_collection(
            name="test_function_update_server_collection",
            description="test_collection_description",
        )
        tabsserver_connection.register_function(
            collection_name="test_function_update_server_collection",
            description="test_function_update_server_description",
            function_path=(
                f"{os.path.join(ABSOLUTE_TEST_FOLDER_LOCATION, "testing_resources",
                                "test_input_plugin", "example.py")}::input_plugin"
            ),
            local_packages=LOCAL_PACKAGES_LIST,
        )
        functions = tabsserver_connection.list_functions(
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
        functions = tabsserver_connection.list_functions(
            "test_function_update_server_collection"
        )
        assert len(functions) == 1
        assert functions[0].name == "input_file_csv_modified_format"

    finally:
        tabsserver_connection.delete_function(
            "test_function_update_server_collection",
            "input_file_csv_modified_format",
            raise_for_status=False,
        )
        tabsserver_connection.delete_collection(
            "test_function_update_server_collection", raise_for_status=False
        )


@pytest.mark.integration
@pytest.mark.requires_internet
def test_function_delete(tabsserver_connection):
    try:
        tabsserver_connection.create_collection(
            name="test_function_delete_collection",
            description="test_collection_description",
        )
        tabsserver_connection.register_function(
            collection_name="test_function_delete_collection",
            description="test_function_delete_description",
            function_path=(
                f"{os.path.join(ABSOLUTE_TEST_FOLDER_LOCATION, "testing_resources",
                                "test_input_plugin", "example.py")}::input_plugin"
            ),
            local_packages=LOCAL_PACKAGES_LIST,
        )
        functions = tabsserver_connection.list_functions(
            "test_function_delete_collection"
        )
        assert any(function.name == "test_input_plugin" for function in functions)
        tabsserver_connection.delete_function(
            "test_function_delete_collection", "test_input_plugin"
        )
        functions = tabsserver_connection.list_functions(
            "test_function_delete_collection"
        )
        assert not any(function.name == "test_input_plugin" for function in functions)
    finally:
        tabsserver_connection.delete_function(
            "test_function_delete_collection",
            "test_input_plugin",
            raise_for_status=False,
        )
        tabsserver_connection.delete_collection(
            "test_function_delete_collection", raise_for_status=False
        )


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
        tabsserver_connection.create_collection(
            name="test_tabsdata_server_collection_create",
            description="test_tabsdata_server_collection_create_description",
        )
        collections = tabsserver_connection.collections
        assert any(
            collection.name == "test_tabsdata_server_collection_create"
            for collection in collections
        )
    finally:
        tabsserver_connection.delete_collection(
            "test_tabsdata_server_collection_create"
        )


@pytest.mark.integration
@pytest.mark.requires_internet
def test_tabsdata_server_collection_get(tabsserver_connection):
    try:
        tabsserver_connection.create_collection(
            name="test_tabsdata_server_collection_get",
            description="test_tabsdata_server_collection_get_description",
        )
        collections = tabsserver_connection.collections
        assert any(
            collection.name == "test_tabsdata_server_collection_get"
            for collection in collections
        )
        collection = tabsserver_connection.get_collection(
            "test_tabsdata_server_collection_get"
        )
        assert collection.name == "test_tabsdata_server_collection_get"
    finally:
        tabsserver_connection.delete_collection("test_tabsdata_server_collection_get")


@pytest.mark.integration
@pytest.mark.requires_internet
@pytest.mark.slow
def test_tabsdata_server_list_collections(tabsserver_connection):
    amount = 101
    try:
        for i in range(amount):
            tabsserver_connection.create_collection(
                name=f"test_tabsdata_server_list_collections_no_generator_{i}"
            ),
        collections = tabsserver_connection.list_collections(
            filter="name:eq:test_tabsdata_server_list_collections_no_generator_0"
        )
        assert len(collections) == 1
        assert all(isinstance(collection, Collection) for collection in collections)
        collections = tabsserver_connection.list_collections(
            filter="name:lk:test_tabsdata_server_list_collections_no_generator_*"
        )
        assert len(collections) == amount
        assert all(isinstance(collection, Collection) for collection in collections)
    finally:
        for i in range(amount):
            tabsserver_connection.delete_collection(
                f"test_tabsdata_server_list_collections_no_generator_{i}",
                raise_for_status=False,
            )


@pytest.mark.integration
@pytest.mark.requires_internet
@pytest.mark.slow
def test_tabsdata_server_list_collections_generator(tabsserver_connection):
    amount = 101
    try:
        for i in range(amount):
            tabsserver_connection.create_collection(
                name=f"test_tabsdata_server_list_collections_generator_{i}",
            )
        collections = tabsserver_connection.list_collections_generator(
            filter="name:eq:test_tabsdata_server_list_collections_generator_0"
        )
        assert isinstance(collections, types.GeneratorType)
        materialized_collections = list(collections)
        assert len(materialized_collections) == 1
        assert all(
            isinstance(collection, Collection)
            for collection in materialized_collections
        )
        collections = tabsserver_connection.list_collections_generator(
            filter="name:lk:test_tabsdata_server_list_collections_generator_*"
        )
        assert isinstance(collections, types.GeneratorType)
        materialized_collections = list(collections)
        assert len(materialized_collections) == amount
        assert all(
            isinstance(collection, Collection)
            for collection in materialized_collections
        )
    finally:
        for i in range(amount):
            tabsserver_connection.delete_collection(
                f"test_tabsdata_server_list_collections_generator_{i}",
                raise_for_status=False,
            )


@pytest.mark.integration
@pytest.mark.requires_internet
def test_tabsdata_server_collection_update(tabsserver_connection):
    try:
        tabsserver_connection.create_collection(
            name="test_tabsdata_server_collection_update",
            description="test_tabsdata_server_collection_update_description",
        )
        collections = tabsserver_connection.collections
        assert any(
            collection.name == "test_tabsdata_server_collection_update"
            for collection in collections
        )
        new_description = "test_tabsdata_server_collection_update_new_description"
        tabsserver_connection.update_collection(
            "test_tabsdata_server_collection_update",
            new_description=new_description,
        )
        collection = tabsserver_connection.get_collection(
            "test_tabsdata_server_collection_update"
        )
        assert collection.name == "test_tabsdata_server_collection_update"
        assert collection.description == new_description
    finally:
        tabsserver_connection.delete_collection(
            "test_tabsdata_server_collection_update"
        )


@pytest.mark.integration
@pytest.mark.requires_internet
def test_tabsdata_server_role_list(tabsserver_connection):
    roles = tabsserver_connection.roles
    assert isinstance(roles, list)
    assert all(isinstance(role, Role) for role in roles)


@pytest.mark.integration
@pytest.mark.requires_internet
def test_tabsdata_server_role_create(tabsserver_connection):
    try:
        tabsserver_connection.create_role(
            name="test_tabsdata_server_role_create",
            description="test_tabsdata_server_role_create_description",
        )
        roles = tabsserver_connection.roles
        assert any(role.name == "test_tabsdata_server_role_create" for role in roles)
    finally:
        tabsserver_connection.delete_role(
            "test_tabsdata_server_role_create",
            raise_for_status=False,
        )


@pytest.mark.integration
@pytest.mark.requires_internet
def test_tabsdata_server_role_delete(tabsserver_connection):
    try:
        tabsserver_connection.create_role(
            name="test_tabsdata_server_role_delete",
            description="test_tabsdata_server_role_delete_description",
        )
        roles = tabsserver_connection.roles
        assert any(role.name == "test_tabsdata_server_role_delete" for role in roles)
        tabsserver_connection.delete_role(
            "test_tabsdata_server_role_delete",
            raise_for_status=True,
        )
        roles = tabsserver_connection.roles
        assert not any(
            role.name == "test_tabsdata_server_role_delete" for role in roles
        )
    finally:
        tabsserver_connection.delete_role(
            "test_tabsdata_server_role_delete",
            raise_for_status=False,
        )


@pytest.mark.integration
@pytest.mark.requires_internet
def test_tabsdata_server_role_get(tabsserver_connection):
    try:
        tabsserver_connection.create_role(
            name="test_tabsdata_server_role_get",
            description="test_tabsdata_server_role_get_description",
        )
        roles = tabsserver_connection.roles
        assert any(role.name == "test_tabsdata_server_role_get" for role in roles)
        role = tabsserver_connection.get_role("test_tabsdata_server_role_get")
        assert role.name == "test_tabsdata_server_role_get"
    finally:
        tabsserver_connection.delete_role(
            "test_tabsdata_server_role_get", raise_for_status=False
        )


@pytest.mark.integration
@pytest.mark.requires_internet
@pytest.mark.slow
def test_tabsdata_server_list_roles(tabsserver_connection):
    amount = 101
    try:
        for i in range(amount):
            tabsserver_connection.create_role(
                name=f"test_tabsdata_server_list_roles_no_generator_{i}"
            ),
        roles = tabsserver_connection.list_roles(
            filter="name:eq:test_tabsdata_server_list_roles_no_generator_0"
        )
        assert len(roles) == 1
        assert all(isinstance(role, Role) for role in roles)
        roles = tabsserver_connection.list_roles(
            filter="name:lk:test_tabsdata_server_list_roles_no_generator_*"
        )
        assert len(roles) == amount
        assert all(isinstance(role, Role) for role in roles)
    finally:
        for i in range(amount):
            tabsserver_connection.delete_role(
                f"test_tabsdata_server_list_roles_no_generator_{i}",
                raise_for_status=False,
            )


@pytest.mark.integration
@pytest.mark.requires_internet
@pytest.mark.slow
def test_tabsdata_server_list_roles_generator(tabsserver_connection):
    amount = 101
    try:
        for i in range(amount):
            tabsserver_connection.create_role(
                name=f"test_tabsdata_server_list_roles_generator_{i}",
            )
        roles = tabsserver_connection.list_roles_generator(
            filter="name:eq:test_tabsdata_server_list_roles_generator_0"
        )
        assert isinstance(roles, types.GeneratorType)
        materialized_roles = list(roles)
        assert len(materialized_roles) == 1
        assert all(isinstance(role, Role) for role in materialized_roles)
        roles = tabsserver_connection.list_roles_generator(
            filter="name:lk:test_tabsdata_server_list_roles_generator_*"
        )
        assert isinstance(roles, types.GeneratorType)
        materialized_roles = list(roles)
        assert len(materialized_roles) == amount
        assert all(isinstance(role, Role) for role in materialized_roles)
    finally:
        for i in range(amount):
            tabsserver_connection.delete_role(
                f"test_tabsdata_server_list_roles_generator_{i}",
                raise_for_status=False,
            )


@pytest.mark.integration
@pytest.mark.requires_internet
def test_tabsdata_server_role_update(tabsserver_connection):
    try:
        tabsserver_connection.create_role(
            name="test_tabsdata_server_role_update",
            description="test_tabsdata_server_role_update_description",
        )
        roles = tabsserver_connection.roles
        assert any(role.name == "test_tabsdata_server_role_update" for role in roles)
        new_description = "test_tabsdata_server_role_update_new_description"
        tabsserver_connection.update_role(
            "test_tabsdata_server_role_update",
            new_description=new_description,
        )
        role = tabsserver_connection.get_role("test_tabsdata_server_role_update")
        assert role.name == "test_tabsdata_server_role_update"
        assert role.description == new_description
    finally:
        tabsserver_connection.delete_role(
            "test_tabsdata_server_role_update",
            raise_for_status=False,
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
def test_tabsdata_server_executions_list(tabsserver_connection):
    executions = tabsserver_connection.executions
    assert isinstance(executions, list)
    assert all(isinstance(user, Execution) for user in executions)


@pytest.mark.integration
@pytest.mark.requires_internet
def test_tabsdata_server_table_list(
    tabsserver_connection, testing_collection_with_table
):
    tables = tabsserver_connection.list_tables(
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
    tabsserver_connection.download_table(
        collection_name=testing_collection_with_table,
        table_name="output",
        destination_file=destination_file,
    )

    assert os.path.exists(destination_file)


@pytest.mark.integration
@pytest.mark.requires_internet
def test_table_sample(tabsserver_connection, testing_collection_with_table):
    table = tabsserver_connection.sample_table(
        collection_name=testing_collection_with_table,
        table_name="output",
    )
    assert isinstance(table, pl.DataFrame)


@pytest.mark.integration
@pytest.mark.requires_internet
def test_table_get_schema(tabsserver_connection, testing_collection_with_table):
    schema = tabsserver_connection.get_table_schema(
        collection_name=testing_collection_with_table,
        table_name="output",
    )
    assert schema


@pytest.mark.integration
@pytest.mark.requires_internet
def test_table_download_at(
    tabsserver_connection, tmp_path, testing_collection_with_table
):
    destination_file = os.path.join(
        tmp_path, "test_table_download_at_collection_output.parquet"
    )
    epoch_ms = int(time.time() * 1000)  # Current time in milliseconds
    tabsserver_connection.download_table(
        collection_name=testing_collection_with_table,
        table_name="output",
        destination_file=destination_file,
        at=epoch_ms,
    )

    assert os.path.exists(destination_file)


@pytest.mark.integration
@pytest.mark.requires_internet
def test_table_sample(tabsserver_connection, testing_collection_with_table):
    epoch_ms = int(time.time() * 1000)  # Current time in milliseconds
    table = tabsserver_connection.sample_table(
        collection_name=testing_collection_with_table,
        table_name="output",
        at=epoch_ms,
    )
    assert isinstance(table, pl.DataFrame)


@pytest.mark.integration
@pytest.mark.requires_internet
def test_table_get_schema(tabsserver_connection, testing_collection_with_table):
    epoch_ms = int(time.time() * 1000)  # Current time in milliseconds
    schema = tabsserver_connection.get_table_schema(
        collection_name=testing_collection_with_table,
        table_name="output",
        at=epoch_ms,
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
def test_dataversion_list(tabsserver_connection, testing_collection_with_table):
    table_name = tabsserver_connection.list_tables(testing_collection_with_table)[
        0
    ].name
    data_versions = tabsserver_connection.list_dataversions(
        testing_collection_with_table, table_name
    )
    assert data_versions
    assert isinstance(data_versions, list)
    assert all(isinstance(version, DataVersion) for version in data_versions)


@pytest.mark.integration
@pytest.mark.requires_internet
def test_worker_message_get(tabsserver_connection, testing_collection_with_table):
    transaction_id = None
    for transaction in tabsserver_connection.transactions:
        if transaction.status in TRANSACTION_FINAL_STATUSES:
            transaction_id = transaction.id
            break
    logger.debug(f"Transactions: {tabsserver_connection.transactions}")
    logger.debug(f"Transaction ID: {transaction_id}")
    assert transaction_id
    function = tabsserver_connection.list_functions(testing_collection_with_table)[0]
    messages = tabsserver_connection.list_workers(
        filter=[
            f"function:eq:{function.name}",
            f"collection:eq:{testing_collection_with_table}",
        ]
    )
    logger.debug(f"Messages: {messages}")
    assert messages
    assert isinstance(messages, list)
    assert all(isinstance(message, Worker) for message in messages)
    message_id = messages[0].id
    logger.debug(f"Message ID: {message_id}")
    # Flakiness should be fixed now, comment if it starts failing again
    log = tabsserver_connection.get_worker_log(message_id)
    assert isinstance(log, str)


@pytest.mark.integration
@pytest.mark.requires_internet
def test_workers_property(tabsserver_connection, testing_collection_with_table):
    workers = tabsserver_connection.workers
    assert isinstance(workers, list)
    assert all(isinstance(worker, Worker) for worker in workers)


@pytest.mark.integration
@pytest.mark.requires_internet
def test_worker_messages_list_by_execution_id(
    tabsserver_connection, testing_collection_with_table
):
    execution_id = tabsserver_connection.executions[0].id
    messages = tabsserver_connection.list_workers(
        filter=[f"execution_id:eq:{execution_id}"]
    )
    assert isinstance(messages, list)
    assert all(isinstance(message, Worker) for message in messages)


@pytest.mark.integration
@pytest.mark.requires_internet
def test_worker_messages_list_by_transaction_id(
    tabsserver_connection, testing_collection_with_table
):
    transaction_id = None
    for transaction in tabsserver_connection.transactions:
        if transaction.status in TRANSACTION_FINAL_STATUSES:
            transaction_id = transaction.id
            break
    logger.debug(f"Transactions: {tabsserver_connection.transactions}")
    logger.debug(f"Transaction ID: {transaction_id}")
    assert transaction_id
    messages = tabsserver_connection.list_workers(
        filter=[f"transaction_id:eq:{transaction_id}"]
    )
    assert messages
    assert isinstance(messages, list)
    assert all(isinstance(message, Worker) for message in messages)


@pytest.mark.integration
@pytest.mark.requires_internet
def test_tabsdata_server_class_worker_messages_list_by_function_and_collection(
    tabsserver_connection, testing_collection_with_table
):
    function_name = tabsserver_connection.list_functions(testing_collection_with_table)[
        0
    ].name
    messages = tabsserver_connection.list_workers(
        filter=[
            f"function:eq:{function_name}",
            f"collection:eq:{testing_collection_with_table}",
        ]
    )
    assert messages
    assert isinstance(messages, list)
    assert all(isinstance(message, Worker) for message in messages)


@pytest.mark.integration
def test_tabsdata_server_class_read_run(tabsserver_connection):
    collection = tabsserver_connection.create_collection(
        f"test_tabsdata_server_class_read_run_{uuid.uuid4().hex[:16]}"
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
        f"test_tabsdata_server_class_read_run_plan_{uuid.uuid4().hex[:16]}"
    )
    response = tabsserver_connection.read_function_run(
        collection.name, function.name, plan.id
    )
    assert response.status_code == 200
    response = tabsserver_connection.read_function_run(collection, function, plan)
    assert response.status_code == 200


@pytest.mark.integration
@pytest.mark.requires_internet
def test_tabsdata_server_execution_cancel(tabsserver_connection):
    try:
        tabsserver_connection.create_collection(
            name="test_tabsdata_server_execution_cancel_collection",
            description="test_collection_description",
        )
        tabsserver_connection.register_function(
            collection_name="test_tabsdata_server_execution_cancel_collection",
            description="test_tabsdata_server_execution_cancel_description",
            function_path=(
                f"{os.path.join(ABSOLUTE_TEST_FOLDER_LOCATION, "testing_resources",
                                "test_input_plugin", "example.py")}::input_plugin"
            ),
            local_packages=LOCAL_PACKAGES_LIST,
        )
        execution = tabsserver_connection.trigger_function(
            "test_tabsdata_server_execution_cancel_collection", "test_input_plugin"
        )
        assert isinstance(execution, Execution)
        response = tabsserver_connection.cancel_execution(execution.id)
        assert response.status_code == 200
    finally:
        tabsserver_connection.delete_function(
            "test_tabsdata_server_execution_cancel_collection",
            "test_input_plugin",
            raise_for_status=False,
        )
        tabsserver_connection.delete_collection(
            "test_tabsdata_server_execution_cancel_collection", raise_for_status=False
        )


@pytest.mark.integration
@pytest.mark.requires_internet
@pytest.mark.skip(reason="Awaiting decision of behavior of recover method.")
def test_tabsdata_server_execution_recover(tabsserver_connection):
    try:
        tabsserver_connection.create_collection(
            name="test_tabsdata_server_execution_recover_collection",
            description="test_collection_description",
        )
        tabsserver_connection.register_function(
            collection_name="test_tabsdata_server_execution_recover_collection",
            description="test_tabsdata_server_execution_recover_description",
            function_path=(
                f"{os.path.join(ABSOLUTE_TEST_FOLDER_LOCATION, "testing_resources",
                                "test_input_plugin", "example.py")}::input_plugin"
            ),
            local_packages=LOCAL_PACKAGES_LIST,
        )
        execution = tabsserver_connection.trigger_function(
            "test_tabsdata_server_execution_recover_collection", "test_input_plugin"
        )
        assert isinstance(execution, Execution)
        response = tabsserver_connection.recover_execution(execution.id)
        assert response.status_code == 200
    finally:
        tabsserver_connection.delete_function(
            "test_tabsdata_server_execution_recover_collection",
            "test_input_plugin",
            raise_for_status=False,
        )
        tabsserver_connection.delete_collection(
            "test_tabsdata_server_execution_recover_collection", raise_for_status=False
        )


@pytest.mark.integration
@pytest.mark.requires_internet
def test_tabsdata_server_get_transaction(
    tabsserver_connection, testing_collection_with_table
):
    transaction_id = None
    for element in tabsserver_connection.transactions:
        transaction_id = element.id
        break
    logger.debug(f"Transactions: {tabsserver_connection.transactions}")
    logger.debug(f"Transaction ID: {transaction_id}")
    assert transaction_id
    transaction = tabsserver_connection.get_transaction(transaction_id)
    assert isinstance(transaction, Transaction)
    assert transaction.id == transaction_id
    assert transaction.__repr__()
    assert transaction.__str__()


@pytest.mark.integration
@pytest.mark.requires_internet
def test_tabsdata_server_get_execution(
    tabsserver_connection, testing_collection_with_table
):
    execution_id = None
    for element in tabsserver_connection.executions:
        execution_id = element.id
        break
    logger.debug(f"Executions: {tabsserver_connection.executions}")
    logger.debug(f"Execution ID: {execution_id}")
    assert execution_id
    execution = tabsserver_connection.get_execution(execution_id)
    assert isinstance(execution, Execution)
    assert execution.id == execution_id
    assert execution.__repr__()
    assert execution.__str__()


@pytest.mark.integration
@pytest.mark.requires_internet
def test_tabsdata_server_transaction_cancel(tabsserver_connection):
    try:
        tabsserver_connection.create_collection(
            name="test_tabsdata_server_transaction_cancel_collection",
            description="test_collection_description",
        )
        tabsserver_connection.register_function(
            collection_name="test_tabsdata_server_transaction_cancel_collection",
            description="test_tabsdata_server_transaction_cancel_description",
            function_path=(
                f"{os.path.join(ABSOLUTE_TEST_FOLDER_LOCATION, "testing_resources",
                                "test_input_plugin", "example.py")}::input_plugin"
            ),
            local_packages=LOCAL_PACKAGES_LIST,
        )
        execution = tabsserver_connection.trigger_function(
            "test_tabsdata_server_transaction_cancel_collection", "test_input_plugin"
        )
        assert isinstance(execution, Execution)
        transaction = execution.transactions[0]
        response = tabsserver_connection.cancel_transaction(transaction.id)
        assert response.status_code == 200
    finally:
        tabsserver_connection.delete_function(
            "test_tabsdata_server_transaction_cancel_collection",
            "test_input_plugin",
            raise_for_status=False,
        )
        tabsserver_connection.delete_collection(
            "test_tabsdata_server_transaction_cancel_collection", raise_for_status=False
        )


@pytest.mark.integration
@pytest.mark.requires_internet
@pytest.mark.skip(reason="Awaiting decision of behavior of recover method.")
def test_tabsdata_server_transaction_recover(tabsserver_connection):
    try:
        tabsserver_connection.create_collection(
            name="test_tabsdata_server_transaction_recover_collection",
            description="test_collection_description",
        )
        tabsserver_connection.register_function(
            collection_name="test_tabsdata_server_transaction_recover_collection",
            description="test_tabsdata_server_transaction_recover_description",
            function_path=(
                f"{os.path.join(ABSOLUTE_TEST_FOLDER_LOCATION, "testing_resources",
                                "test_input_plugin", "example.py")}::input_plugin"
            ),
            local_packages=LOCAL_PACKAGES_LIST,
        )
        execution = tabsserver_connection.trigger_function(
            "test_tabsdata_server_transaction_recover_collection", "test_input_plugin"
        )
        assert isinstance(execution, Execution)
        transaction = execution.transactions[0]
        response = tabsserver_connection.recover_transaction(transaction.id)
        assert response.status_code == 200
    finally:
        tabsserver_connection.delete_function(
            "test_tabsdata_server_transaction_recover_collection",
            "test_input_plugin",
            raise_for_status=False,
        )
        tabsserver_connection.delete_collection(
            "test_tabsdata_server_transaction_recover_collection",
            raise_for_status=False,
        )


def test_complete_datetime():
    result = top_and_convert_to_timestamp("2025-01-16Z")
    assert datetime.datetime.fromtimestamp(
        result / 1000, datetime.UTC
    ) == datetime.datetime(2025, 1, 17, 0, 0, tzinfo=datetime.timezone.utc)

    result = top_and_convert_to_timestamp("2025-01-16T15Z")
    assert datetime.datetime.fromtimestamp(
        result / 1000, datetime.UTC
    ) == datetime.datetime(2025, 1, 16, 16, 0, tzinfo=datetime.timezone.utc)

    result = top_and_convert_to_timestamp("2025-01-16T15:30Z")
    assert datetime.datetime.fromtimestamp(
        result / 1000, datetime.UTC
    ) == datetime.datetime(2025, 1, 16, 15, 31, tzinfo=datetime.timezone.utc)

    result = top_and_convert_to_timestamp("2025-01-16T15:59Z")
    assert datetime.datetime.fromtimestamp(
        result / 1000, datetime.UTC
    ) == datetime.datetime(2025, 1, 16, 16, 0, tzinfo=datetime.timezone.utc)

    result = top_and_convert_to_timestamp("2025-01-16T15:30:45Z")
    assert datetime.datetime.fromtimestamp(
        result / 1000, datetime.UTC
    ) == datetime.datetime(2025, 1, 16, 15, 30, 46, tzinfo=datetime.timezone.utc)

    result = top_and_convert_to_timestamp("2025-01-16T15:30:59Z")
    assert datetime.datetime.fromtimestamp(
        result / 1000, datetime.UTC
    ) == datetime.datetime(2025, 1, 16, 15, 31, 0, tzinfo=datetime.timezone.utc)

    assert top_and_convert_to_timestamp(None) is None
    assert top_and_convert_to_timestamp("123456") == 123456


@pytest.mark.integration
@pytest.mark.requires_internet
def test_function_run_list_by_execution_id(
    tabsserver_connection, testing_collection_with_table
):
    execution_id = tabsserver_connection.executions[0].id
    messages = tabsserver_connection.list_function_runs(
        filter=[f"execution_id:eq:{execution_id}"]
    )
    assert isinstance(messages, list)
    assert all(isinstance(message, FunctionRun) for message in messages)


@pytest.mark.integration
@pytest.mark.requires_internet
def test_function_run_list_by_transaction_id(
    tabsserver_connection, testing_collection_with_table
):
    transaction_id = None
    for transaction in tabsserver_connection.transactions:
        if transaction.status in TRANSACTION_FINAL_STATUSES:
            transaction_id = transaction.id
            break
    logger.debug(f"Transactions: {tabsserver_connection.transactions}")
    logger.debug(f"Transaction ID: {transaction_id}")
    assert transaction_id
    messages = tabsserver_connection.list_function_runs(
        filter=[f"transaction_id:eq:{transaction_id}"]
    )
    assert messages
    assert isinstance(messages, list)
    assert all(isinstance(message, FunctionRun) for message in messages)


@pytest.mark.integration
@pytest.mark.requires_internet
def test_tabsdata_server_class_function_run_list_by_function_and_collection(
    tabsserver_connection, testing_collection_with_table
):
    function_name = tabsserver_connection.list_functions(testing_collection_with_table)[
        0
    ].name
    messages = tabsserver_connection.list_function_runs(
        filter=[
            f"name:eq:{function_name}",
            f"collection:eq:{testing_collection_with_table}",
        ]
    )
    assert messages
    assert isinstance(messages, list)
    assert all(isinstance(message, FunctionRun) for message in messages)


@pytest.mark.integration
@pytest.mark.requires_internet
def test_tabsdata_valid_python_versions(tabsserver_connection):
    python_versions = tabsserver_connection.valid_python_versions
    assert isinstance(python_versions, list)
    assert all(isinstance(version, str) for version in python_versions)


@pytest.mark.integration
@pytest.mark.requires_internet
def test_tabsserver_class_table_delete(tabsserver_connection):
    try:
        tabsserver_connection.create_collection(
            name="test_tabsserver_class_table_delete_collection",
            description="test_collection_description",
        )
        tabsserver_connection.register_function(
            collection_name="test_tabsserver_class_table_delete_collection",
            description="test_table_delete_description",
            function_path=(
                f"{os.path.join(ABSOLUTE_TEST_FOLDER_LOCATION, "testing_resources",
                                "test_input_plugin", "example.py")}::input_plugin"
            ),
            local_packages=LOCAL_PACKAGES_LIST,
        )
        functions = tabsserver_connection.list_functions(
            "test_tabsserver_class_table_delete_collection"
        )
        assert any(function.name == "test_input_plugin" for function in functions)
        assert any(
            table.name == "output"
            for table in tabsserver_connection.list_tables(
                "test_tabsserver_class_table_delete_collection"
            )
        )
        tabsserver_connection.delete_function(
            "test_tabsserver_class_table_delete_collection", "test_input_plugin"
        )
        functions = tabsserver_connection.list_functions(
            "test_tabsserver_class_table_delete_collection"
        )
        assert not any(function.name == "test_input_plugin" for function in functions)
        tabsserver_connection.delete_table(
            "test_tabsserver_class_table_delete_collection", "output"
        )
        assert not any(
            table.name == "output"
            for table in tabsserver_connection.list_tables(
                "test_tabsserver_class_table_delete_collection"
            )
        )
    finally:
        tabsserver_connection.delete_function(
            "test_tabsserver_class_table_delete_collection",
            "test_input_plugin",
            raise_for_status=False,
        )
        tabsserver_connection.delete_collection(
            "test_tabsserver_class_table_delete_collection", raise_for_status=False
        )
