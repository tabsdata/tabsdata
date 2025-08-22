#
# Copyright 2025 Tabs Data Inc.
#

import time

import pytest

from tabsdata.api.tabsdata_server import (
    Role,
    User,
    _convert_timestamp_to_string,
)

# noinspection PyUnresolvedReferences
from . import pytestmark  # noqa: F401


@pytest.mark.integration
def test_role_class(tabsserver_connection):
    created_time = int(time.time())
    role = Role(
        connection=tabsserver_connection.connection,
        name="test_role_class",
        created_on=created_time,
        created_by="test",
        description="test_role_class_description",
        example_kwarg="example",
    )
    assert role.name == "test_role_class"
    assert role.description == "test_role_class_description"
    assert role.created_on == created_time
    assert role.created_on_str == _convert_timestamp_to_string(created_time)
    assert role.created_by == "test"
    assert role.kwargs == {
        "example_kwarg": "example",
        "created_by": "test",
        "created_on": created_time,
        "description": "test_role_class_description",
    }
    assert role.__repr__()
    assert role.__str__()
    assert role == role
    assert role != Role(
        connection=tabsserver_connection.connection,
        name="test_role_class_second_name",
        description="test_role_class_description",
        id="test_role_class_id",
        created_on=int(time.time()),
        created_by="test",
    )
    assert role != "test"


@pytest.mark.integration
def test_role_class_lazy_properties(tabsserver_connection):
    try:
        tabsserver_connection.create_role(
            name="test_role_class_lazy_properties",
            description="test_role_class_lazy_properties",
        )
        example_role = tabsserver_connection.get_role("test_role_class_lazy_properties")
        lazy_role = Role(tabsserver_connection.connection, example_role.name)
        assert lazy_role.name == example_role.name
        assert lazy_role.description == example_role.description
        assert lazy_role.created_on == example_role.created_on
        assert lazy_role.created_on_str == example_role.created_on_str
        assert lazy_role.created_by == example_role.created_by
        assert lazy_role._data
        assert lazy_role.__repr__()
        assert lazy_role.__str__()
    finally:
        tabsserver_connection.delete_role(
            "test_role_class_lazy_properties", raise_for_status=False
        )


@pytest.mark.integration
def test_role_delete(tabsserver_connection):
    role = Role(tabsserver_connection.connection, "test_role_delete_role").create()
    assert role in tabsserver_connection.roles
    role.delete()
    assert role not in tabsserver_connection.roles


@pytest.mark.integration
def test_role_update(tabsserver_connection):
    role = Role(
        tabsserver_connection.connection,
        "test_role_update_role",
        description="old_description",
    )
    try:
        role.create()
        assert role.description == "old_description"
        assert role.name == "test_role_update_role"
        role.update(
            name="test_role_update_role_new_name",
            description="new_description",
        )
        assert role.description == "new_description"
        assert role.name == "test_role_update_role_new_name"
    finally:
        role.delete(raise_for_status=False)


@pytest.mark.integration
def test_role_create(tabsserver_connection):
    role = Role(tabsserver_connection.connection, "test_role_create_role")
    try:
        role.create()
        assert role in tabsserver_connection.roles
    finally:
        role.delete(raise_for_status=False)


@pytest.mark.integration
def test_role_refresh(tabsserver_connection):
    role = Role(
        tabsserver_connection.connection,
        "test_role_refresh_role",
        description="old_description",
    )
    try:
        role.create()
        assert role in tabsserver_connection.roles
        assert role.description == "old_description"
        tabsserver_connection.update_role(
            "test_role_refresh_role", new_description="new_description"
        )
        assert role.description == "old_description"
        role.refresh()
        assert role.description == "new_description"
    finally:
        role.delete(raise_for_status=False)


@pytest.mark.integration
def test_role_class_permission_list(tabsserver_connection):
    role_name = "test_role_class_permission_list"
    permission_type = "sa"
    tabsserver_connection.delete_role(role_name, raise_for_status=False)
    try:
        role = Role(tabsserver_connection.connection, role_name)
        role.create()
        permission = role.create_permission(permission_type)
        assert permission in role.permissions
    finally:
        tabsserver_connection.delete_role(role_name, raise_for_status=False)


@pytest.mark.integration
def test_role_class_permission_create(tabsserver_connection):
    role_name = "test_role_class_permission_create"
    permission_type = "sa"
    tabsserver_connection.delete_role(role_name, raise_for_status=False)
    try:
        role = Role(tabsserver_connection.connection, role_name)
        role.create()
        permission = role.create_permission(permission_type)
        assert permission in role.permissions
        assert permission.permission_type == permission_type
        assert permission.role == role
    finally:
        tabsserver_connection.delete_role(role_name, raise_for_status=False)


@pytest.mark.integration
def test_role_class_permission_delete(tabsserver_connection):
    role_name = "test_role_class_permission_delete"
    permission_type = "sa"
    tabsserver_connection.delete_role(role_name, raise_for_status=False)
    try:
        role = Role(tabsserver_connection.connection, role_name)
        role.create()
        permission = role.create_permission(permission_type)
        assert permission in role.permissions
        role.delete_permission(permission.id)
        assert permission not in role.permissions
    finally:
        tabsserver_connection.delete_role(role_name, raise_for_status=False)


@pytest.mark.integration
def test_role_class_permission_delete_no_exists_raises_error(tabsserver_connection):
    role_name = "test_role_class_permission_delete_no_exists_raises_error"
    tabsserver_connection.delete_role(role_name, raise_for_status=False)
    try:
        role = Role(tabsserver_connection.connection, role_name)
        role.create()
        with pytest.raises(Exception):
            role.delete_permission("test_role_delete_no_exists")
    finally:
        tabsserver_connection.delete_role(role_name, raise_for_status=False)


@pytest.mark.integration
def test_role_class_user_list(tabsserver_connection):
    role_name = "test_role_class_user_list_role"
    user_name = "test_role_class_user_list_user"
    tabsserver_connection.delete_user(user_name, raise_for_status=False)
    tabsserver_connection.delete_role(role_name, raise_for_status=False)
    try:
        role = Role(tabsserver_connection.connection, role_name)
        role.create()
        user = User(tabsserver_connection.connection, user_name)
        user.create("fakepassword")
        role.add_user(user.name)
        assert user in role.users
    finally:
        tabsserver_connection.delete_user(user_name, raise_for_status=False)
        tabsserver_connection.delete_role(role_name, raise_for_status=False)


@pytest.mark.integration
def test_role_class_user_add(tabsserver_connection):
    role_name = "test_role_class_user_add_role"
    user_name = "test_role_class_user_add_user"
    tabsserver_connection.delete_user(user_name, raise_for_status=False)
    tabsserver_connection.delete_role(role_name, raise_for_status=False)
    try:
        role = Role(tabsserver_connection.connection, role_name)
        role.create()
        user = User(tabsserver_connection.connection, user_name)
        user.create("fakepassword")
        assert user not in role.users
        added_user = role.add_user(user.name)
        assert added_user == user
        assert user in role.users
    finally:
        tabsserver_connection.delete_user(user_name, raise_for_status=False)
        tabsserver_connection.delete_role(role_name, raise_for_status=False)


@pytest.mark.integration
def test_role_class_user_delete(tabsserver_connection):
    role_name = "test_role_class_user_delete_role"
    user_name = "test_role_class_user_delete_user"
    tabsserver_connection.delete_user(user_name, raise_for_status=False)
    tabsserver_connection.delete_role(role_name, raise_for_status=False)
    try:
        role = Role(tabsserver_connection.connection, role_name)
        role.create()
        user = User(tabsserver_connection.connection, user_name)
        user.create("fakepassword")
        assert user not in role.users
        role.add_user(user.name)
        assert user in role.users
        deleted_user = role.delete_user(user.name)
        assert deleted_user == user
        assert user not in role.users
    finally:
        tabsserver_connection.delete_user(user_name, raise_for_status=False)
        tabsserver_connection.delete_role(role_name, raise_for_status=False)


@pytest.mark.integration
def test_role_class_user_delete_no_exists_raises_error(tabsserver_connection):
    role_name = "test_role_class_user_delete_no_exists_raises_error_role"
    user_name = "test_role_class_user_delete_no_exists_raises_error_user"
    tabsserver_connection.delete_user(user_name, raise_for_status=False)
    tabsserver_connection.delete_role(role_name, raise_for_status=False)
    try:
        role = Role(tabsserver_connection.connection, role_name)
        role.create()
        with pytest.raises(Exception):
            role.delete_user(user_name)
    finally:
        tabsserver_connection.delete_role(role_name, raise_for_status=False)
