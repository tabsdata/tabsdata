#
# Copyright 2025 Tabs Data Inc.
#

import time

import pytest

from tabsdata.api.tabsdata_server import (
    Role,
    RolePermission,
    _convert_timestamp_to_string,
)

# noinspection PyUnresolvedReferences
from . import pytestmark  # noqa: F401


@pytest.mark.integration
def test_role_permission_class(tabsserver_connection):
    created_time = int(time.time())
    role = tabsserver_connection.create_role("test_role_permission_class_role")
    role_permission = RolePermission(
        connection=tabsserver_connection.connection,
        id="fake_id",
        role=role,
        granted_on=created_time,
        example_kwarg="example",
    )
    assert role_permission.id == "fake_id"
    assert role_permission.role == role
    assert role_permission.granted_on == created_time
    assert role_permission.granted_on_str == _convert_timestamp_to_string(created_time)
    assert role_permission.kwargs == {
        "example_kwarg": "example",
        "granted_on": created_time,
    }
    assert role_permission.__repr__()
    assert role_permission.__str__()
    assert role_permission == role_permission
    assert role_permission != RolePermission(
        connection=tabsserver_connection.connection,
        id="another_fake_id",
        role=role,
        granted_on=created_time,
        example_kwarg="example",
    )
    assert role_permission != "test"


@pytest.mark.integration
def test_role_permission_class_lazy_properties(tabsserver_connection):
    role_name = "test_role_permission_class_lazy_properties"
    try:
        role = tabsserver_connection.create_role(
            name=role_name,
        )
        permission_type = "sa"
        permission = role.create_permission(permission_type)
        lazy_permission = RolePermission(
            tabsserver_connection.connection, permission.id, role.name
        )
        assert lazy_permission.id == permission.id
        assert lazy_permission.role == role
        assert lazy_permission.granted_on == permission.granted_on
        assert lazy_permission.granted_on_str == permission.granted_on_str
        assert lazy_permission.permission_type == permission_type
        assert lazy_permission.permission_type == permission.permission_type
        assert lazy_permission._data
        assert lazy_permission.__repr__()
        assert lazy_permission.__str__()
    finally:
        tabsserver_connection.delete_role(role_name, raise_for_status=False)


@pytest.mark.integration
def test_role_permission_class_permission_type_long_format(tabsserver_connection):
    role_name = "test_role_permission_class_permission_type_long_format"
    try:
        role = tabsserver_connection.create_role(
            name=role_name,
        )
        permission_type = "sys_admin"
        permission = role.create_permission(permission_type)
        lazy_permission = RolePermission(
            tabsserver_connection.connection, permission.id, role.name
        )
        assert lazy_permission.permission_type == "sa"
        assert lazy_permission.permission_type == permission.permission_type
    finally:
        tabsserver_connection.delete_role(role_name, raise_for_status=False)


@pytest.mark.integration
def test_role_permission_class_wrong_permission_type(tabsserver_connection):
    role_name = "test_role_permission_class_wrong_permission_type"
    try:
        role = tabsserver_connection.create_role(
            name=role_name,
        )
        permission_type = "invalid_permission_type"
        with pytest.raises(ValueError):
            role.create_permission(permission_type)
    finally:
        tabsserver_connection.delete_role(role_name, raise_for_status=False)


@pytest.mark.integration
def test_role_permission_class_delete(tabsserver_connection):
    role_name = "test_role_permission_delete_role_perm_class"
    permission_type = "sa"
    tabsserver_connection.delete_role(role_name, raise_for_status=False)
    try:
        role = Role(
            tabsserver_connection.connection,
            role_name,
        )
        role.create()
        permission = role.create_permission(permission_type)
        assert permission in role.permissions
        permission.delete()
        assert permission not in role.permissions
    finally:
        tabsserver_connection.delete_role(role_name, raise_for_status=False)
