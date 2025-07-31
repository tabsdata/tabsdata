#
# Copyright 2025 Tabs Data Inc.
#

import time

import pytest

from tabsdata._api.tabsdata_server import (
    Role,
    convert_timestamp_to_string,
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
    assert role.created_on_str == convert_timestamp_to_string(created_time)
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
