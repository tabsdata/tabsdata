#
# Copyright 2025 Tabs Data Inc.
#

import pytest

from tabsdata.api.tabsdata_server import User

# noinspection PyUnresolvedReferences
from . import pytestmark  # noqa: F401


@pytest.mark.integration
def test_user_class(tabsserver_connection):
    user = User(
        tabsserver_connection.connection,
        name="test",
        full_name="Test User",
        email="test_email@tabsdata.com",
        enabled=True,
        example_kwarg="example",
    )
    assert user.name == "test"
    assert user.full_name == "Test User"
    assert user.email == "test_email@tabsdata.com"
    assert user.enabled is True
    assert user.kwargs == {"example_kwarg": "example"}
    assert user.__repr__()
    assert user.__str__()
    assert user == user
    assert user != User(
        tabsserver_connection.connection,
        name="test2",
        full_name="Test User",
        email="test_email@tabsdata.com",
        enabled=True,
    )
    assert user != "test"


@pytest.mark.integration
def test_user_class_lazy_properties(tabsserver_connection):
    try:
        tabsserver_connection.user_create(
            name="test_user_class_lazy_properties",
            password="testingpassword",
            full_name="Test User Class",
            email="test_user_class_email@tabsdata.com",
            enabled=True,
        )
        user = User(tabsserver_connection.connection, "test_user_class_lazy_properties")
        assert user.name == "test_user_class_lazy_properties"
        assert user.full_name == "Test User Class"
        assert user.email == "test_user_class_email@tabsdata.com"
        assert user.enabled is True
        assert user._data
        assert user.__repr__()
        assert user.__str__()
    finally:
        tabsserver_connection.user_delete(
            "test_user_class_lazy_properties", raise_for_status=False
        )


@pytest.mark.integration
def test_user_class_create(tabsserver_connection):
    user = User(
        tabsserver_connection.connection,
        name="test_user_class_create",
        enabled=False,
        full_name="Test User Class Create",
        email="test_user_class_create_email@tabsdata.com",
    )
    try:
        assert user not in tabsserver_connection.users
        user.create(password="testingpassword")
        assert user in tabsserver_connection.users
        found_user = tabsserver_connection.user_get("test_user_class_create")
        assert found_user.name == "test_user_class_create"
        assert found_user.full_name == "Test User Class Create"
        assert found_user.email == "test_user_class_create_email@tabsdata.com"
        assert found_user.enabled is False
        assert user.name == "test_user_class_create"
        assert user.full_name == "Test User Class Create"
        assert user.email == "test_user_class_create_email@tabsdata.com"
        assert user.enabled is False
    finally:
        tabsserver_connection.user_delete(
            "test_user_class_create", raise_for_status=False
        )


@pytest.mark.integration
def test_user_class_delete(tabsserver_connection):
    user = User(
        tabsserver_connection.connection,
        name="test_user_class_delete",
    )
    try:
        user.create(password="testingpassword")
        assert user in tabsserver_connection.users
        user.delete()
        assert user not in tabsserver_connection.users
    finally:
        tabsserver_connection.user_delete(
            "test_user_class_delete", raise_for_status=False
        )


@pytest.mark.integration
def test_user_class_update(tabsserver_connection):
    user = User(
        tabsserver_connection.connection,
        name="test_user_class_update",
        full_name="Test User Class Update",
        email="test_user_class_update_email@tabsdata.com",
    )
    try:
        user.create(password="testingpassword")
        assert user.full_name == "Test User Class Update"
        assert user.email == "test_user_class_update_email@tabsdata.com"
        assert user.enabled is True
        found_user = tabsserver_connection.user_get("test_user_class_update")
        assert found_user.full_name == "Test User Class Update"
        assert found_user.email == "test_user_class_update_email@tabsdata.com"
        assert found_user.enabled is True

        user.update(
            full_name="Test User Class Update Updated",
            email="test_user_class_update_email_updated@tabsdata.com",
            enabled=False,
        )
        assert user.full_name == "Test User Class Update Updated"
        assert user.email == "test_user_class_update_email_updated@tabsdata.com"
        assert user.enabled is False
        found_user = tabsserver_connection.user_get("test_user_class_update")
        assert found_user.full_name == "Test User Class Update Updated"
        assert found_user.email == "test_user_class_update_email_updated@tabsdata.com"
        assert found_user.enabled is False
    finally:
        tabsserver_connection.user_delete(
            "test_user_class_update", raise_for_status=False
        )


@pytest.mark.integration
def test_user_class_refresh(tabsserver_connection):
    user = User(
        tabsserver_connection.connection,
        name="test_user_class_refresh",
        full_name="Test User Class Refresh",
        email="test_user_class_refresh_email@tabsdata.com",
    )
    try:
        user.create(password="testingpassword")
        assert user.full_name == "Test User Class Refresh"
        assert user.email == "test_user_class_refresh_email@tabsdata.com"
        assert user.enabled is True

        tabsserver_connection.user_update(
            name="test_user_class_refresh",
            full_name="Test User Class Refresh Refreshed",
            email="test_user_class_refresh_email_refreshed@tabsdata.com",
            enabled=False,
        )
        assert user.full_name == "Test User Class Refresh"
        assert user.email == "test_user_class_refresh_email@tabsdata.com"
        assert user.enabled is True
        user.refresh()
        assert user.full_name == "Test User Class Refresh Refreshed"
        assert user.email == "test_user_class_refresh_email_refreshed@tabsdata.com"
        assert user.enabled is False
    finally:
        tabsserver_connection.user_delete(
            "test_user_class_refresh", raise_for_status=False
        )
