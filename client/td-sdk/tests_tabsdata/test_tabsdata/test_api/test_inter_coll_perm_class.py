#
# Copyright 2025 Tabs Data Inc.
#

import time

import pytest

from tabsdata.api.tabsdata_server import (
    Collection,
    InterCollectionPermission,
    _convert_timestamp_to_string,
)

# noinspection PyUnresolvedReferences
from . import pytestmark  # noqa: F401


@pytest.mark.integration
def test_inter_coll_perm_class(tabsserver_connection):
    created_time = int(time.time())
    coll_a = tabsserver_connection.create_collection(
        "test_inter_coll_perm_class_coll_a"
    )
    inter_coll_perm = InterCollectionPermission(
        connection=tabsserver_connection.connection,
        id="fake_id",
        collection=coll_a,
        granted_on=created_time,
        example_kwarg="example",
    )
    assert inter_coll_perm.id == "fake_id"
    assert inter_coll_perm.collection == coll_a
    assert inter_coll_perm.granted_on == created_time
    assert inter_coll_perm.granted_on_str == _convert_timestamp_to_string(created_time)
    assert inter_coll_perm.kwargs == {
        "example_kwarg": "example",
        "granted_on": created_time,
    }
    assert inter_coll_perm.__repr__()
    assert inter_coll_perm.__str__()
    assert inter_coll_perm == inter_coll_perm
    assert inter_coll_perm != InterCollectionPermission(
        connection=tabsserver_connection.connection,
        id="another_fake_id",
        collection=coll_a,
        granted_on=created_time,
        example_kwarg="example",
    )
    assert inter_coll_perm != "test"


@pytest.mark.integration
def test_inter_coll_perm_class_lazy_properties(tabsserver_connection):
    coll_a_name = "test_inter_coll_perm_class_lazy_properties_coll_a"
    coll_b_name = "test_inter_coll_perm_class_lazy_properties_coll_b"
    try:
        coll_a = tabsserver_connection.create_collection(
            coll_a_name,
        )
        coll_b = tabsserver_connection.create_collection(
            coll_b_name,
        )
        permission = coll_a.create_permission(coll_b)
        lazy_permission = InterCollectionPermission(
            tabsserver_connection.connection, permission.id, coll_a.name
        )
        assert lazy_permission.id == permission.id
        assert lazy_permission.collection == coll_a
        assert lazy_permission.to_collection == coll_b
        assert lazy_permission.granted_on == permission.granted_on
        assert lazy_permission.granted_on_str == permission.granted_on_str
        assert lazy_permission._data
        assert lazy_permission.__repr__()
        assert lazy_permission.__str__()
    finally:
        tabsserver_connection.delete_collection(coll_a_name, raise_for_status=False)
        tabsserver_connection.delete_collection(coll_b_name, raise_for_status=False)


@pytest.mark.integration
def test_inter_coll_perm_class_delete(tabsserver_connection):
    coll_a_name = "test_inter_coll_perm_class_lazy_properties_coll_a"
    coll_b_name = "test_inter_coll_perm_class_lazy_properties_coll_b"
    try:
        coll_a = tabsserver_connection.create_collection(
            coll_a_name,
        )
        coll_b = tabsserver_connection.create_collection(
            coll_b_name,
        )
        permission = coll_a.create_permission(coll_b)
        assert permission in coll_a.permissions
        permission.delete()
        assert permission not in coll_a.permissions
    finally:
        tabsserver_connection.delete_collection(coll_a_name, raise_for_status=False)
        tabsserver_connection.delete_collection(coll_b_name, raise_for_status=False)
