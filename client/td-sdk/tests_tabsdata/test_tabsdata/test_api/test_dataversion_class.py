#
# Copyright 2025 Tabs Data Inc.
#

import time

import pytest

from tabsdata.api.tabsdata_server import (
    Collection,
    DataVersion,
    Execution,
    Function,
    Table,
)

# noinspection PyUnresolvedReferences
from . import pytestmark  # noqa: F401


@pytest.mark.integration
@pytest.mark.slow
@pytest.mark.skip(reason="To be unskipped in an upstream PR")
def test_data_version_class_lazy_properties(
    tabsserver_connection, testing_collection_with_table
):
    table_name = (
        tabsserver_connection.get_collection(testing_collection_with_table)
        .tables[0]
        .name
    )
    data_versions = tabsserver_connection.list_dataversions(
        testing_collection_with_table, table_name
    )
    assert data_versions
    assert isinstance(data_versions, list)
    assert all(isinstance(version, DataVersion) for version in data_versions)
    data_version = data_versions[0]
    assert data_version.id
    assert data_version.collection == Collection(
        tabsserver_connection.connection, testing_collection_with_table
    )
    assert data_version.table.name == table_name
    assert data_version.table == Table(
        tabsserver_connection.connection, testing_collection_with_table, table_name
    )
    assert data_version.function
    assert isinstance(data_version.function, Function)
    assert data_version.status
    assert data_version.__repr__()
    assert data_version.__str__()
    assert data_version.execution
    assert isinstance(data_version.execution, Execution)

    lazy_data_version = DataVersion(
        data_version.connection,
        data_version.id,
        data_version.collection,
        data_version.table,
    )
    assert lazy_data_version.id == data_version.id
    assert lazy_data_version.collection == data_version.collection
    assert lazy_data_version.table == data_version.table
    assert lazy_data_version.function
    assert isinstance(lazy_data_version.function, Function)
    assert lazy_data_version == data_version
    assert lazy_data_version.__repr__()
    assert lazy_data_version.__str__()
