#
# Copyright 2025 Tabs Data Inc.
#

import time

import pytest

from tabsdata.api.tabsdata_server import (
    Collection,
    DataVersion,
    ExecutionPlan,
    Function,
    Worker,
)


@pytest.mark.integration
@pytest.mark.wip
@pytest.mark.skip(reason="Pending rework after server last refactors.")
def test_data_version_class(tabsserver_connection):
    time_triggered = int(time.time())
    data_version = DataVersion(
        connection=tabsserver_connection.connection,
        collection="test_collection",
        function="test_function",
        id="test_id",
        triggered_on=time_triggered,
        status="F",
        function_id="test_function_id",
        example_kwarg="example",
    )
    assert data_version.id == "test_id"
    assert data_version.triggered_on == time_triggered
    assert isinstance(data_version.triggered_on_str, str)
    assert data_version.status == "Failed"
    assert data_version.function_id == "test_function_id"
    assert data_version.__repr__()
    assert data_version.__str__()


@pytest.mark.integration
@pytest.mark.slow
@pytest.mark.wip
@pytest.mark.skip(reason="Pending rework after server last refactors.")
def test_data_version_class_lazy_properties(
    tabsserver_connection, testing_collection_with_table
):
    function_name = tabsserver_connection.collection_list_functions(
        testing_collection_with_table
    )[0].name
    data_versions = tabsserver_connection.dataversion_list(
        testing_collection_with_table, function_name
    )
    assert data_versions
    assert isinstance(data_versions, list)
    assert all(isinstance(version, DataVersion) for version in data_versions)
    data_version = data_versions[0]
    assert data_version.id
    assert data_version.collection == Collection(
        tabsserver_connection.connection, testing_collection_with_table
    )
    assert data_version.function.name == function_name
    assert data_version.function == Function(
        tabsserver_connection.connection, testing_collection_with_table, function_name
    )
    assert data_version.status
    assert data_version.__repr__()
    assert data_version.__str__()
    assert isinstance(data_version.workers, list)
    assert all(isinstance(worker, Worker) for worker in data_version.workers)
    assert data_version.execution_plan
    assert isinstance(data_version.execution_plan, ExecutionPlan)
