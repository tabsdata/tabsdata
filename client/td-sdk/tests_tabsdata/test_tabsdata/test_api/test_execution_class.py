#
# Copyright 2025 Tabs Data Inc.
#

import os
import time

import pytest
from tests_tabsdata.conftest import ABSOLUTE_TEST_FOLDER_LOCATION, LOCAL_PACKAGES_LIST

from tabsdata.api.tabsdata_server import (
    Collection,
    Execution,
    Function,
    FunctionRun,
    Transaction,
    Worker,
)

# noinspection PyUnresolvedReferences
from . import pytestmark  # noqa: F401


@pytest.mark.integration
def test_execution_class(tabsserver_connection):
    time_triggered = int(time.time())
    execution = Execution(
        connection=tabsserver_connection.connection,
        id="execution_id",
        name="execution_name",
        triggered_by="triggered_by",
        triggered_on=time_triggered,
        status="F",
        random_kwarg="kwarg",
        started_on=time_triggered,
        ended_on=time_triggered,
    )
    assert execution.id == "execution_id"
    assert execution.name == "execution_name"
    assert execution.triggered_by == "triggered_by"
    assert execution.triggered_on == time_triggered
    assert isinstance(execution.triggered_on_str, str)
    assert execution.started_on == time_triggered
    assert isinstance(execution.started_on_str, str)
    assert execution.ended_on == time_triggered
    assert isinstance(execution.ended_on_str, str)
    assert execution.status == "Finished"
    assert execution.kwargs
    assert execution.__repr__()
    assert execution.__str__()


@pytest.mark.integration
@pytest.mark.slow
def test_execution_class_lazy_properties(
    tabsserver_connection, testing_collection_with_table
):
    api_plan = tabsserver_connection.executions[0]
    assert api_plan.id
    execution = Execution(tabsserver_connection.connection, api_plan.id)
    assert api_plan.id == execution.id
    assert api_plan.name == execution.name
    assert isinstance(execution.collection, Collection)
    assert api_plan.collection == execution.collection
    assert isinstance(execution.function, Function)
    assert api_plan.function == execution.function
    execution.refresh()
    assert api_plan.triggered_by == execution.triggered_by
    assert execution.__repr__()
    assert execution.__str__()
    assert isinstance(execution.workers, list)
    assert all(isinstance(worker, Worker) for worker in execution.workers)
    assert isinstance(execution.function_runs, list)
    assert all(
        isinstance(function_run, FunctionRun)
        for function_run in execution.function_runs
    )


@pytest.mark.integration
@pytest.mark.requires_internet
def test_execution_class_cancel(tabsserver_connection):
    try:
        tabsserver_connection.create_collection(
            name="test_execution_class_cancel_collection",
            description="test_collection_description",
        )
        tabsserver_connection.register_function(
            collection_name="test_execution_class_cancel_collection",
            description="test_execution_class_cancel_description",
            function_path=(
                f"{os.path.join(ABSOLUTE_TEST_FOLDER_LOCATION, "testing_resources",
                                "test_input_plugin", "example.py")}::input_plugin"
            ),
            local_packages=LOCAL_PACKAGES_LIST,
        )
        execution = tabsserver_connection.trigger_function(
            "test_execution_class_cancel_collection", "test_input_plugin"
        )
        assert isinstance(execution, Execution)
        response = execution.cancel()
        assert response.status_code == 200
    finally:
        tabsserver_connection.delete_function(
            "test_execution_class_cancel_collection",
            "test_input_plugin",
            raise_for_status=False,
        )
        tabsserver_connection.delete_collection(
            "test_execution_class_cancel_collection", raise_for_status=False
        )


@pytest.mark.integration
@pytest.mark.requires_internet
@pytest.mark.skip(reason="Awaiting decision of behavior of recover method.")
def test_execution_class_recover(tabsserver_connection):
    try:
        tabsserver_connection.create_collection(
            name="test_execution_class_recover_collection",
            description="test_collection_description",
        )
        tabsserver_connection.register_function(
            collection_name="test_execution_class_recover_collection",
            description="test_execution_class_recover_description",
            function_path=(
                f"{os.path.join(ABSOLUTE_TEST_FOLDER_LOCATION, "testing_resources",
                                "test_input_plugin", "example.py")}::input_plugin"
            ),
            local_packages=LOCAL_PACKAGES_LIST,
        )
        execution = tabsserver_connection.trigger_function(
            "test_execution_class_recover_collection", "test_input_plugin"
        )
        assert isinstance(execution, Execution)
        response = execution.recover()
        assert response.status_code == 200
    finally:
        tabsserver_connection.delete_function(
            "test_execution_class_recover_collection",
            "test_input_plugin",
            raise_for_status=False,
        )
        tabsserver_connection.delete_collection(
            "test_execution_class_recover_collection", raise_for_status=False
        )


@pytest.mark.integration
@pytest.mark.slow
def test_execution_class_transactions(
    tabsserver_connection, testing_collection_with_table
):
    api_plan = tabsserver_connection.executions[0]
    assert api_plan.id
    execution = Execution(tabsserver_connection.connection, api_plan.id)
    assert isinstance(execution.transactions, list)
    assert all(
        isinstance(transaction, Transaction) for transaction in execution.transactions
    )
    for transaction in execution.transactions:
        assert transaction.execution == execution
