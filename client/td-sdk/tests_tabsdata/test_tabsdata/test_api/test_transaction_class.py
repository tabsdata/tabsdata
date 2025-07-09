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
def test_transaction_class(tabsserver_connection):
    time_triggered = int(time.time())
    transaction = Transaction(
        connection=tabsserver_connection.connection,
        id="transaction_id",
        name="transaction_name",
        dataset="function_name",
        triggered_by="triggered_by",
        triggered_on=time_triggered,
        status="C",
        random_kwarg="kwarg",
        started_on=time_triggered,
        ended_on=time_triggered,
    )
    assert transaction.id == "transaction_id"
    assert transaction.triggered_on == time_triggered
    assert isinstance(transaction.triggered_on_str, str)
    assert transaction.started_on == time_triggered
    assert isinstance(transaction.started_on_str, str)
    assert transaction.ended_on == time_triggered
    assert isinstance(transaction.ended_on_str, str)
    assert transaction.status == "Committed"
    assert transaction.kwargs
    assert transaction.__repr__()
    assert transaction.__str__()


@pytest.mark.integration
@pytest.mark.slow
def test_transaction_class_lazy_properties(
    tabsserver_connection, testing_collection_with_table
):
    api_transaction = tabsserver_connection.transactions[0]
    assert api_transaction.id
    transaction = Transaction(tabsserver_connection.connection, api_transaction.id)
    assert api_transaction.id == transaction.id
    execution = transaction.execution
    assert execution
    assert isinstance(execution, Execution)
    assert isinstance(execution.collection, Collection)
    assert isinstance(execution.function, Function)
    transaction.refresh()
    assert transaction.__repr__()
    assert transaction.__str__()
    assert isinstance(transaction.workers, list)
    assert all(isinstance(worker, Worker) for worker in transaction.workers)
    assert transaction.status
    assert transaction.triggered_by
    assert isinstance(transaction.collection, Collection)
    assert isinstance(transaction.function_runs, list)
    assert all(
        isinstance(function_run, FunctionRun)
        for function_run in transaction.function_runs
    )


@pytest.mark.integration
@pytest.mark.slow
def test_transaction_class_execution(
    tabsserver_connection, testing_collection_with_table
):
    api_transaction = tabsserver_connection.transactions[0]
    assert api_transaction.id
    transaction = Transaction(tabsserver_connection.connection, api_transaction.id)
    assert isinstance(transaction.execution, Execution)
    assert transaction in transaction.execution.transactions


@pytest.mark.integration
@pytest.mark.requires_internet
def test_transaction_class_cancel(tabsserver_connection):
    try:
        tabsserver_connection.create_collection(
            name="test_transaction_class_cancel_collection",
            description="test_collection_description",
        )
        tabsserver_connection.register_function(
            collection_name="test_transaction_class_cancel_collection",
            description="test_transaction_class_cancel_description",
            function_path=(
                f"{os.path.join(ABSOLUTE_TEST_FOLDER_LOCATION, "testing_resources",
                                "test_input_plugin", "example.py")}::input_plugin"
            ),
            local_packages=LOCAL_PACKAGES_LIST,
        )
        execution = tabsserver_connection.trigger_function(
            "test_transaction_class_cancel_collection", "test_input_plugin"
        )
        assert isinstance(execution, Execution)
        transaction = execution.transactions[0]
        response = transaction.cancel()
        assert response.status_code == 200
    finally:
        tabsserver_connection.delete_function(
            "test_transaction_class_cancel_collection",
            "test_input_plugin",
            raise_for_status=False,
        )
        tabsserver_connection.delete_collection(
            "test_transaction_class_cancel_collection", raise_for_status=False
        )


@pytest.mark.integration
@pytest.mark.requires_internet
@pytest.mark.skip(reason="Awaiting decision of behavior of recover method.")
def test_transaction_class_recover(tabsserver_connection):
    try:
        tabsserver_connection.create_collection(
            name="test_transaction_class_recover_collection",
            description="test_collection_description",
        )
        tabsserver_connection.register_function(
            collection_name="test_transaction_class_recover_collection",
            description="test_transaction_class_recover_description",
            function_path=(
                f"{os.path.join(ABSOLUTE_TEST_FOLDER_LOCATION, "testing_resources",
                                "test_input_plugin", "example.py")}::input_plugin"
            ),
            local_packages=LOCAL_PACKAGES_LIST,
        )
        execution = tabsserver_connection.trigger_function(
            "test_transaction_class_recover_collection", "test_input_plugin"
        )
        assert isinstance(execution, Execution)
        transaction = execution.transactions[0]
        response = transaction.recover()
        assert response.status_code == 200
    finally:
        tabsserver_connection.delete_function(
            "test_transaction_class_recover_collection",
            "test_input_plugin",
            raise_for_status=False,
        )
        tabsserver_connection.delete_collection(
            "test_transaction_class_recover_collection", raise_for_status=False
        )
