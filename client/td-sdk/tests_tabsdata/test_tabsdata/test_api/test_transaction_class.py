#
# Copyright 2025 Tabs Data Inc.
#

import time

import pytest

from tabsdata.api.tabsdata_server import Collection, Function, Transaction, Worker


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
        status="F",
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
    assert transaction.status == "Failed"
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
    execution_plan = transaction.execution_plan
    assert execution_plan.name
    assert isinstance(execution_plan.collection, Collection)
    assert isinstance(execution_plan.function, Function)
    transaction.refresh()
    assert transaction.__repr__()
    assert transaction.__str__()
    assert isinstance(transaction.workers, list)
    assert all(isinstance(worker, Worker) for worker in transaction.workers)
    assert transaction.status
