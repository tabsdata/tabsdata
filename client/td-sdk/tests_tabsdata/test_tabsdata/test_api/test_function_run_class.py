#
# Copyright 2025 Tabs Data Inc.
#

import logging

import pytest

from tabsdata._api.tabsdata_server import (
    Collection,
    Execution,
    Function,
    FunctionRun,
    Transaction,
)

# noinspection PyUnresolvedReferences
from . import pytestmark  # noqa: F401

logger = logging.getLogger(__name__)
logger.setLevel(logging.DEBUG)


@pytest.mark.integration
def test_function_run_class_lazy_properties(
    tabsserver_connection, testing_collection_with_table
):
    collection = Collection(
        tabsserver_connection.connection, testing_collection_with_table
    )
    function = collection.functions[0]
    function_runs = tabsserver_connection.list_function_runs(
        filter=[f"name:eq:{function.name}", f"collection:eq:{collection.name}"]
    )
    logger.debug(f"Function Runs: {function_runs}")
    assert function_runs
    assert isinstance(function_runs, list)
    assert all(isinstance(run, FunctionRun) for run in function_runs)
    run_id = function_runs[0].id
    logger.debug(f"Function Run ID: {run_id}")
    lazy_function_run = FunctionRun(tabsserver_connection.connection, run_id)
    assert isinstance(lazy_function_run.function, Function)
    assert lazy_function_run.function == function
    assert lazy_function_run in function.runs
    assert lazy_function_run.collection == collection
    assert lazy_function_run.__repr__()
    assert lazy_function_run.__str__()
    assert lazy_function_run.execution
    execution = lazy_function_run.execution
    assert isinstance(execution, Execution)
    transaction = lazy_function_run.transaction
    assert isinstance(transaction, Transaction)
    assert lazy_function_run.started_on
    assert lazy_function_run.started_on_str
    assert isinstance(lazy_function_run.started_on, int)
    assert isinstance(lazy_function_run.started_on_str, str)
