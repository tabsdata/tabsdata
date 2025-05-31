#
# Copyright 2025 Tabs Data Inc.
#

import logging

import pytest

from tabsdata.api.tabsdata_server import (
    Collection,
    Execution,
    Function,
    Transaction,
    Worker,
)

# noinspection PyUnresolvedReferences
from . import pytestmark  # noqa: F401

logger = logging.getLogger(__name__)
logger.setLevel(logging.DEBUG)


@pytest.mark.integration
@pytest.mark.requires_internet
def test_worker_class_lazy_properties(
    tabsserver_connection, testing_collection_with_table
):
    collection = Collection(
        tabsserver_connection.connection, testing_collection_with_table
    )
    function = collection.functions[0]
    workers = tabsserver_connection.list_workers(
        filter=[f"function:eq:{function.name}", f"collection:eq:{collection.name}"]
    )
    logger.debug(f"Workers: {workers}")
    assert workers
    assert isinstance(workers, list)
    assert all(isinstance(message, Worker) for message in workers)
    worker_id = workers[0].id
    logger.debug(f"Worker ID: {worker_id}")
    worker = Worker(tabsserver_connection.connection, worker_id)
    assert isinstance(worker.function, Function)
    assert worker.function == function
    assert worker in function.workers
    assert worker.collection == collection
    assert worker.__repr__()
    assert worker.__str__()
    assert worker.execution
    execution = worker.execution
    assert isinstance(execution, Execution)
    assert worker in execution.workers
    assert worker == execution.get_worker(worker.id)
    transaction = worker.transaction
    assert isinstance(transaction, Transaction)
    assert worker in transaction.workers
    assert worker == transaction.get_worker(worker.id)


@pytest.mark.integration
@pytest.mark.requires_internet
def test_worker_class_log(tabsserver_connection, testing_collection_with_table):
    collection = Collection(
        tabsserver_connection.connection, testing_collection_with_table
    )
    function = collection.functions[0]
    workers = tabsserver_connection.list_workers(
        filter=[f"function:eq:{function.name}", f"collection:eq:{collection.name}"]
    )
    logger.debug(f"Workers: {workers}")
    assert workers
    assert isinstance(workers, list)
    assert all(isinstance(message, Worker) for message in workers)
    worker_id = workers[0].id
    logger.debug(f"Worker ID: {worker_id}")
    worker = Worker(tabsserver_connection.connection, worker_id)
    log = worker.log
    assert isinstance(log, str)
