#
# Copyright 2025 Tabs Data Inc.
#

import logging
import os

import pytest
from click.testing import CliRunner

from tabsdata.cli.cli import cli

logger = logging.getLogger(__name__)
logger.setLevel(logging.DEBUG)


@pytest.mark.integration
@pytest.mark.requires_internet
def test_wrong_command_raises_exception(login):
    runner = CliRunner()
    result = runner.invoke(cli, ["exec", "potato"])
    assert result.exit_code == 2


@pytest.mark.integration
@pytest.mark.requires_internet
def test_execution_plan_list(login, testing_collection_with_table):
    runner = CliRunner()
    result = runner.invoke(cli, ["exec", "list-plans"])
    assert result.exit_code == 0


@pytest.mark.integration
@pytest.mark.requires_internet
def test_transaction_list(login, testing_collection_with_table):
    runner = CliRunner()
    result = runner.invoke(cli, ["exec", "list-trxs"])
    assert result.exit_code == 0


@pytest.mark.integration
@pytest.mark.requires_internet
def test_commit_list(login, testing_collection_with_table):
    runner = CliRunner()
    result = runner.invoke(cli, ["exec", "list-commits"])
    assert result.exit_code == 0


@pytest.mark.integration
@pytest.mark.requires_internet
def test_execution_plan_read(
    login, testing_collection_with_table, tabsserver_connection
):
    execution_plan_id = tabsserver_connection.execution_plans[0].id
    runner = CliRunner()
    result = runner.invoke(cli, ["exec", "show-plan", "--plan", execution_plan_id])
    logger.debug(result.output)
    assert result.exit_code == 0


@pytest.mark.integration
@pytest.mark.requires_internet
def test_exec_logs(login, testing_collection_with_table, tabsserver_connection):
    transaction_id = None
    for element in tabsserver_connection.transactions:
        if element.status in ("Failed", "Published"):
            transaction_id = element.id
            break
    logger.debug(f"Transactions: {tabsserver_connection.transactions}")
    logger.debug(f"Transaction ID: {transaction_id}")
    assert transaction_id
    messages = tabsserver_connection.worker_list(by_transaction_id=transaction_id)
    assert messages
    message_id = messages[0].id
    runner = CliRunner()
    result = runner.invoke(cli, ["exec", "logs", "--worker", message_id])
    logger.debug(result.output)
    assert result.exit_code == 0


@pytest.mark.integration
@pytest.mark.requires_internet
def test_exec_logs_to_file(
    login, testing_collection_with_table, tabsserver_connection, tmp_path
):
    destination = os.path.join(tmp_path, "logs.txt")
    transaction_id = None
    for element in tabsserver_connection.transactions:
        if element.status in ("Failed", "Published"):
            transaction_id = element.id
            break
    logger.debug(f"Transactions: {tabsserver_connection.transactions}")
    logger.debug(f"Transaction ID: {transaction_id}")
    assert transaction_id
    messages = tabsserver_connection.worker_list(by_transaction_id=transaction_id)
    assert messages
    message_id = messages[0].id
    runner = CliRunner()
    result = runner.invoke(
        cli, ["exec", "logs", "--worker", message_id, "--file", destination]
    )
    logger.debug(result.output)
    assert result.exit_code == 0
    assert os.path.exists(destination)


@pytest.mark.integration
@pytest.mark.requires_internet
def test_worker_messages_list_by_execution_plan_id(
    login, tabsserver_connection, testing_collection_with_table
):
    execution_plan_id = tabsserver_connection.execution_plans[0].id
    runner = CliRunner()
    result = runner.invoke(cli, ["exec", "list-workers", "--plan", execution_plan_id])
    logger.debug(result.output)
    assert result.exit_code == 0


@pytest.mark.integration
@pytest.mark.requires_internet
def test_worker_messages_list_by_transaction_id(
    login, tabsserver_connection, testing_collection_with_table
):
    transaction_id = None
    for element in tabsserver_connection.transactions:
        if element.status in ("Failed", "Published"):
            transaction_id = element.id
            break
    logger.debug(f"Transactions: {tabsserver_connection.transactions}")
    logger.debug(f"Transaction ID: {transaction_id}")
    assert transaction_id
    runner = CliRunner()
    result = runner.invoke(cli, ["exec", "list-workers", "--trx", transaction_id])
    logger.debug(result.output)
    assert result.exit_code == 0


@pytest.mark.integration
@pytest.mark.requires_internet
def test_worker_messages_list_by_function_id(
    login, tabsserver_connection, testing_collection_with_table
):
    function_name = tabsserver_connection.collection_list_functions(
        testing_collection_with_table
    )[0].name
    function_id = tabsserver_connection.function_get(
        testing_collection_with_table, function_name
    ).id
    runner = CliRunner()
    result = runner.invoke(cli, ["exec", "list-workers", "--fn", function_id])
    logger.debug(result.output)
    assert result.exit_code == 0


@pytest.mark.integration
@pytest.mark.requires_internet
def test_worker_messages_list_by_data_version_id(
    login, tabsserver_connection, testing_collection_with_table
):
    function_name = tabsserver_connection.collection_list_functions(
        testing_collection_with_table
    )[0].name
    data_version = tabsserver_connection.dataversion_list(
        testing_collection_with_table, function_name
    )[0].id
    runner = CliRunner()
    result = runner.invoke(cli, ["exec", "list-workers", "--data-ver", data_version])
    logger.debug(result.output)
    assert result.exit_code == 0


@pytest.mark.integration
@pytest.mark.requires_internet
def test_worker_messages_list_by_all_options_fails(
    login, tabsserver_connection, testing_collection_with_table
):
    execution_plan_id = tabsserver_connection.execution_plans[0].id
    function_name = tabsserver_connection.collection_list_functions(
        testing_collection_with_table
    )[0].name
    data_version = tabsserver_connection.dataversion_list(
        testing_collection_with_table, function_name
    )[0].id
    function_id = tabsserver_connection.collection_list_functions(
        testing_collection_with_table
    )[0].id
    transaction_id = None
    for element in tabsserver_connection.transactions:
        if element.status in ("Failed", "Published"):
            transaction_id = element.id
            break
    logger.debug(f"Transactions: {tabsserver_connection.transactions}")
    logger.debug(f"Transaction ID: {transaction_id}")
    assert transaction_id
    runner = CliRunner()
    result = runner.invoke(
        cli,
        [
            "exec",
            "list-workers",
            "--fn",
            function_id,
            "--data-ver",
            data_version,
            "--trx",
            transaction_id,
            "--plan",
            execution_plan_id,
        ],
    )
    logger.debug(result.output)
    assert result.exit_code != 0


@pytest.mark.integration
@pytest.mark.requires_internet
def test_worker_messages_list_no_options_fails(
    login, tabsserver_connection, testing_collection_with_table
):
    runner = CliRunner()
    result = runner.invoke(cli, ["exec", "list-workers"])
    logger.debug(result.output)
    assert result.exit_code != 0
