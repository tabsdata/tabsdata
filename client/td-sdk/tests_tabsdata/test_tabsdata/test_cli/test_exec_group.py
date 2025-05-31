#
# Copyright 2025 Tabs Data Inc.
#

import logging
import os

import pytest
from click.testing import CliRunner
from tests_tabsdata.conftest import ABSOLUTE_TEST_FOLDER_LOCATION, LOCAL_PACKAGES_LIST

from tabsdata.api.tabsdata_server import Execution
from tabsdata.cli.cli import cli

# noinspection PyUnresolvedReferences
from . import pytestmark  # noqa: F401

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
def test_execution_list(login, testing_collection_with_table):
    runner = CliRunner()
    result = runner.invoke(cli, ["--no-prompt", "exec", "list"])
    assert result.exit_code == 0


@pytest.mark.integration
@pytest.mark.requires_internet
def test_transaction_list(login, testing_collection_with_table):
    runner = CliRunner()
    result = runner.invoke(cli, ["--no-prompt", "exec", "list-trxs"])
    assert result.exit_code == 0


@pytest.mark.integration
@pytest.mark.requires_internet
def test_transaction_list_published(login, testing_collection_with_table):
    runner = CliRunner()
    result = runner.invoke(cli, ["--no-prompt", "exec", "list-trxs", "--published"])
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
    messages = tabsserver_connection.list_workers(
        filter=[f"transaction_id:eq:{transaction_id}"]
    )
    assert messages
    message_id = messages[0].id
    runner = CliRunner()
    result = runner.invoke(cli, ["--no-prompt", "exec", "logs", "--worker", message_id])
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
    messages = tabsserver_connection.list_workers(
        filter=[f"transaction_id:eq:{transaction_id}"]
    )
    assert messages
    message_id = messages[0].id
    runner = CliRunner()
    result = runner.invoke(
        cli,
        ["--no-prompt", "exec", "logs", "--worker", message_id, "--file", destination],
    )
    logger.debug(result.output)
    assert result.exit_code == 0
    assert os.path.exists(destination)


@pytest.mark.integration
@pytest.mark.requires_internet
def test_worker_messages_list_by_execution_id(
    login, tabsserver_connection, testing_collection_with_table
):
    execution_id = tabsserver_connection.executions[0].id
    runner = CliRunner()
    result = runner.invoke(
        cli, ["--no-prompt", "exec", "list-workers", "--execution", execution_id]
    )
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
    result = runner.invoke(
        cli, ["--no-prompt", "exec", "list-workers", "--trx", transaction_id]
    )
    logger.debug(result.output)
    assert result.exit_code == 0


@pytest.mark.integration
@pytest.mark.requires_internet
def test_worker_messages_list_by_function_and_collection(
    login, tabsserver_connection, testing_collection_with_table
):
    function_name = tabsserver_connection.list_functions(testing_collection_with_table)[
        0
    ].name
    runner = CliRunner()
    result = runner.invoke(
        cli,
        [
            "--no-prompt",
            "exec",
            "list-workers",
            "--fn",
            function_name,
            "--collection",
            testing_collection_with_table,
        ],
    )
    logger.debug(result.output)
    assert result.exit_code == 0


@pytest.mark.integration
@pytest.mark.requires_internet
def test_worker_messages_list_by_all_options_fails(
    login, tabsserver_connection, testing_collection_with_table
):
    execution_id = tabsserver_connection.executions[0].id
    function_name = tabsserver_connection.list_functions(testing_collection_with_table)[
        0
    ].name
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
            "--no-prompt",
            "exec",
            "list-workers",
            "--fn",
            function_name,
            "--trx",
            transaction_id,
            "--execution",
            execution_id,
            "--collection",
            testing_collection_with_table,
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
    result = runner.invoke(cli, ["--no-prompt", "exec", "list-workers"])
    logger.debug(result.output)
    assert result.exit_code != 0


@pytest.mark.integration
@pytest.mark.requires_internet
def test_cli_execution_cancel(login, tabsserver_connection):
    try:
        tabsserver_connection.create_collection(
            name="test_cli_execution_cancel_collection",
            description="test_collection_description",
        )
        tabsserver_connection.register_function(
            collection_name="test_cli_execution_cancel_collection",
            description="test_cli_execution_cancel_description",
            function_path=(
                f"{os.path.join(ABSOLUTE_TEST_FOLDER_LOCATION, "testing_resources",
                                "test_input_plugin", "example.py")}::input_plugin"
            ),
            local_packages=LOCAL_PACKAGES_LIST,
        )
        execution = tabsserver_connection.trigger_function(
            "test_cli_execution_cancel_collection", "test_input_plugin"
        )
        assert isinstance(execution, Execution)
        runner = CliRunner()
        result = runner.invoke(cli, ["--no-prompt", "exec", "cancel", execution.id])
        logger.debug(result.output)
        assert result.exit_code == 0
    finally:
        tabsserver_connection.delete_function(
            "test_cli_execution_cancel_collection",
            "test_input_plugin",
            raise_for_status=False,
        )
        tabsserver_connection.delete_collection(
            "test_cli_execution_cancel_collection", raise_for_status=False
        )


@pytest.mark.integration
@pytest.mark.requires_internet
@pytest.mark.skip(reason="Awaiting decision of behavior of recover method.")
def test_cli_execution_recover(login, tabsserver_connection):
    try:
        tabsserver_connection.create_collection(
            name="test_cli_execution_recover_collection",
            description="test_collection_description",
        )
        tabsserver_connection.register_function(
            collection_name="test_cli_execution_recover_collection",
            description="test_cli_execution_recover_description",
            function_path=(
                f"{os.path.join(ABSOLUTE_TEST_FOLDER_LOCATION, "testing_resources",
                                "test_input_plugin", "example.py")}::input_plugin"
            ),
            local_packages=LOCAL_PACKAGES_LIST,
        )
        execution = tabsserver_connection.trigger_function(
            "test_cli_execution_recover_collection", "test_input_plugin"
        )
        assert isinstance(execution, Execution)
        runner = CliRunner()
        result = runner.invoke(cli, ["--no-prompt", "exec", "recover", execution.id])
        logger.debug(result.output)
        assert result.exit_code == 0
    finally:
        tabsserver_connection.delete_function(
            "test_cli_execution_recover_collection",
            "test_input_plugin",
            raise_for_status=False,
        )
        tabsserver_connection.delete_collection(
            "test_cli_execution_recover_collection", raise_for_status=False
        )


@pytest.mark.integration
@pytest.mark.requires_internet
def test_cli_transaction_cancel(login, tabsserver_connection):
    try:
        tabsserver_connection.create_collection(
            name="test_cli_transaction_cancel_collection",
            description="test_collection_description",
        )
        tabsserver_connection.register_function(
            collection_name="test_cli_transaction_cancel_collection",
            description="test_cli_transaction_cancel_description",
            function_path=(
                f"{os.path.join(ABSOLUTE_TEST_FOLDER_LOCATION, "testing_resources",
                                "test_input_plugin", "example.py")}::input_plugin"
            ),
            local_packages=LOCAL_PACKAGES_LIST,
        )
        execution = tabsserver_connection.trigger_function(
            "test_cli_transaction_cancel_collection", "test_input_plugin"
        )
        assert isinstance(execution, Execution)
        transaction = execution.transactions[0]
        runner = CliRunner()
        result = runner.invoke(
            cli, ["--no-prompt", "exec", "cancel-trx", "--trx", transaction.id]
        )
        logger.debug(result.output)
        assert result.exit_code == 0
    finally:
        tabsserver_connection.delete_function(
            "test_cli_transaction_cancel_collection",
            "test_input_plugin",
            raise_for_status=False,
        )
        tabsserver_connection.delete_collection(
            "test_cli_transaction_cancel_collection", raise_for_status=False
        )


@pytest.mark.integration
@pytest.mark.requires_internet
@pytest.mark.skip(reason="Awaiting decision of behavior of recover method.")
def test_cli_transaction_recover(login, tabsserver_connection):
    try:
        tabsserver_connection.create_collection(
            name="test_cli_transaction_recover_collection",
            description="test_collection_description",
        )
        tabsserver_connection.register_function(
            collection_name="test_cli_transaction_recover_collection",
            description="test_cli_transaction_recover_description",
            function_path=(
                f"{os.path.join(ABSOLUTE_TEST_FOLDER_LOCATION, "testing_resources",
                                "test_input_plugin", "example.py")}::input_plugin"
            ),
            local_packages=LOCAL_PACKAGES_LIST,
        )
        execution = tabsserver_connection.trigger_function(
            "test_cli_transaction_recover_collection", "test_input_plugin"
        )
        assert isinstance(execution, Execution)
        transaction = execution.transactions[0]
        runner = CliRunner()
        result = runner.invoke(
            cli, ["--no-prompt", "exec", "recover-trx", "--trx", transaction.id]
        )
        logger.debug(result.output)
        assert result.exit_code == 0
    finally:
        tabsserver_connection.delete_function(
            "test_cli_transaction_recover_collection",
            "test_input_plugin",
            raise_for_status=False,
        )
        tabsserver_connection.delete_collection(
            "test_cli_transaction_recover_collection", raise_for_status=False
        )
