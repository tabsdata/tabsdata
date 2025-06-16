#
# Copyright 2025 Tabs Data Inc.
#

import logging
import os
import time

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
    result = runner.invoke(cli, ["--no-prompt", "exec", "list-exec"])
    assert result.exit_code == 0


@pytest.mark.integration
@pytest.mark.requires_internet
def test_transaction_list(login, testing_collection_with_table):
    runner = CliRunner()
    result = runner.invoke(cli, ["--no-prompt", "exec", "list-trx"])
    assert result.exit_code == 0


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
        result = runner.invoke(
            cli, ["--no-prompt", "exec", "cancel-exec", execution.id]
        )
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
        result = runner.invoke(
            cli, ["--no-prompt", "exec", "recover-exec", execution.id]
        )
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
            cli, ["--no-prompt", "exec", "cancel-trx", transaction.id]
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
            cli, ["--no-prompt", "exec", "recover-trx", transaction.id]
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


@pytest.mark.integration
@pytest.mark.requires_internet
def test_execution_list_with_status(login, testing_collection_with_table):
    runner = CliRunner()
    result = runner.invoke(
        cli, ["--no-prompt", "exec", "list-exec", "--status", "published"]
    )
    logger.debug(result.output)
    assert result.exit_code == 0


@pytest.mark.integration
@pytest.mark.requires_internet
def test_execution_list_with_multiple_statuses(login, testing_collection_with_table):
    runner = CliRunner()
    result = runner.invoke(
        cli,
        [
            "--no-prompt",
            "exec",
            "list-exec",
            "--status",
            "published",
            "--status",
            "failed",
        ],
    )
    logger.debug(result.output)
    assert result.exit_code == 0


@pytest.mark.integration
@pytest.mark.requires_internet
def test_execution_list_with_wrong_status_fails(login, testing_collection_with_table):
    runner = CliRunner()
    result = runner.invoke(
        cli, ["--no-prompt", "exec", "list-exec", "--status", "doesnotexist"]
    )
    logger.debug(result.output)
    assert result.exit_code != 0


@pytest.mark.integration
@pytest.mark.requires_internet
def test_execution_list_with_function_name(login, testing_collection_with_table):
    runner = CliRunner()
    result = runner.invoke(
        cli,
        ["--no-prompt", "exec", "list-exec", "--fn", "input_file_csv_string_format"],
    )
    logger.debug(result.output)
    assert result.exit_code == 0


@pytest.mark.integration
@pytest.mark.requires_internet
def test_execution_list_with_collection_name(login, testing_collection_with_table):
    runner = CliRunner()
    result = runner.invoke(
        cli,
        [
            "--no-prompt",
            "exec",
            "list-exec",
            "--collection",
            testing_collection_with_table,
        ],
    )
    logger.debug(result.output)
    assert result.exit_code == 0


@pytest.mark.integration
@pytest.mark.requires_internet
def test_execution_list_last(login, testing_collection_with_table):
    runner = CliRunner()
    result = runner.invoke(cli, ["--no-prompt", "exec", "list-exec", "--last"])
    logger.debug(result.output)
    assert result.exit_code == 0


@pytest.mark.integration
@pytest.mark.requires_internet
def test_execution_list_with_wildcard_name(login, testing_collection_with_table):
    runner = CliRunner()
    result = runner.invoke(
        cli, ["--no-prompt", "exec", "list-exec", "--name", "test_*"]
    )
    logger.debug(result.output)
    assert result.exit_code == 0


@pytest.mark.integration
@pytest.mark.requires_internet
def test_execution_list_with_at(login, testing_collection_with_table):
    epoch_ms = int(time.time() * 1000)
    runner = CliRunner()
    result = runner.invoke(
        cli, ["--no-prompt", "exec", "list-exec", "--at", str(epoch_ms)]
    )
    logger.debug(result.output)
    assert result.exit_code == 0


@pytest.mark.integration
@pytest.mark.requires_internet
def test_transaction_list_with_status(login, testing_collection_with_table):
    runner = CliRunner()
    result = runner.invoke(
        cli, ["--no-prompt", "exec", "list-trx", "--status", "published"]
    )
    logger.debug(result.output)
    assert result.exit_code == 0


@pytest.mark.integration
@pytest.mark.requires_internet
def test_transaction_list_with_multiple_statuses(login, testing_collection_with_table):
    runner = CliRunner()
    result = runner.invoke(
        cli,
        [
            "--no-prompt",
            "exec",
            "list-trx",
            "--status",
            "published",
            "--status",
            "failed",
        ],
    )
    logger.debug(result.output)
    assert result.exit_code == 0


@pytest.mark.integration
@pytest.mark.requires_internet
def test_transaction_list_with_wrong_status_fails(login, testing_collection_with_table):
    runner = CliRunner()
    result = runner.invoke(
        cli, ["--no-prompt", "exec", "list-trx", "--status", "doesnotexist"]
    )
    logger.debug(result.output)
    assert result.exit_code != 0


@pytest.mark.integration
@pytest.mark.requires_internet
def test_transaction_list_with_execution_id(
    login, testing_collection_with_table, tabsserver_connection
):
    execution_id = tabsserver_connection.executions[0].id
    runner = CliRunner()
    result = runner.invoke(
        cli,
        ["--no-prompt", "exec", "list-trx", "--exec", str(execution_id)],
    )
    logger.debug(result.output)
    assert result.exit_code == 0


@pytest.mark.integration
@pytest.mark.requires_internet
def test_transaction_list_with_collection_name(login, testing_collection_with_table):
    runner = CliRunner()
    result = runner.invoke(
        cli,
        [
            "--no-prompt",
            "exec",
            "list-trx",
            "--collection",
            testing_collection_with_table,
        ],
    )
    logger.debug(result.output)
    assert result.exit_code == 0


@pytest.mark.integration
@pytest.mark.requires_internet
def test_transaction_list_last(login, testing_collection_with_table):
    runner = CliRunner()
    result = runner.invoke(cli, ["--no-prompt", "exec", "list-trx", "--last"])
    logger.debug(result.output)
    assert result.exit_code == 0


@pytest.mark.integration
@pytest.mark.requires_internet
def test_transaction_list_with_wildcard_name(login, testing_collection_with_table):
    runner = CliRunner()
    result = runner.invoke(
        cli, ["--no-prompt", "exec", "list-trx", "--exec-name", "test_*"]
    )
    logger.debug(result.output)
    assert result.exit_code == 0


@pytest.mark.integration
@pytest.mark.requires_internet
def test_transaction_list_with_at(login, testing_collection_with_table):
    epoch_ms = int(time.time() * 1000)
    runner = CliRunner()
    result = runner.invoke(
        cli, ["--no-prompt", "exec", "list-trx", "--at", str(epoch_ms)]
    )
    logger.debug(result.output)
    assert result.exit_code == 0


@pytest.mark.integration
@pytest.mark.requires_internet
def test_worker_list_with_status(login, testing_collection_with_table):
    runner = CliRunner()
    result = runner.invoke(
        cli, ["--no-prompt", "exec", "list-worker", "--status", "published"]
    )
    logger.debug(result.output)
    assert result.exit_code == 0


@pytest.mark.integration
@pytest.mark.requires_internet
def test_worker_list_with_multiple_statuses(login, testing_collection_with_table):
    runner = CliRunner()
    result = runner.invoke(
        cli,
        [
            "--no-prompt",
            "exec",
            "list-worker",
            "--status",
            "published",
            "--status",
            "failed",
        ],
    )
    logger.debug(result.output)
    assert result.exit_code == 0


@pytest.mark.integration
@pytest.mark.requires_internet
def test_worker_list_with_wrong_status_fails(login, testing_collection_with_table):
    runner = CliRunner()
    result = runner.invoke(
        cli, ["--no-prompt", "exec", "list-worker", "--status", "doesnotexist"]
    )
    logger.debug(result.output)
    assert result.exit_code != 0


@pytest.mark.integration
@pytest.mark.requires_internet
def test_worker_list_with_only_function_name_fails(
    login, testing_collection_with_table
):
    runner = CliRunner()
    result = runner.invoke(
        cli,
        ["--no-prompt", "exec", "list-worker", "--fn", "input_file_csv_string_format"],
    )
    logger.debug(result.output)
    assert result.exit_code != 0


@pytest.mark.integration
@pytest.mark.requires_internet
def test_worker_list_with_collection_name(login, testing_collection_with_table):
    runner = CliRunner()
    result = runner.invoke(
        cli,
        [
            "--no-prompt",
            "exec",
            "list-worker",
            "--collection",
            testing_collection_with_table,
        ],
    )
    logger.debug(result.output)
    assert result.exit_code == 0


@pytest.mark.integration
@pytest.mark.requires_internet
def test_worker_list_with_wildcard_execution_name(login, testing_collection_with_table):
    runner = CliRunner()
    result = runner.invoke(
        cli, ["--no-prompt", "exec", "list-worker", "--exec-name", "test_*"]
    )
    logger.debug(result.output)
    assert result.exit_code == 0


@pytest.mark.integration
@pytest.mark.requires_internet
def test_worker_list_with_execution_id(
    login, testing_collection_with_table, tabsserver_connection
):
    execution_id = tabsserver_connection.executions[0].id
    runner = CliRunner()
    result = runner.invoke(
        cli, ["--no-prompt", "exec", "list-worker", "--exec", str(execution_id)]
    )
    logger.debug(result.output)
    assert result.exit_code == 0


@pytest.mark.integration
@pytest.mark.requires_internet
def test_worker_messages_list_by_execution_id(
    login, tabsserver_connection, testing_collection_with_table
):
    execution_id = tabsserver_connection.executions[0].id
    runner = CliRunner()
    result = runner.invoke(
        cli, ["--no-prompt", "exec", "list-worker", "--exec", execution_id]
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
        cli, ["--no-prompt", "exec", "list-worker", "--trx", transaction_id]
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
            "list-worker",
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
            "list-worker",
            "--fn",
            function_name,
            "--trx",
            transaction_id,
            "--exec",
            execution_id,
            "--collection",
            testing_collection_with_table,
        ],
    )
    logger.debug(result.output)
    assert result.exit_code != 0


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
    result = runner.invoke(
        cli, ["--no-prompt", "exec", "worker-logs", "--worker", message_id]
    )
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
        [
            "--no-prompt",
            "exec",
            "worker-logs",
            "--worker",
            message_id,
            "--file",
            destination,
        ],
    )
    logger.debug(result.output)
    assert result.exit_code == 0
    assert os.path.exists(destination)


@pytest.mark.integration
@pytest.mark.requires_internet
def test_exec_logs_collection(login, testing_collection_with_table):
    runner = CliRunner()
    result = runner.invoke(
        cli,
        [
            "--no-prompt",
            "exec",
            "worker-logs",
            "--collection",
            testing_collection_with_table,
        ],
    )
    logger.debug(result.output)
    assert result.exit_code == 0


@pytest.mark.integration
@pytest.mark.requires_internet
def test_exec_logs_collection_to_file(login, testing_collection_with_table, tmp_path):
    destination = os.path.join(tmp_path, "logs.txt")
    runner = CliRunner()
    result = runner.invoke(
        cli,
        [
            "--no-prompt",
            "exec",
            "worker-logs",
            "--collection",
            testing_collection_with_table,
            "--file",
            destination,
        ],
    )
    logger.debug(result.output)
    assert result.exit_code == 0


@pytest.mark.integration
@pytest.mark.requires_internet
def test_exec_logs_collection_to_file_with_prompt(
    login, testing_collection_with_table, tmp_path
):
    destination = os.path.join(tmp_path, "logs.txt")
    runner = CliRunner()
    result = runner.invoke(
        cli,
        [
            "exec",
            "worker-logs",
            "--collection",
            testing_collection_with_table,
            "--file",
            destination,
        ],
        input="\n",
    )
    logger.debug(result.output)
    assert result.exit_code == 0


@pytest.mark.integration
@pytest.mark.requires_internet
def test_exec_logs_transaction(
    login, testing_collection_with_table, tabsserver_connection
):
    transaction_id = None
    for element in tabsserver_connection.transactions:
        if element.status in ("Failed", "Published"):
            transaction_id = element.id
            break
    assert transaction_id
    runner = CliRunner()
    result = runner.invoke(
        cli, ["--no-prompt", "exec", "worker-logs", "--trx", transaction_id]
    )
    logger.debug(result.output)
    assert result.exit_code == 0


@pytest.mark.integration
@pytest.mark.requires_internet
def test_exec_logs_transaction_to_file(
    login, testing_collection_with_table, tmp_path, tabsserver_connection
):
    destination = os.path.join(tmp_path, "logs.txt")
    transaction_id = None
    for element in tabsserver_connection.transactions:
        if element.status in ("Failed", "Published"):
            transaction_id = element.id
            break
    assert transaction_id
    runner = CliRunner()
    result = runner.invoke(
        cli,
        [
            "--no-prompt",
            "exec",
            "worker-logs",
            "--trx",
            transaction_id,
            "--file",
            destination,
        ],
    )
    logger.debug(result.output)
    assert result.exit_code == 0


@pytest.mark.integration
@pytest.mark.requires_internet
def test_exec_logs_transaction_to_file_with_prompt(
    login, testing_collection_with_table, tmp_path, tabsserver_connection
):
    destination = os.path.join(tmp_path, "logs.txt")
    transaction_id = None
    for element in tabsserver_connection.transactions:
        if element.status in ("Failed", "Published"):
            transaction_id = element.id
            break
    assert transaction_id
    runner = CliRunner()
    result = runner.invoke(
        cli,
        ["exec", "worker-logs", "--trx", transaction_id, "--file", destination],
        input="\n",
    )
    logger.debug(result.output)
    assert result.exit_code == 0


@pytest.mark.integration
@pytest.mark.requires_internet
def test_exec_info(login, testing_collection_with_table, tabsserver_connection):
    exec_id = tabsserver_connection.executions[0].id
    runner = CliRunner()
    result = runner.invoke(
        cli,
        ["--no-prompt", "exec", "info", exec_id],
    )
    logger.debug(result.output)
    assert result.exit_code == 0
