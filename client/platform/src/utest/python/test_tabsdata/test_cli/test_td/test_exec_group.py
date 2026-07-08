#
# Copyright 2025 Tabs Data Inc.
#

import logging
import os
import time

import pytest
from click.testing import CliRunner

from tabsdata._cli.td.cli import cli
from tabsdata._cli.td.exe_group import (
    obtain_list_exec_filters,
    obtain_list_fn_run_filters,
    obtain_list_trx_filters,
    obtain_list_worker_filters,
)
from tabsdata.api.status_utils.execution import EXECUTION_FINAL_STATUSES
from tabsdata.api.status_utils.transaction import TRANSACTION_FINAL_STATUSES
from tabsdata.api.tabsdata_server import Execution
from test_tabsdata.conftest import (
    ABSOLUTE_TEST_FOLDER_LOCATION,
    LOCAL_PACKAGES_LIST,
    TESTING_RESOURCES_FOLDER,
)

# noinspection PyUnresolvedReferences
from . import pytestmark  # noqa: F401

logger = logging.getLogger(__name__)
logger.setLevel(logging.DEBUG)


@pytest.mark.integration
@pytest.mark.requires_internet
def test_wrong_command_raises_exception(login):
    runner = CliRunner()
    result = runner.invoke(cli, ["exe", "potato"])
    assert result.exit_code == 2


@pytest.mark.integration
@pytest.mark.requires_internet
def test_execution_list(login, testing_collection_with_table):
    runner = CliRunner()
    result = runner.invoke(cli, ["--no-prompt", "exe", "list-plan"])
    assert result.exit_code == 0


@pytest.mark.integration
@pytest.mark.requires_internet
def test_transaction_list(login, testing_collection_with_table):
    runner = CliRunner()
    result = runner.invoke(cli, ["--no-prompt", "exe", "list-trx"])
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
            function_path=f"{os.path.join(TESTING_RESOURCES_FOLDER,
                                "test_input_plugin", "example.py")}::input_plugin",
            local_packages=LOCAL_PACKAGES_LIST,
        )
        execution = tabsserver_connection.trigger_function(
            "test_cli_execution_cancel_collection", "test_input_plugin"
        )
        assert isinstance(execution, Execution)
        runner = CliRunner()
        result = runner.invoke(
            cli, ["--no-prompt", "exe", "cancel", "--plan", execution.id]
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
            function_path=f"{os.path.join(TESTING_RESOURCES_FOLDER,
                                "test_input_plugin", "example.py")}::input_plugin",
            local_packages=LOCAL_PACKAGES_LIST,
        )
        execution = tabsserver_connection.trigger_function(
            "test_cli_execution_recover_collection", "test_input_plugin"
        )
        assert isinstance(execution, Execution)
        runner = CliRunner()
        result = runner.invoke(
            cli, ["--no-prompt", "exe", "recover", "--plan", execution.id]
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
            function_path=f"{os.path.join(TESTING_RESOURCES_FOLDER,
                                "test_input_plugin", "example.py")}::input_plugin",
            local_packages=LOCAL_PACKAGES_LIST,
        )
        execution = tabsserver_connection.trigger_function(
            "test_cli_transaction_cancel_collection", "test_input_plugin"
        )
        assert isinstance(execution, Execution)
        transaction = execution.transactions[0]
        runner = CliRunner()
        result = runner.invoke(
            cli, ["--no-prompt", "exe", "cancel", "--trx", transaction.id]
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
            function_path=f"{os.path.join(TESTING_RESOURCES_FOLDER,
                                "test_input_plugin", "example.py")}::input_plugin",
            local_packages=LOCAL_PACKAGES_LIST,
        )
        execution = tabsserver_connection.trigger_function(
            "test_cli_transaction_recover_collection", "test_input_plugin"
        )
        assert isinstance(execution, Execution)
        transaction = execution.transactions[0]
        runner = CliRunner()
        result = runner.invoke(
            cli, ["--no-prompt", "exe", "recover", "--trx", transaction.id]
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
    first_status = list(EXECUTION_FINAL_STATUSES)[0]
    result = runner.invoke(
        cli,
        [
            "--no-prompt",
            "exe",
            "list-plan",
            "--status",
            first_status,
        ],
    )
    logger.debug(result.output)
    assert result.exit_code == 0


@pytest.mark.integration
@pytest.mark.requires_internet
def test_execution_list_with_multiple_statuses(login, testing_collection_with_table):
    runner = CliRunner()
    execution_status_list = list(EXECUTION_FINAL_STATUSES)
    first_status = execution_status_list[0]
    second_status = execution_status_list[1]
    result = runner.invoke(
        cli,
        [
            "--no-prompt",
            "exe",
            "list-plan",
            "--status",
            first_status,
            "--status",
            second_status,
        ],
    )
    logger.debug(result.output)
    assert result.exit_code == 0


@pytest.mark.integration
@pytest.mark.requires_internet
def test_execution_list_with_wrong_status_fails(login, testing_collection_with_table):
    runner = CliRunner()
    result = runner.invoke(
        cli, ["--no-prompt", "exe", "list-plan", "--status", "doesnotexist"]
    )
    logger.debug(result.output)
    assert result.exit_code != 0


@pytest.mark.integration
@pytest.mark.requires_internet
def test_execution_list_with_function_name(login, testing_collection_with_table):
    runner = CliRunner()
    result = runner.invoke(
        cli,
        ["--no-prompt", "exe", "list-plan", "--fn", "input_file_csv_string_format"],
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
            "exe",
            "list-plan",
            "--coll",
            testing_collection_with_table,
        ],
    )
    logger.debug(result.output)
    assert result.exit_code == 0


@pytest.mark.integration
@pytest.mark.requires_internet
def test_execution_list_last(login, testing_collection_with_table):
    runner = CliRunner()
    result = runner.invoke(cli, ["--no-prompt", "exe", "list-plan", "--last"])
    logger.debug(result.output)
    assert result.exit_code == 0


@pytest.mark.integration
@pytest.mark.requires_internet
def test_execution_list_with_wildcard_name(login, testing_collection_with_table):
    runner = CliRunner()
    result = runner.invoke(cli, ["--no-prompt", "exe", "list-plan", "--name", "test_*"])
    logger.debug(result.output)
    assert result.exit_code == 0


@pytest.mark.integration
@pytest.mark.requires_internet
def test_execution_list_with_at(login, testing_collection_with_table):
    epoch_ms = int(time.time() * 1000)
    runner = CliRunner()
    result = runner.invoke(
        cli, ["--no-prompt", "exe", "list-plan", "--at", str(epoch_ms)]
    )
    logger.debug(result.output)
    assert result.exit_code == 0


@pytest.mark.integration
@pytest.mark.requires_internet
def test_transaction_list_with_status(login, testing_collection_with_table):
    runner = CliRunner()
    first_status = list(TRANSACTION_FINAL_STATUSES)[0]
    result = runner.invoke(
        cli,
        [
            "--no-prompt",
            "exe",
            "list-trx",
            "--status",
            first_status,
        ],
    )
    logger.debug(result.output)
    assert result.exit_code == 0


@pytest.mark.integration
@pytest.mark.requires_internet
def test_transaction_list_with_multiple_statuses(login, testing_collection_with_table):
    runner = CliRunner()
    list_of_status = list(TRANSACTION_FINAL_STATUSES)
    first_status = list_of_status[0]
    second_status = list_of_status[1]
    result = runner.invoke(
        cli,
        [
            "--no-prompt",
            "exe",
            "list-trx",
            "--status",
            first_status,
            "--status",
            second_status,
        ],
    )
    logger.debug(result.output)
    assert result.exit_code == 0


@pytest.mark.integration
@pytest.mark.requires_internet
def test_transaction_list_with_wrong_status_fails(login, testing_collection_with_table):
    runner = CliRunner()
    result = runner.invoke(
        cli, ["--no-prompt", "exe", "list-trx", "--status", "doesnotexist"]
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
        ["--no-prompt", "exe", "list-trx", "--plan", str(execution_id)],
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
            "exe",
            "list-trx",
            "--coll",
            testing_collection_with_table,
        ],
    )
    logger.debug(result.output)
    assert result.exit_code == 0


@pytest.mark.integration
@pytest.mark.requires_internet
def test_transaction_list_last(login, testing_collection_with_table):
    runner = CliRunner()
    result = runner.invoke(cli, ["--no-prompt", "exe", "list-trx", "--last"])
    logger.debug(result.output)
    assert result.exit_code == 0


@pytest.mark.integration
@pytest.mark.requires_internet
def test_transaction_list_with_wildcard_name(login, testing_collection_with_table):
    runner = CliRunner()
    result = runner.invoke(
        cli, ["--no-prompt", "exe", "list-trx", "--plan-name", "test_*"]
    )
    logger.debug(result.output)
    assert result.exit_code == 0


@pytest.mark.integration
@pytest.mark.requires_internet
def test_transaction_list_with_at(login, testing_collection_with_table):
    epoch_ms = int(time.time() * 1000)
    runner = CliRunner()
    result = runner.invoke(
        cli, ["--no-prompt", "exe", "list-trx", "--at", str(epoch_ms)]
    )
    logger.debug(result.output)
    assert result.exit_code == 0


@pytest.mark.integration
@pytest.mark.requires_internet
def test_worker_list_with_status(login, testing_collection_with_table):
    runner = CliRunner()
    result = runner.invoke(
        cli, ["--no-prompt", "exe", "list-worker", "--status", "done"]
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
            "exe",
            "list-worker",
            "--status",
            "done",
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
        cli, ["--no-prompt", "exe", "list-worker", "--status", "doesnotexist"]
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
        ["--no-prompt", "exe", "list-worker", "--fn", "input_file_csv_string_format"],
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
            "exe",
            "list-worker",
            "--coll",
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
        cli, ["--no-prompt", "exe", "list-worker", "--plan-name", "test_*"]
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
        cli, ["--no-prompt", "exe", "list-worker", "--plan", str(execution_id)]
    )
    logger.debug(result.output)
    assert result.exit_code == 0


@pytest.mark.integration
@pytest.mark.requires_internet
def test_worker_list_with_function_run_id(
    login, testing_collection_with_table, tabsserver_connection
):
    fn_run_id = tabsserver_connection.list_function_runs()[0].id
    runner = CliRunner()
    logger.debug(f"Function Run ID: {fn_run_id}")
    result = runner.invoke(
        cli, ["--no-prompt", "exe", "list-worker", "--fn-run", str(fn_run_id)]
    )
    logger.debug(result.output)
    assert result.exit_code == 0


@pytest.mark.integration
@pytest.mark.requires_internet
def test_worker_messages_list_by_transaction_id(
    login, tabsserver_connection, testing_collection_with_table
):
    transaction_id = None
    for transaction in tabsserver_connection.transactions:
        if transaction.status in TRANSACTION_FINAL_STATUSES:
            transaction_id = transaction.id
            break
    logger.debug(f"Transactions: {tabsserver_connection.transactions}")
    logger.debug(f"Transaction ID: {transaction_id}")
    assert transaction_id
    runner = CliRunner()
    result = runner.invoke(
        cli, ["--no-prompt", "exe", "list-worker", "--trx", transaction_id]
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
            "exe",
            "list-worker",
            "--fn",
            function_name,
            "--coll",
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
    for transaction in tabsserver_connection.transactions:
        if transaction.status in TRANSACTION_FINAL_STATUSES:
            transaction_id = transaction.id
            break
    logger.debug(f"Transactions: {tabsserver_connection.transactions}")
    logger.debug(f"Transaction ID: {transaction_id}")
    assert transaction_id
    runner = CliRunner()
    result = runner.invoke(
        cli,
        [
            "--no-prompt",
            "exe",
            "list-worker",
            "--fn",
            function_name,
            "--trx",
            transaction_id,
            "--plan",
            execution_id,
            "--coll",
            testing_collection_with_table,
        ],
    )
    logger.debug(result.output)
    assert result.exit_code != 0


@pytest.mark.integration
@pytest.mark.requires_internet
def test_exec_logs(login, testing_collection_with_table, tabsserver_connection):
    transaction_id = None
    for transaction in tabsserver_connection.transactions:
        if transaction.status in TRANSACTION_FINAL_STATUSES:
            transaction_id = transaction.id
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
    result = runner.invoke(cli, ["--no-prompt", "exe", "logs", "--worker", message_id])
    logger.debug(result.output)
    assert result.exit_code == 0


@pytest.mark.integration
@pytest.mark.requires_internet
def test_exec_logs_to_file(
    login, testing_collection_with_table, tabsserver_connection, tmp_path
):
    destination = os.path.join(tmp_path, "logs.txt")
    transaction_id = None
    for transaction in tabsserver_connection.transactions:
        if transaction.status in TRANSACTION_FINAL_STATUSES:
            transaction_id = transaction.id
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
            "exe",
            "logs",
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
            "exe",
            "logs",
            "--coll",
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
            "exe",
            "logs",
            "--coll",
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
            "exe",
            "logs",
            "--coll",
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
    for transaction in tabsserver_connection.transactions:
        if transaction.status in TRANSACTION_FINAL_STATUSES:
            transaction_id = transaction.id
            break
    assert transaction_id
    runner = CliRunner()
    result = runner.invoke(cli, ["--no-prompt", "exe", "logs", "--trx", transaction_id])
    logger.debug(result.output)
    assert result.exit_code == 0


@pytest.mark.integration
@pytest.mark.requires_internet
def test_exec_logs_transaction_to_file(
    login, testing_collection_with_table, tmp_path, tabsserver_connection
):
    destination = os.path.join(tmp_path, "logs.txt")
    transaction_id = None
    for transaction in tabsserver_connection.transactions:
        if transaction.status in TRANSACTION_FINAL_STATUSES:
            transaction_id = transaction.id
            break
    assert transaction_id
    runner = CliRunner()
    result = runner.invoke(
        cli,
        [
            "--no-prompt",
            "exe",
            "logs",
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
    for transaction in tabsserver_connection.transactions:
        if transaction.status in TRANSACTION_FINAL_STATUSES:
            transaction_id = transaction.id
            break
    assert transaction_id
    runner = CliRunner()
    result = runner.invoke(
        cli,
        ["exe", "logs", "--trx", transaction_id, "--file", destination],
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
        ["--no-prompt", "exe", "info", "--plan", exec_id],
    )
    logger.debug(result.output)
    assert result.exit_code == 0


@pytest.mark.integration
@pytest.mark.requires_internet
def test_exec_monitor_plan(
    login, testing_collection_with_table, tabsserver_connection
):
    exec_id = tabsserver_connection.executions[0].id
    runner = CliRunner()
    result = runner.invoke(
        cli,
        ["--no-prompt", "exe", "monitor", "--plan", exec_id],
    )
    logger.debug(result.output)
    assert result.exit_code == 0


@pytest.mark.integration
@pytest.mark.requires_internet
def test_exec_monitor_trx(
    login, testing_collection_with_table, tabsserver_connection
):
    transaction_id = tabsserver_connection.transactions[0].id
    runner = CliRunner()
    result = runner.invoke(
        cli,
        ["--no-prompt", "exe", "monitor", "--trx", transaction_id],
    )
    logger.debug(result.output)
    assert result.exit_code == 0


@pytest.mark.integration
@pytest.mark.requires_internet
def test_fn_run_list_with_status(login, testing_collection_with_table):
    runner = CliRunner()
    result = runner.invoke(
        cli, ["--no-prompt", "exe", "list-fn-run", "--status", "done"]
    )
    logger.debug(result.output)
    assert result.exit_code == 0


@pytest.mark.integration
@pytest.mark.requires_internet
def test_fn_run_list_with_multiple_statuses(login, testing_collection_with_table):
    runner = CliRunner()
    result = runner.invoke(
        cli,
        [
            "--no-prompt",
            "exe",
            "list-fn-run",
            "--status",
            "done",
            "--status",
            "failed",
        ],
    )
    logger.debug(result.output)
    assert result.exit_code == 0


@pytest.mark.integration
@pytest.mark.requires_internet
def test_fn_run_list_with_wrong_status_fails(login, testing_collection_with_table):
    runner = CliRunner()
    result = runner.invoke(
        cli, ["--no-prompt", "exe", "list-fn-run", "--status", "doesnotexist"]
    )
    logger.debug(result.output)
    assert result.exit_code != 0


@pytest.mark.integration
@pytest.mark.requires_internet
def test_fn_run_list_with_only_function_name_fails(
    login, testing_collection_with_table
):
    runner = CliRunner()
    result = runner.invoke(
        cli,
        ["--no-prompt", "exe", "list-fn-run", "--fn", "input_file_csv_string_format"],
    )
    logger.debug(result.output)
    assert result.exit_code != 0


@pytest.mark.integration
@pytest.mark.requires_internet
def test_fn_run_list_with_collection_name(login, testing_collection_with_table):
    runner = CliRunner()
    result = runner.invoke(
        cli,
        [
            "--no-prompt",
            "exe",
            "list-fn-run",
            "--coll",
            testing_collection_with_table,
        ],
    )
    logger.debug(result.output)
    assert result.exit_code == 0


@pytest.mark.integration
@pytest.mark.requires_internet
def test_fn_run_list_with_wildcard_execution_name(login, testing_collection_with_table):
    runner = CliRunner()
    result = runner.invoke(
        cli, ["--no-prompt", "exe", "list-fn-run", "--plan-name", "test_*"]
    )
    logger.debug(result.output)
    assert result.exit_code == 0


@pytest.mark.integration
@pytest.mark.requires_internet
def test_fn_run_list_with_execution_id(
    login, testing_collection_with_table, tabsserver_connection
):
    execution_id = tabsserver_connection.executions[0].id
    runner = CliRunner()
    result = runner.invoke(
        cli, ["--no-prompt", "exe", "list-fn-run", "--plan", str(execution_id)]
    )
    logger.debug(result.output)
    assert result.exit_code == 0


@pytest.mark.integration
@pytest.mark.requires_internet
def test_fn_run_list_by_transaction_id(
    login, tabsserver_connection, testing_collection_with_table
):
    transaction_id = None
    for transaction in tabsserver_connection.transactions:
        if transaction.status in TRANSACTION_FINAL_STATUSES:
            transaction_id = transaction.id
            break
    logger.debug(f"Transactions: {tabsserver_connection.transactions}")
    logger.debug(f"Transaction ID: {transaction_id}")
    assert transaction_id
    runner = CliRunner()
    result = runner.invoke(
        cli, ["--no-prompt", "exe", "list-fn-run", "--trx", transaction_id]
    )
    logger.debug(result.output)
    assert result.exit_code == 0


@pytest.mark.integration
@pytest.mark.requires_internet
def test_fn_run_list_by_function_and_collection(
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
            "exe",
            "list-fn-run",
            "--fn",
            function_name,
            "--coll",
            testing_collection_with_table,
        ],
    )
    logger.debug(result.output)
    assert result.exit_code == 0


@pytest.mark.integration
@pytest.mark.requires_internet
def test_fn_run_list_by_all_options_fails(
    login, tabsserver_connection, testing_collection_with_table
):
    execution_id = tabsserver_connection.executions[0].id
    function_name = tabsserver_connection.list_functions(testing_collection_with_table)[
        0
    ].name
    transaction_id = None
    for transaction in tabsserver_connection.transactions:
        if transaction.status in TRANSACTION_FINAL_STATUSES:
            transaction_id = transaction.id
            break
    logger.debug(f"Transactions: {tabsserver_connection.transactions}")
    logger.debug(f"Transaction ID: {transaction_id}")
    assert transaction_id
    runner = CliRunner()
    result = runner.invoke(
        cli,
        [
            "--no-prompt",
            "exe",
            "list-fn-run",
            "--fn",
            function_name,
            "--trx",
            transaction_id,
            "--plan",
            execution_id,
            "--coll",
            testing_collection_with_table,
        ],
    )
    logger.debug(result.output)
    assert result.exit_code != 0


# ---------------------------------------------------------------------------
# Unit tests for filter construction helpers
# ---------------------------------------------------------------------------


def _assert_flat_list_of_strings(result):
    """Assert that result is a flat list where every element is a string."""
    assert isinstance(result, list)
    for i, item in enumerate(result):
        assert isinstance(item, str), (
            f"Element at index {i} is {type(item).__name__}, not str: {item!r}"
        )


class TestObtainListFnRunFilters:
    def test_no_filters(self):
        result = obtain_list_fn_run_filters(
            status=None, exe_name=None, execution_id=None,
            fn=None, collection=None, trx=None,
        )
        assert result == []

    def test_single_status(self):
        result = obtain_list_fn_run_filters(
            status=("done",), exe_name=None, execution_id=None,
            fn=None, collection=None, trx=None,
        )
        _assert_flat_list_of_strings(result)
        assert len(result) == 1
        assert "status:eq:" in result[0]

    def test_multiple_statuses_produces_flat_list(self):
        result = obtain_list_fn_run_filters(
            status=("done", "failed"), exe_name=None, execution_id=None,
            fn=None, collection=None, trx=None,
        )
        _assert_flat_list_of_strings(result)
        assert len(result) == 2

    def test_multiple_statuses_with_other_filters(self):
        result = obtain_list_fn_run_filters(
            status=("done", "failed"), exe_name="my_exec",
            execution_id="exec-123", fn="my_fn",
            collection="my_coll", trx="trx-456",
        )
        _assert_flat_list_of_strings(result)
        status_filters = [f for f in result if f.startswith("status:eq:")]
        assert len(status_filters) == 2
        assert "execution:lk:my_exec" in result
        assert "execution_id:eq:exec-123" in result
        assert "name:eq:my_fn" in result
        assert "collection:eq:my_coll" in result
        assert "transaction_id:eq:trx-456" in result

    def test_non_status_filters_only(self):
        result = obtain_list_fn_run_filters(
            status=None, exe_name="my_exec", execution_id=None,
            fn=None, collection="my_coll", trx=None,
        )
        _assert_flat_list_of_strings(result)
        assert len(result) == 2


class TestObtainListExecFilters:
    def test_no_filters(self):
        result = obtain_list_exec_filters(
            status=None, exe_name=None, fn=None, collection=None, at=None,
        )
        assert result == []

    def test_single_status(self):
        result = obtain_list_exec_filters(
            status=("finished",), exe_name=None, fn=None,
            collection=None, at=None,
        )
        _assert_flat_list_of_strings(result)
        assert len(result) == 1

    def test_multiple_statuses_produces_flat_list(self):
        result = obtain_list_exec_filters(
            status=("finished", "stalled"), exe_name=None, fn=None,
            collection=None, at=None,
        )
        _assert_flat_list_of_strings(result)
        assert len(result) == 2

    def test_multiple_statuses_with_other_filters(self):
        result = obtain_list_exec_filters(
            status=("finished", "stalled"), exe_name="my_exec",
            fn="my_fn", collection="my_coll", at=None,
        )
        _assert_flat_list_of_strings(result)
        status_filters = [f for f in result if f.startswith("status:eq:")]
        assert len(status_filters) == 2
        assert "name:lk:my_exec" in result
        assert "function:eq:my_fn" in result
        assert "collection:eq:my_coll" in result


class TestObtainListTrxFilters:
    def test_no_filters(self):
        result = obtain_list_trx_filters(
            status=None, exe_name=None, execution_id=None,
            collection=None, at=None,
        )
        assert result == []

    def test_single_status(self):
        result = obtain_list_trx_filters(
            status=("committed",), exe_name=None, execution_id=None,
            collection=None, at=None,
        )
        _assert_flat_list_of_strings(result)
        assert len(result) == 1

    def test_multiple_statuses_produces_flat_list(self):
        result = obtain_list_trx_filters(
            status=("committed", "stalled"), exe_name=None, execution_id=None,
            collection=None, at=None,
        )
        _assert_flat_list_of_strings(result)
        assert len(result) == 2

    def test_multiple_statuses_with_other_filters(self):
        result = obtain_list_trx_filters(
            status=("committed", "stalled"), exe_name="my_exec",
            execution_id="exec-123", collection="my_coll", at=None,
        )
        _assert_flat_list_of_strings(result)
        status_filters = [f for f in result if f.startswith("status:eq:")]
        assert len(status_filters) == 2
        assert "execution:lk:my_exec" in result
        assert "execution_id:eq:exec-123" in result
        assert "collection:eq:my_coll" in result


class TestObtainListWorkerFilters:
    def test_no_filters(self):
        result = obtain_list_worker_filters()
        assert result == []

    def test_single_status(self):
        result = obtain_list_worker_filters(status=("done",))
        _assert_flat_list_of_strings(result)
        assert len(result) == 1

    def test_multiple_statuses_produces_flat_list(self):
        result = obtain_list_worker_filters(status=("done", "failed"))
        _assert_flat_list_of_strings(result)
        assert len(result) == 2

    def test_multiple_statuses_with_other_filters(self):
        result = obtain_list_worker_filters(
            status=("done", "failed"), exe_name="my_exec",
            execution_id="exec-123", fn="my_fn",
            fn_run="run-789", collection="my_coll",
            trx="trx-456", worker_type="runner",
        )
        _assert_flat_list_of_strings(result)
        status_filters = [f for f in result if f.startswith("status:eq:")]
        assert len(status_filters) == 2
        assert "execution:lk:my_exec" in result
        assert "execution_id:eq:exec-123" in result
        assert "function:eq:my_fn" in result
        assert "logical_function_run_id:eq:run-789" in result
        assert "collection:eq:my_coll" in result
        assert "transaction_id:eq:trx-456" in result
        assert "worker_type:eq:runner" in result


@pytest.mark.parametrize("filter_fn, kwargs", [
    (obtain_list_fn_run_filters, dict(
        status=("done", "failed", "running"),
        exe_name=None, execution_id=None,
        fn=None, collection=None, trx=None,
    )),
    (obtain_list_exec_filters, dict(
        status=("finished", "stalled", "running"),
        exe_name=None, fn=None, collection=None, at=None,
    )),
    (obtain_list_trx_filters, dict(
        status=("committed", "stalled", "running"),
        exe_name=None, execution_id=None, collection=None, at=None,
    )),
    (obtain_list_worker_filters, dict(
        status=("done", "failed", "running"),
    )),
])
def test_no_nested_lists_in_filters(filter_fn, kwargs):
    """Regression test: filter functions must never produce nested lists."""
    result = filter_fn(**kwargs)
    _assert_flat_list_of_strings(result)
