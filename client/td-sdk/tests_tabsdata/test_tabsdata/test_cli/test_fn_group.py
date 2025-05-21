#
# Copyright 2024 Tabs Data Inc.
#

import logging
import os
import uuid

import pytest
from click.testing import CliRunner
from tests_tabsdata.conftest import ABSOLUTE_TEST_FOLDER_LOCATION, LOCAL_PACKAGES_LIST

from tabsdata.cli.cli import cli

# noinspection PyUnresolvedReferences
from . import pytestmark  # noqa: F401

logger = logging.getLogger(__name__)
logger.setLevel(logging.DEBUG)


@pytest.mark.integration
@pytest.mark.requires_internet
def test_wrong_command_raises_exception(login, testing_collection):
    runner = CliRunner()
    result = runner.invoke(cli, ["fn", "potato"])
    assert result.exit_code == 2


@pytest.mark.integration
@pytest.mark.requires_internet
@pytest.mark.wip
@pytest.mark.skip(reason="Pending rework after server last refactors.")
def test_function_create_prompt(
    testing_collection, function_path, tabsserver_connection
):
    runner = CliRunner()
    result = runner.invoke(
        cli,
        ["fn", "register"],
        input=f"{testing_collection}\n{function_path}\n",
    )
    logger.debug(result.output)
    assert result.exit_code == 0
    functions = tabsserver_connection.collection_list_functions(testing_collection)
    assert len(functions) == 1


@pytest.mark.integration
@pytest.mark.requires_internet
@pytest.mark.wip
@pytest.mark.skip(reason="Pending rework after server last refactors.")
def test_function_create_no_prompt(
    testing_collection, function_path, tabsserver_connection
):
    runner = CliRunner()
    result = runner.invoke(
        cli,
        [
            "--no-prompt",
            "fn",
            "register",
            "--collection",
            testing_collection,
            "--fn-path",
            function_path,
        ],
    )
    logger.debug(result.output)
    assert result.exit_code == 0
    functions = tabsserver_connection.collection_list_functions(testing_collection)
    assert len(functions) == 1


@pytest.mark.integration
@pytest.mark.requires_internet
@pytest.mark.wip
@pytest.mark.skip(reason="Pending rework after server last refactors.")
def test_function_create_no_prompt_multiple_local_packages(
    testing_collection, function_path, tabsserver_connection
):
    runner = CliRunner()
    result = runner.invoke(
        cli,
        [
            "--no-prompt",
            "fn",
            "register",
            "--collection",
            testing_collection,
            "--fn-path",
            function_path,
            "--local-pkg",
            os.getcwd(),
            "--local-pkg",
            os.getcwd(),
        ],
    )
    logger.debug(result.output)
    assert result.exit_code == 0
    functions = tabsserver_connection.collection_list_functions(testing_collection)
    assert len(functions) == 1


@pytest.mark.integration
@pytest.mark.requires_internet
@pytest.mark.skip(
    reason=(
        "Not working due to a bug in the backend. Function delete is "
        "not working properly."
    )
)
def test_function_delete_cli(testing_collection, function_path, tabsserver_connection):
    runner = CliRunner()
    result = runner.invoke(
        cli,
        [
            "--no-prompt",
            "fn",
            "register",
            "--collection",
            testing_collection,
            "--fn-path",
            function_path,
        ],
    )
    logger.debug(result.output)
    assert result.exit_code == 0
    functions = tabsserver_connection.collection_list_functions(testing_collection)
    assert len(functions) == 1
    result = runner.invoke(
        cli,
        [
            "fn",
            "delete",
            "--name",
            "test_input_plugin",
            "--collection",
            testing_collection,
        ],
        input="delete\n",
    )
    logger.debug(result.output)
    assert result.exit_code == 0
    functions = tabsserver_connection.collection_list_functions(testing_collection)
    assert len(functions) == 0


@pytest.mark.integration
@pytest.mark.requires_internet
@pytest.mark.skip(
    reason=(
        "Not working due to a bug in the backend. Function delete is "
        "not working properly."
    )
)
def test_function_delete_no_prompt(
    testing_collection, function_path, tabsserver_connection
):
    runner = CliRunner()
    result = runner.invoke(
        cli,
        [
            "--no-prompt",
            "fn",
            "register",
            "--collection",
            testing_collection,
            "--fn-path",
            function_path,
        ],
    )
    logger.debug(result.output)
    assert result.exit_code == 0
    functions = tabsserver_connection.collection_list_functions(testing_collection)
    assert len(functions) == 1
    result = runner.invoke(
        cli,
        [
            "--no-prompt",
            "fn",
            "delete",
            "--name",
            "test_input_plugin",
            "--collection",
            testing_collection,
            "--confirm",
            "delete",
        ],
    )
    logger.debug(result.output)
    assert result.exit_code == 0
    functions = tabsserver_connection.collection_list_functions(testing_collection)
    assert len(functions) == 0


@pytest.mark.integration
@pytest.mark.requires_internet
@pytest.mark.skip("Not currently supported")
def test_function_delete_wrong_options_raises_error(
    testing_collection, function_path, tabsserver_connection
):
    runner = CliRunner()
    result = runner.invoke(
        cli,
        [
            "collection",
            "delete",
            "--name",
            "test_function_delete_no_exists_raises_error",
            "--collection",
            testing_collection,
            "--confirm",
            "delete",
        ],
    )
    assert result.exit_code != 0
    result = runner.invoke(
        cli,
        [
            "--no-prompt",
            "fn",
            "register",
            "--collection",
            testing_collection,
            "--fn-path",
            function_path,
        ],
    )
    logger.debug(result.output)
    assert result.exit_code == 0
    functions = tabsserver_connection.collection_list_functions(testing_collection)
    assert len(functions) == 1
    result = runner.invoke(
        cli,
        [
            "fn",
            "delete",
            "--name",
            "test_input_plugin",
            "--collection",
            testing_collection,
            "--confirm",
            "yes",
        ],
    )
    assert result.exit_code != 0
    result = runner.invoke(
        cli,
        [
            "fn",
            "delete",
            "--name",
            "test_input_plugin",
            "--collection",
            testing_collection,
        ],
        input="yes\n",
    )
    assert result.exit_code != 0
    result = runner.invoke(
        cli,
        [
            "--no-promptfunction",
            "delete",
            "--name",
            "test_input_plugin",
            "--collection",
            testing_collection,
        ],
        input="delete\n",
    )
    assert result.exit_code != 0


@pytest.mark.integration
@pytest.mark.requires_internet
@pytest.mark.wip
@pytest.mark.skip(reason="Pending rework after server last refactors.")
def test_function_list(testing_collection, function_path, tabsserver_connection):
    runner = CliRunner()
    result = runner.invoke(
        cli,
        [
            "--no-prompt",
            "fn",
            "register",
            "--collection",
            testing_collection,
            "--fn-path",
            function_path,
        ],
    )
    logger.debug(result.output)
    result = runner.invoke(cli, ["fn", "list", "--collection", testing_collection])
    logger.debug(result.output)
    assert result.exit_code == 0


@pytest.mark.integration
@pytest.mark.requires_internet
@pytest.mark.wip
@pytest.mark.skip(reason="Pending rework after server last refactors.")
def test_function_update(testing_collection, function_path, tabsserver_connection):
    runner = CliRunner()
    result = runner.invoke(
        cli,
        [
            "--no-prompt",
            "fn",
            "register",
            "--collection",
            testing_collection,
            "--fn-path",
            function_path,
        ],
    )
    logger.debug(result.output)
    functions = tabsserver_connection.collection_list_functions(testing_collection)
    assert len(functions) == 1
    result = runner.invoke(
        cli,
        [
            "fn",
            "update",
            "--name",
            "test_input_plugin",
            "--collection",
            testing_collection,
            "--description",
            "new_description",
            "--fn-path",
            function_path,
        ],
    )
    logger.debug(result.output)
    assert result.exit_code == 0
    functions = tabsserver_connection.collection_list_functions(testing_collection)
    assert len(functions) == 1
    assert functions[0].description == "new_description"


@pytest.mark.integration
@pytest.mark.requires_internet
@pytest.mark.wip
@pytest.mark.skip(reason="Pending rework after server last refactors.")
def test_function_info(testing_collection, function_path, tabsserver_connection):
    runner = CliRunner()
    result = runner.invoke(
        cli,
        [
            "--no-prompt",
            "fn",
            "register",
            "--collection",
            testing_collection,
            "--fn-path",
            function_path,
        ],
    )
    logger.debug(result.output)
    functions = tabsserver_connection.collection_list_functions(testing_collection)
    assert len(functions) != 0
    result = runner.invoke(
        cli,
        [
            "fn",
            "info",
            "--collection",
            testing_collection,
            "--name",
            "test_input_plugin",
        ],
    )
    logger.debug(result.output)
    assert result.exit_code == 0


@pytest.mark.integration
@pytest.mark.requires_internet
@pytest.mark.wip
@pytest.mark.skip(reason="Pending rework after server last refactors.")
def test_function_info_show_versions(
    testing_collection, function_path, tabsserver_connection
):
    runner = CliRunner()
    result = runner.invoke(
        cli,
        [
            "--no-prompt",
            "fn",
            "register",
            "--collection",
            testing_collection,
            "--fn-path",
            function_path,
        ],
    )
    logger.debug(result.output)
    functions = tabsserver_connection.collection_list_functions(testing_collection)
    assert len(functions) == 1
    result = runner.invoke(
        cli,
        [
            "fn",
            "info",
            "--collection",
            testing_collection,
            "--name",
            "test_input_plugin",
            "--show-history",
        ],
    )
    logger.debug(result.output)
    assert result.exit_code == 0


@pytest.mark.integration
@pytest.mark.requires_internet
@pytest.mark.wip
@pytest.mark.skip(reason="Pending rework after server last refactors.")
def test_function_trigger(testing_collection, function_path, tabsserver_connection):
    runner = CliRunner()
    result = runner.invoke(
        cli,
        [
            "--no-prompt",
            "fn",
            "register",
            "--collection",
            testing_collection,
            "--fn-path",
            function_path,
        ],
    )
    logger.debug(result.output)
    functions = tabsserver_connection.collection_list_functions(testing_collection)
    assert len(functions) == 1
    result = runner.invoke(
        cli,
        [
            "fn",
            "info",
            "--collection",
            testing_collection,
            "--name",
            "test_input_plugin",
        ],
    )
    logger.debug(result.output)
    assert result.exit_code == 0
    result = runner.invoke(
        cli,
        [
            "fn",
            "trigger",
            "--collection",
            testing_collection,
            "--name",
            "test_input_plugin",
        ],
    )
    logger.debug(result.output)
    assert result.exit_code == 0


@pytest.mark.integration
@pytest.mark.requires_internet
@pytest.mark.wip
@pytest.mark.skip(reason="Pending rework after server last refactors.")
def test_function_trigger_execution_plan_name(
    testing_collection, function_path, tabsserver_connection
):
    runner = CliRunner()
    result = runner.invoke(
        cli,
        [
            "--no-prompt",
            "fn",
            "register",
            "--collection",
            testing_collection,
            "--fn-path",
            function_path,
        ],
    )
    logger.debug(result.output)
    result = runner.invoke(
        cli,
        [
            "fn",
            "info",
            "--collection",
            testing_collection,
            "--name",
            "test_input_plugin",
        ],
    )
    logger.debug(result.output)
    assert result.exit_code == 0
    result = runner.invoke(
        cli,
        [
            "fn",
            "trigger",
            "--collection",
            testing_collection,
            "--name",
            "test_input_plugin",
            "--execution-plan-name",
            "test_execution_plan_name",
        ],
    )
    logger.debug(result.output)
    assert result.exit_code == 0


@pytest.mark.integration
@pytest.mark.requires_internet
@pytest.mark.wip
@pytest.mark.skip(reason="Pending rework after server last refactors.")
def test_function_info_error(testing_collection):
    runner = CliRunner()
    result = runner.invoke(
        cli,
        [
            "fn",
            "info",
            "--name",
            "test_function_info_error",
            "--collection",
            testing_collection,
        ],
    )
    assert result.exit_code != 0


@pytest.mark.integration
def test_function_cli_read_run(login, tabsserver_connection):
    collection = tabsserver_connection.create_collection(
        f"test_function_class_read_run_{uuid.uuid4().hex[:16]}"
    )
    file_path = os.path.join(
        ABSOLUTE_TEST_FOLDER_LOCATION,
        "testing_resources",
        "test_input_file_csv_string_format",
        "example.py",
    )
    function_path = file_path + "::input_file_csv_string_format"
    function = collection.register_function(
        function_path, local_packages=LOCAL_PACKAGES_LIST
    )
    plan = function.trigger(
        f"test_function_class_read_run_plan_{uuid.uuid4().hex[:16]}"
    )
    runner = CliRunner()
    result = runner.invoke(
        cli,
        [
            "fn",
            "read-run",
            "--collection",
            collection.name,
            "--name",
            "input_file_csv_string_format",
            "--execution-id",
            plan.id,
        ],
    )
    logger.debug(result.output)
    assert result.exit_code == 0
