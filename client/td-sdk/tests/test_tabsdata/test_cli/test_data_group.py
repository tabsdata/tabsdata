#
# Copyright 2025 Tabs Data Inc.
#

import logging

import pytest
from click.testing import CliRunner

from tabsdata.cli.cli import cli

logger = logging.getLogger(__name__)
logger.setLevel(logging.DEBUG)


@pytest.mark.integration
@pytest.mark.requires_internet
def test_wrong_command_raises_exception(login, testing_collection_with_table):
    runner = CliRunner()
    result = runner.invoke(cli, ["--no-prompt", "table", "potato"])
    assert result.exit_code == 2


@pytest.mark.integration
@pytest.mark.requires_internet
def test_data_version(login, testing_collection_with_table, tabsserver_connection):
    function_name = tabsserver_connection.collection_list_functions(
        testing_collection_with_table
    )[0].name
    runner = CliRunner()
    result = runner.invoke(
        cli,
        [
            "--no-prompt",
            "data",
            "versions",
            "--collection",
            testing_collection_with_table,
            "--function",
            function_name,
        ],
    )
    logger.debug(result.output)
    assert result.exit_code == 0
