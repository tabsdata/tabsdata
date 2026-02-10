#
# Copyright 2024 Tabs Data Inc.
#

import logging
import os
import time
from datetime import datetime, timedelta, timezone

import polars as pl
import pytest
from click.testing import CliRunner

from tabsdata._cli.cli import cli
from tabsdata.api.tabsdata_server import Table
from tests_tabsdata.conftest import (
    ABSOLUTE_TEST_FOLDER_LOCATION,
    LOCAL_PACKAGES_LIST,
)

# noinspection PyUnresolvedReferences
from . import pytestmark  # noqa: F401

logger = logging.getLogger(__name__)
logger.setLevel(logging.DEBUG)


@pytest.mark.integration
@pytest.mark.requires_internet
@pytest.mark.slow
def test_wrong_command_raises_exception(login, testing_collection_with_table):
    runner = CliRunner()
    result = runner.invoke(cli, ["--no-prompt", "table", "rex"])
    assert result.exit_code == 2


@pytest.mark.integration
@pytest.mark.requires_internet
@pytest.mark.slow
def test_table_download(login, testing_collection_with_table, tmp_path):
    destination_file = os.path.join(tmp_path, "test_table_download_cli.parquet")
    runner = CliRunner()
    result = runner.invoke(
        cli,
        [
            "--no-prompt",
            "table",
            "download",
            "--coll",
            testing_collection_with_table,
            "--name",
            "output",
            "--file",
            destination_file,
        ],
    )
    logger.debug(result.output)
    assert result.exit_code == 0
    assert os.path.exists(destination_file)


@pytest.mark.integration
@pytest.mark.requires_internet
@pytest.mark.slow
def test_table_list(login, testing_collection_with_table):
    runner = CliRunner()
    result = runner.invoke(
        cli,
        ["--no-prompt", "table", "list", "--coll", testing_collection_with_table],
    )
    logger.debug(result.output)
    assert result.exit_code == 0


@pytest.mark.integration
@pytest.mark.requires_internet
@pytest.mark.slow
def test_table_sample(login, testing_collection_with_table):
    runner = CliRunner()
    result = runner.invoke(
        cli,
        [
            "--no-prompt",
            "table",
            "sample",
            "--coll",
            testing_collection_with_table,
            "--name",
            "output",
        ],
    )
    logger.debug(result.output)
    assert result.exit_code == 0


@pytest.mark.integration
@pytest.mark.requires_internet
@pytest.mark.slow
def test_table_sample_file_destination(login, testing_collection_with_table, tmp_path):
    runner = CliRunner()
    destination_file = os.path.join(tmp_path, "test_table_sample_cli.ndjson")
    result = runner.invoke(
        cli,
        [
            "--no-prompt",
            "table",
            "sample",
            "--coll",
            testing_collection_with_table,
            "--name",
            "output",
            "--file",
            destination_file,
        ],
    )
    logger.debug(result.output)
    assert result.exit_code == 0
    assert os.path.exists(destination_file)
    assert isinstance(pl.read_ndjson(destination_file), pl.DataFrame)


@pytest.mark.integration
@pytest.mark.requires_internet
@pytest.mark.slow
def test_table_schema(login, testing_collection_with_table):
    runner = CliRunner()
    result = runner.invoke(
        cli,
        [
            "--no-prompt",
            "table",
            "schema",
            "--coll",
            testing_collection_with_table,
            "--name",
            "output",
        ],
    )
    logger.debug(result.output)
    assert result.exit_code == 0


@pytest.mark.integration
@pytest.mark.requires_internet
@pytest.mark.slow
def test_table_download_at(login, testing_collection_with_table, tmp_path):
    destination_file = os.path.join(tmp_path, "test_table_download_at_cli.parquet")
    epoch_ms = int(time.time() * 1000)  # Current time in milliseconds
    runner = CliRunner()
    result = runner.invoke(
        cli,
        [
            "--no-prompt",
            "table",
            "download",
            "--coll",
            testing_collection_with_table,
            "--name",
            "output",
            "--file",
            destination_file,
            "--at",
            str(epoch_ms),
        ],
    )
    logger.debug(result.output)
    assert result.exit_code == 0
    assert os.path.exists(destination_file)


@pytest.mark.integration
@pytest.mark.requires_internet
@pytest.mark.slow
def test_table_sample_at(login, testing_collection_with_table):
    runner = CliRunner()
    epoch_ms = int(time.time() * 1000)  # Current time in milliseconds
    result = runner.invoke(
        cli,
        [
            "--no-prompt",
            "table",
            "sample",
            "--coll",
            testing_collection_with_table,
            "--name",
            "output",
            "--at",
            str(epoch_ms),
        ],
    )
    logger.debug(result.output)
    assert result.exit_code == 0


@pytest.mark.integration
@pytest.mark.requires_internet
@pytest.mark.slow
def test_table_sample_file_destination_at(
    login, testing_collection_with_table, tmp_path
):
    runner = CliRunner()
    destination_file = os.path.join(tmp_path, "test_table_sample_at_cli.ndjson")
    epoch_ms = int(time.time() * 1000)  # Current time in milliseconds
    result = runner.invoke(
        cli,
        [
            "--no-prompt",
            "table",
            "sample",
            "--coll",
            testing_collection_with_table,
            "--name",
            "output",
            "--file",
            destination_file,
            "--at",
            str(epoch_ms),
        ],
    )
    logger.debug(result.output)
    assert result.exit_code == 0
    assert os.path.exists(destination_file)
    assert isinstance(pl.read_ndjson(destination_file), pl.DataFrame)


@pytest.mark.integration
@pytest.mark.requires_internet
@pytest.mark.slow
def test_table_schema_at(login, testing_collection_with_table):
    runner = CliRunner()
    epoch_ms = int(time.time() * 1000)  # Current time in milliseconds
    result = runner.invoke(
        cli,
        [
            "--no-prompt",
            "table",
            "schema",
            "--coll",
            testing_collection_with_table,
            "--name",
            "output",
            "--at",
            str(epoch_ms),
        ],
    )
    logger.debug(result.output)
    assert result.exit_code == 0


@pytest.mark.integration
@pytest.mark.requires_internet
@pytest.mark.slow
def test_table_sample_at_date(login, testing_collection_with_table):
    runner = CliRunner()
    next_day = (datetime.now(timezone.utc) + timedelta(days=1)).strftime("%Y-%m-%d")
    result = runner.invoke(
        cli,
        [
            "--no-prompt",
            "table",
            "sample",
            "--coll",
            testing_collection_with_table,
            "--name",
            "output",
            "--at",
            str(next_day),
        ],
    )
    logger.debug(result.output)
    assert result.exit_code == 0


@pytest.mark.integration
@pytest.mark.requires_internet
@pytest.mark.slow
def test_table_sample_file_destination_at_date(
    login, testing_collection_with_table, tmp_path
):
    runner = CliRunner()
    destination_file = os.path.join(tmp_path, "test_table_sample_at_date_cli.ndjson")
    next_day = (datetime.now(timezone.utc) + timedelta(days=1)).strftime("%Y-%m-%d")
    result = runner.invoke(
        cli,
        [
            "--no-prompt",
            "table",
            "sample",
            "--coll",
            testing_collection_with_table,
            "--name",
            "output",
            "--file",
            destination_file,
            "--at",
            str(next_day),
        ],
    )
    logger.debug(result.output)
    assert result.exit_code == 0
    assert os.path.exists(destination_file)
    assert isinstance(pl.read_ndjson(destination_file), pl.DataFrame)


@pytest.mark.integration
@pytest.mark.requires_internet
@pytest.mark.slow
def test_table_schema_at_date(login, testing_collection_with_table):
    runner = CliRunner()
    next_day = (datetime.now(timezone.utc) + timedelta(days=1)).strftime("%Y-%m-%d")
    result = runner.invoke(
        cli,
        [
            "--no-prompt",
            "table",
            "schema",
            "--coll",
            testing_collection_with_table,
            "--name",
            "output",
            "--at",
            str(next_day),
        ],
    )
    logger.debug(result.output)
    assert result.exit_code == 0


@pytest.mark.integration
@pytest.mark.requires_internet
@pytest.mark.slow
def test_table_sample_at_utc_time(login, testing_collection_with_table):
    runner = CliRunner()
    next_day = (datetime.now(timezone.utc) + timedelta(days=1)).strftime("%Y-%m-%dT%HZ")
    result = runner.invoke(
        cli,
        [
            "--no-prompt",
            "table",
            "sample",
            "--coll",
            testing_collection_with_table,
            "--name",
            "output",
            "--at",
            str(next_day),
        ],
    )
    logger.debug(result.output)
    assert result.exit_code == 0


@pytest.mark.integration
@pytest.mark.requires_internet
@pytest.mark.slow
def test_table_sample_file_destination_at_utc_time(
    login, testing_collection_with_table, tmp_path
):
    runner = CliRunner()
    destination_file = os.path.join(
        tmp_path, "test_table_sample_at_utc_time_cli.ndjson"
    )
    next_day = (datetime.now(timezone.utc) + timedelta(days=1)).strftime("%Y-%m-%dT%HZ")
    result = runner.invoke(
        cli,
        [
            "--no-prompt",
            "table",
            "sample",
            "--coll",
            testing_collection_with_table,
            "--name",
            "output",
            "--file",
            destination_file,
            "--at",
            str(next_day),
        ],
    )
    logger.debug(result.output)
    assert result.exit_code == 0
    assert os.path.exists(destination_file)
    assert isinstance(pl.read_ndjson(destination_file), pl.DataFrame)


@pytest.mark.integration
@pytest.mark.requires_internet
@pytest.mark.slow
def test_table_schema_at_utc_time(login, testing_collection_with_table):
    runner = CliRunner()
    next_day = (datetime.now(timezone.utc) + timedelta(days=1)).strftime("%Y-%m-%dT%HZ")
    result = runner.invoke(
        cli,
        [
            "--no-prompt",
            "table",
            "schema",
            "--coll",
            testing_collection_with_table,
            "--name",
            "output",
            "--at",
            str(next_day),
        ],
    )
    logger.debug(result.output)
    assert result.exit_code == 0


@pytest.mark.integration
@pytest.mark.requires_internet
@pytest.mark.slow
def test_table_sample_at_dataversion_id(
    login, testing_collection_with_table, tabsserver_connection
):
    runner = CliRunner()
    origin_table = Table(
        tabsserver_connection.connection, testing_collection_with_table, "output"
    )
    version = origin_table.dataversions[-1]  # Get the latest data version
    result = runner.invoke(
        cli,
        [
            "--no-prompt",
            "table",
            "sample",
            "--coll",
            testing_collection_with_table,
            "--name",
            "output",
            "--version",
            str(version.id),
        ],
    )
    logger.debug(result.output)
    assert result.exit_code == 0


@pytest.mark.integration
@pytest.mark.requires_internet
@pytest.mark.slow
def test_table_sample_file_destination_at_dataversion_id(
    login, testing_collection_with_table, tmp_path, tabsserver_connection
):
    runner = CliRunner()
    destination_file = os.path.join(
        tmp_path, "test_table_sample_at_dataversion_id_cli.ndjson"
    )
    origin_table = Table(
        tabsserver_connection.connection, testing_collection_with_table, "output"
    )
    version = origin_table.dataversions[-1]  # Get the latest data version
    result = runner.invoke(
        cli,
        [
            "--no-prompt",
            "table",
            "sample",
            "--coll",
            testing_collection_with_table,
            "--name",
            "output",
            "--file",
            destination_file,
            "--version",
            str(version.id),
        ],
    )
    logger.debug(result.output)
    assert result.exit_code == 0
    assert os.path.exists(destination_file)
    assert isinstance(pl.read_ndjson(destination_file), pl.DataFrame)


@pytest.mark.integration
@pytest.mark.requires_internet
@pytest.mark.slow
def test_table_schema_at_dataversion_id(
    login, testing_collection_with_table, tabsserver_connection
):
    runner = CliRunner()
    origin_table = Table(
        tabsserver_connection.connection, testing_collection_with_table, "output"
    )
    version = origin_table.dataversions[-1]  # Get the latest data version
    result = runner.invoke(
        cli,
        [
            "--no-prompt",
            "table",
            "schema",
            "--coll",
            testing_collection_with_table,
            "--name",
            "output",
            "--version",
            str(version.id),
        ],
    )
    logger.debug(result.output)
    assert result.exit_code == 0


@pytest.mark.integration
@pytest.mark.requires_internet
@pytest.mark.slow
def test_table_sample_at_transaction_id(
    login, testing_collection_with_table, tabsserver_connection
):
    runner = CliRunner()
    trx = tabsserver_connection.list_transactions(order_by="triggered_on+")[-1]
    result = runner.invoke(
        cli,
        [
            "--no-prompt",
            "table",
            "sample",
            "--coll",
            testing_collection_with_table,
            "--name",
            "output",
            "--at-trx",
            str(trx.id),
        ],
    )
    logger.debug(result.output)
    assert result.exit_code == 0


@pytest.mark.integration
@pytest.mark.requires_internet
@pytest.mark.slow
def test_table_sample_file_destination_at_transaction_id(
    login, testing_collection_with_table, tmp_path, tabsserver_connection
):
    runner = CliRunner()
    destination_file = os.path.join(
        tmp_path, "test_table_sample_at_transaction_id_cli.ndjson"
    )
    trx = tabsserver_connection.list_transactions(order_by="triggered_on+")[-1]
    result = runner.invoke(
        cli,
        [
            "--no-prompt",
            "table",
            "sample",
            "--coll",
            testing_collection_with_table,
            "--name",
            "output",
            "--file",
            destination_file,
            "--at-trx",
            str(trx.id),
        ],
    )
    logger.debug(result.output)
    assert result.exit_code == 0
    assert os.path.exists(destination_file)
    assert isinstance(pl.read_ndjson(destination_file), pl.DataFrame)


@pytest.mark.integration
@pytest.mark.requires_internet
@pytest.mark.slow
def test_table_schema_at_transaction_id(
    login, testing_collection_with_table, tabsserver_connection
):
    runner = CliRunner()
    trx = tabsserver_connection.list_transactions(order_by="triggered_on+")[-1]
    result = runner.invoke(
        cli,
        [
            "--no-prompt",
            "table",
            "schema",
            "--coll",
            testing_collection_with_table,
            "--name",
            "output",
            "--at-trx",
            str(trx.id),
        ],
    )
    logger.debug(result.output)
    assert result.exit_code == 0


@pytest.mark.integration
@pytest.mark.requires_internet
def test_data_version(login, testing_collection_with_table, tabsserver_connection):
    table_name = tabsserver_connection.list_tables(testing_collection_with_table)[
        0
    ].name
    runner = CliRunner()
    result = runner.invoke(
        cli,
        [
            "--no-prompt",
            "table",
            "versions",
            "--coll",
            testing_collection_with_table,
            "--name",
            table_name,
        ],
    )
    logger.debug(result.output)
    assert result.exit_code == 0


@pytest.mark.integration
@pytest.mark.requires_internet
def test_data_version_with_details(
    login, testing_collection_with_table, tabsserver_connection
):
    table_name = tabsserver_connection.list_tables(testing_collection_with_table)[
        0
    ].name
    runner = CliRunner()
    result = runner.invoke(
        cli,
        [
            "--no-prompt",
            "table",
            "versions",
            "--coll",
            testing_collection_with_table,
            "--name",
            table_name,
            "--details",
        ],
    )
    logger.debug(result.output)
    assert result.exit_code == 0


@pytest.mark.integration
@pytest.mark.requires_internet
@pytest.mark.slow
def test_table_list_with_wildcard(login, testing_collection_with_table):
    runner = CliRunner()
    result = runner.invoke(
        cli,
        [
            "--no-prompt",
            "table",
            "list",
            "--coll",
            testing_collection_with_table,
            "--name",
            "*",
        ],
    )
    logger.debug(result.output)
    assert result.exit_code == 0


@pytest.mark.integration
@pytest.mark.requires_internet
@pytest.mark.slow
def test_table_list_with_at(login, testing_collection_with_table):
    epoch_ms = int(time.time() * 1000)  # Current time in milliseconds
    runner = CliRunner()
    result = runner.invoke(
        cli,
        [
            "--no-prompt",
            "table",
            "list",
            "--coll",
            testing_collection_with_table,
            "--at",
            str(epoch_ms),
        ],
    )
    logger.debug(result.output)
    assert result.exit_code == 0


@pytest.mark.integration
@pytest.mark.requires_internet
@pytest.mark.slow
def test_table_list_with_at_date(login, testing_collection_with_table):
    next_day = (datetime.now(timezone.utc) + timedelta(days=1)).strftime("%Y-%m-%d")
    runner = CliRunner()
    result = runner.invoke(
        cli,
        [
            "--no-prompt",
            "table",
            "list",
            "--coll",
            testing_collection_with_table,
            "--at",
            str(next_day),
        ],
    )
    logger.debug(result.output)
    assert result.exit_code == 0


@pytest.mark.integration
@pytest.mark.requires_internet
@pytest.mark.slow
def test_table_list_with_at_utc_time(login, testing_collection_with_table):
    next_day = (datetime.now(timezone.utc) + timedelta(days=1)).strftime("%Y-%m-%dT%HZ")
    runner = CliRunner()
    result = runner.invoke(
        cli,
        [
            "--no-prompt",
            "table",
            "list",
            "--coll",
            testing_collection_with_table,
            "--at",
            str(next_day),
        ],
    )
    logger.debug(result.output)
    assert result.exit_code == 0


@pytest.mark.integration
@pytest.mark.requires_internet
@pytest.mark.slow
def test_table_list_with_at_transaction_id(
    login, testing_collection_with_table, tabsserver_connection
):
    trx = tabsserver_connection.list_transactions(order_by="triggered_on+")[-1]
    runner = CliRunner()
    result = runner.invoke(
        cli,
        [
            "--no-prompt",
            "table",
            "list",
            "--coll",
            testing_collection_with_table,
            "--at-trx",
            str(trx.id),
        ],
    )
    logger.debug(result.output)
    assert result.exit_code == 0


@pytest.mark.integration
@pytest.mark.requires_internet
def test_cli_table_delete(
    tabsserver_connection,
    login,
):
    try:
        tabsserver_connection.create_collection(
            name="test_cli_table_delete_collection",
            description="test_collection_description",
        )
        tabsserver_connection.register_function(
            collection_name="test_cli_table_delete_collection",
            description="test_table_delete_description",
            function_path=(
                f"{os.path.join(ABSOLUTE_TEST_FOLDER_LOCATION, "testing_resources",
                                "test_input_plugin", "example.py")}::input_plugin"
            ),
            local_packages=LOCAL_PACKAGES_LIST,
        )
        functions = tabsserver_connection.list_functions(
            "test_cli_table_delete_collection"
        )
        assert any(function.name == "test_input_plugin" for function in functions)
        assert any(
            table.name == "output"
            for table in tabsserver_connection.list_tables(
                "test_cli_table_delete_collection"
            )
        )
        tabsserver_connection.delete_function(
            "test_cli_table_delete_collection", "test_input_plugin"
        )
        functions = tabsserver_connection.list_functions(
            "test_cli_table_delete_collection"
        )
        assert not any(function.name == "test_input_plugin" for function in functions)
        runner = CliRunner()
        result = runner.invoke(
            cli,
            [
                "--no-prompt",
                "table",
                "delete",
                "--coll",
                "test_cli_table_delete_collection",
                "--name",
                "output",
                "--confirm",
                "delete",
            ],
        )
        logger.debug(result.output)
        assert result.exit_code == 0
        assert not any(
            table.name == "output"
            for table in tabsserver_connection.list_tables(
                "test_cli_table_delete_collection"
            )
        )
    finally:
        tabsserver_connection.delete_function(
            "test_cli_table_delete_collection",
            "test_input_plugin",
            raise_for_status=False,
        )
        tabsserver_connection.delete_collection(
            "test_cli_table_delete_collection", raise_for_status=False
        )
