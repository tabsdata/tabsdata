#
# Copyright 2024 Tabs Data Inc.
#

import datetime
import logging
import os

import polars as pl
import pytest
from click.testing import CliRunner

from tabsdata.cli.cli import cli

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
            "--collection",
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
        ["--no-prompt", "table", "list", "--collection", testing_collection_with_table],
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
            "--collection",
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
            "--collection",
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
            "--collection",
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
def test_table_download_with_version(login, testing_collection_with_table, tmp_path):
    destination_file = os.path.join(
        tmp_path, "test_table_download_with_version_cli.parquet"
    )
    runner = CliRunner()
    result = runner.invoke(
        cli,
        [
            "--no-prompt",
            "table",
            "download",
            "--collection",
            testing_collection_with_table,
            "--name",
            "output",
            "--file",
            destination_file,
            "--version",
            "HEAD",
        ],
    )
    logger.debug(result.output)
    assert result.exit_code == 0
    assert os.path.exists(destination_file)


@pytest.mark.integration
@pytest.mark.requires_internet
@pytest.mark.slow
def test_table_sample_with_version(login, testing_collection_with_table):
    runner = CliRunner()
    result = runner.invoke(
        cli,
        [
            "--no-prompt",
            "table",
            "sample",
            "--collection",
            testing_collection_with_table,
            "--name",
            "output",
            "--version",
            "HEAD",
        ],
    )
    logger.debug(result.output)
    assert result.exit_code == 0


@pytest.mark.integration
@pytest.mark.requires_internet
@pytest.mark.slow
def test_table_schema_with_version(login, testing_collection_with_table):
    runner = CliRunner()
    result = runner.invoke(
        cli,
        [
            "--no-prompt",
            "table",
            "schema",
            "--collection",
            testing_collection_with_table,
            "--name",
            "output",
            "--version",
            "HEAD",
        ],
    )
    logger.debug(result.output)
    assert result.exit_code == 0


@pytest.mark.integration
@pytest.mark.requires_internet
@pytest.mark.slow
def test_table_download_with_wrong_version(
    login, testing_collection_with_table, tmp_path
):
    destination_file = os.path.join(
        tmp_path, "test_table_download_with_wrong_version_cli.parquet"
    )
    runner = CliRunner()
    result = runner.invoke(
        cli,
        [
            "--no-prompt",
            "table",
            "download",
            "--collection",
            testing_collection_with_table,
            "--name",
            "output",
            "--file",
            destination_file,
            "--version",
            "DOESNTEXIST",
        ],
    )
    logger.debug(result.output)
    assert result.exit_code != 0


@pytest.mark.integration
@pytest.mark.requires_internet
@pytest.mark.slow
def test_table_sample_with_wrong_version(login, testing_collection_with_table):
    runner = CliRunner()
    result = runner.invoke(
        cli,
        [
            "--no-prompt",
            "table",
            "sample",
            "--collection",
            testing_collection_with_table,
            "--name",
            "output",
            "--version",
            "DOESNTEXIST",
        ],
    )
    logger.debug(result.output)
    assert result.exit_code != 0


@pytest.mark.integration
@pytest.mark.requires_internet
@pytest.mark.slow
def test_table_schema_with_wrong_version(login, testing_collection_with_table):
    runner = CliRunner()
    result = runner.invoke(
        cli,
        [
            "--no-prompt",
            "table",
            "schema",
            "--collection",
            testing_collection_with_table,
            "--name",
            "output",
            "--version",
            "DOESNTEXIST",
        ],
    )
    logger.debug(result.output)
    assert result.exit_code != 0


@pytest.mark.integration
@pytest.mark.requires_internet
@pytest.mark.slow
def test_table_download_with_commit(
    login, testing_collection_with_table, tmp_path, tabsserver_connection
):
    destination_file = os.path.join(
        tmp_path, "test_table_download_with_commit_cli.parquet"
    )
    commit = tabsserver_connection.commits[0].id
    runner = CliRunner()
    result = runner.invoke(
        cli,
        [
            "--no-prompt",
            "table",
            "download",
            "--collection",
            testing_collection_with_table,
            "--name",
            "output",
            "--file",
            destination_file,
            "--commit",
            commit,
        ],
    )
    logger.debug(result.output)
    assert result.exit_code == 0
    assert os.path.exists(destination_file)


@pytest.mark.integration
@pytest.mark.requires_internet
@pytest.mark.slow
def test_table_sample_with_commit(
    login, testing_collection_with_table, tabsserver_connection
):
    runner = CliRunner()
    commit = tabsserver_connection.commits[0].id
    result = runner.invoke(
        cli,
        [
            "--no-prompt",
            "table",
            "sample",
            "--collection",
            testing_collection_with_table,
            "--name",
            "output",
            "--commit",
            commit,
        ],
    )
    logger.debug(result.output)
    assert result.exit_code == 0


@pytest.mark.integration
@pytest.mark.requires_internet
@pytest.mark.slow
def test_table_schema_with_commit(
    login, testing_collection_with_table, tabsserver_connection
):
    runner = CliRunner()
    commit = tabsserver_connection.commits[0].id
    result = runner.invoke(
        cli,
        [
            "--no-prompt",
            "table",
            "schema",
            "--collection",
            testing_collection_with_table,
            "--name",
            "output",
            "--commit",
            commit,
        ],
    )
    logger.debug(result.output)
    assert result.exit_code == 0


@pytest.mark.integration
@pytest.mark.requires_internet
@pytest.mark.slow
def test_table_download_with_wrong_commit(
    login, testing_collection_with_table, tmp_path
):
    destination_file = os.path.join(
        tmp_path, "test_table_download_with_wrong_commit_cli.parquet"
    )
    runner = CliRunner()
    result = runner.invoke(
        cli,
        [
            "--no-prompt",
            "table",
            "download",
            "--collection",
            testing_collection_with_table,
            "--name",
            "output",
            "--file",
            destination_file,
            "--commit",
            "DOESNTEXIST",
        ],
    )
    logger.debug(result.output)
    assert result.exit_code != 0


@pytest.mark.integration
@pytest.mark.requires_internet
@pytest.mark.slow
def test_table_sample_with_wrong_commit(login, testing_collection_with_table):
    runner = CliRunner()
    result = runner.invoke(
        cli,
        [
            "--no-prompt",
            "table",
            "sample",
            "--collection",
            testing_collection_with_table,
            "--name",
            "output",
            "--commit",
            "DOESNTEXIST",
        ],
    )
    logger.debug(result.output)
    assert result.exit_code != 0


@pytest.mark.integration
@pytest.mark.requires_internet
@pytest.mark.slow
def test_table_schema_with_wrong_commit(login, testing_collection_with_table):
    runner = CliRunner()
    result = runner.invoke(
        cli,
        [
            "--no-prompt",
            "table",
            "schema",
            "--collection",
            testing_collection_with_table,
            "--name",
            "output",
            "--commit",
            "DOESNTEXIST",
        ],
    )
    logger.debug(result.output)
    assert result.exit_code != 0


@pytest.mark.integration
@pytest.mark.requires_internet
@pytest.mark.slow
def test_table_download_with_time(login, testing_collection_with_table, tmp_path):
    destination_file = os.path.join(
        tmp_path, "test_table_download_with_time_cli.parquet"
    )
    current_time = datetime.datetime.now(datetime.UTC)
    formatted_time = current_time.strftime("%Y-%m-%dT%H:%M:%S.%f")[:-3] + "Z"
    runner = CliRunner()
    result = runner.invoke(
        cli,
        [
            "--no-prompt",
            "table",
            "download",
            "--collection",
            testing_collection_with_table,
            "--name",
            "output",
            "--file",
            destination_file,
            "--time",
            formatted_time,
        ],
    )
    logger.debug(result.output)
    assert result.exit_code == 0
    assert os.path.exists(destination_file)


@pytest.mark.integration
@pytest.mark.requires_internet
@pytest.mark.slow
def test_table_sample_with_time(login, testing_collection_with_table):
    runner = CliRunner()
    current_time = datetime.datetime.now(datetime.UTC)
    formatted_time = current_time.strftime("%Y-%m-%dT%H:%M:%S.%f")[:-3] + "Z"
    result = runner.invoke(
        cli,
        [
            "--no-prompt",
            "table",
            "sample",
            "--collection",
            testing_collection_with_table,
            "--name",
            "output",
            "--time",
            formatted_time,
        ],
    )
    logger.debug(result.output)
    assert result.exit_code == 0


@pytest.mark.integration
@pytest.mark.requires_internet
@pytest.mark.slow
def test_table_schema_with_time(login, testing_collection_with_table):
    runner = CliRunner()
    current_time = datetime.datetime.now(datetime.UTC)
    formatted_time = current_time.strftime("%Y-%m-%dT%H:%M:%S.%f")[:-3] + "Z"
    result = runner.invoke(
        cli,
        [
            "--no-prompt",
            "table",
            "schema",
            "--collection",
            testing_collection_with_table,
            "--name",
            "output",
            "--time",
            formatted_time,
        ],
    )
    logger.debug(result.output)
    assert result.exit_code == 0


@pytest.mark.integration
@pytest.mark.requires_internet
@pytest.mark.slow
def test_table_download_with_wrong_time(login, testing_collection_with_table, tmp_path):
    destination_file = os.path.join(
        tmp_path, "test_table_download_with_wrong_time_cli.parquet"
    )
    runner = CliRunner()
    result = runner.invoke(
        cli,
        [
            "--no-prompt",
            "table",
            "download",
            "--collection",
            testing_collection_with_table,
            "--name",
            "output",
            "--file",
            destination_file,
            "--time",
            "DOESNTEXIST",
        ],
    )
    logger.debug(result.output)
    assert result.exit_code != 0


@pytest.mark.integration
@pytest.mark.requires_internet
@pytest.mark.slow
def test_table_sample_with_wrong_time(login, testing_collection_with_table):
    runner = CliRunner()
    result = runner.invoke(
        cli,
        [
            "--no-prompt",
            "table",
            "sample",
            "--collection",
            testing_collection_with_table,
            "--name",
            "output",
            "--time",
            "DOESNTEXIST",
        ],
    )
    logger.debug(result.output)
    assert result.exit_code != 0


@pytest.mark.integration
@pytest.mark.requires_internet
@pytest.mark.slow
def test_table_schema_with_wrong_time(login, testing_collection_with_table):
    runner = CliRunner()
    result = runner.invoke(
        cli,
        [
            "--no-prompt",
            "table",
            "schema",
            "--collection",
            testing_collection_with_table,
            "--name",
            "output",
            "--time",
            "DOESNTEXIST",
        ],
    )
    logger.debug(result.output)
    assert result.exit_code != 0


@pytest.mark.integration
@pytest.mark.requires_internet
@pytest.mark.slow
def test_table_download_with_all_options_fails(
    login, testing_collection_with_table, tmp_path, tabsserver_connection
):
    destination_file = os.path.join(
        tmp_path, "test_table_download_with_commit_cli.parquet"
    )
    runner = CliRunner()
    commit = tabsserver_connection.commits[0].id
    current_time = datetime.datetime.now(datetime.UTC)
    formatted_time = current_time.strftime("%Y-%m-%dT%H:%M:%S.%f")[:-3] + "Z"
    result = runner.invoke(
        cli,
        [
            "--no-prompt",
            "table",
            "download",
            "--collection",
            testing_collection_with_table,
            "--name",
            "output",
            "--file",
            destination_file,
            "--commit",
            commit,
            "--version",
            "HEAD",
            "--time",
            formatted_time,
        ],
    )
    logger.debug(result.output)
    assert result.exit_code != 0


@pytest.mark.integration
@pytest.mark.requires_internet
@pytest.mark.slow
def test_table_sample_with_all_options_fails(
    login, testing_collection_with_table, tabsserver_connection
):
    runner = CliRunner()
    current_time = datetime.datetime.now(datetime.UTC)
    formatted_time = current_time.strftime("%Y-%m-%dT%H:%M:%S.%f")[:-3] + "Z"
    commit = tabsserver_connection.commits[0].id
    result = runner.invoke(
        cli,
        [
            "--no-prompt",
            "table",
            "sample",
            "--collection",
            testing_collection_with_table,
            "--name",
            "output",
            "--commit",
            commit,
            "--version",
            "HEAD",
            "--time",
            formatted_time,
        ],
    )
    logger.debug(result.output)
    assert result.exit_code != 0


@pytest.mark.integration
@pytest.mark.requires_internet
@pytest.mark.slow
def test_table_schema_with_all_options_fails(
    login, testing_collection_with_table, tabsserver_connection
):
    runner = CliRunner()
    commit = tabsserver_connection.commits[0].id
    current_time = datetime.datetime.now(datetime.UTC)
    formatted_time = current_time.strftime("%Y-%m-%dT%H:%M:%S.%f")[:-3] + "Z"
    result = runner.invoke(
        cli,
        [
            "--no-prompt",
            "table",
            "schema",
            "--collection",
            testing_collection_with_table,
            "--name",
            "output",
            "--commit",
            commit,
            "--version",
            "HEAD",
            "--time",
            formatted_time,
        ],
    )
    logger.debug(result.output)
    assert result.exit_code != 0
